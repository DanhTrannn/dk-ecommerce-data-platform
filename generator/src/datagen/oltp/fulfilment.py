"""Shipments, returns, refunds and reviews derived from the order lifecycle."""

import numpy as np
import polars as pl

from datagen.oltp.plan import IdAllocator
from datagen.oltp.reference import Reference
from datagen.oltp.tables import conform
from datagen.randomness import uuid4_strings

FAILURE_REASON = "buyer_refused"
RETURN_REASON = "Sản phẩm không vừa"
REJECTION_REASON = "Nội dung không phù hợp"
REVIEW_CONTENT = {
    1: "Sản phẩm không như mong đợi.",
    2: "Chất lượng chưa tốt.",
    3: "Tạm ổn so với giá.",
    4: "Sản phẩm đẹp, giao hàng ổn.",
    5: "Rất hài lòng, sẽ mua lại.",
}


def _code(prefix: str, column: str) -> pl.Expr:
    return pl.format(f"{prefix}-{{}}", pl.col(column).cast(pl.String).str.zfill(12))


def shipment_tables(lifecycle: pl.DataFrame, rng: np.random.Generator, ids: IdAllocator) -> dict[str, pl.DataFrame]:
    shipped = lifecycle.filter(pl.col("dispatched_at").is_not_null())
    attempts_done = pl.sum_horizontal(pl.col(f"attempt_{n}_at").is_not_null() for n in (1, 2, 3))
    shipped = shipped.with_columns(
        shipment_id=pl.Series(ids.take("shipments", shipped.height)),
        public_id=uuid4_strings(rng, shipped.height),
        cod_amount_vnd=pl.when(pl.col("payment_method") == "cod").then(pl.col("total_vnd")).otherwise(0),
        attempts_done=attempts_done,
    ).with_columns(
        status=pl.when(pl.col("delivered_at").is_not_null())
        .then(pl.lit("delivered"))
        .when(pl.col("failed_at").is_not_null())
        .then(pl.lit("failed"))
        .otherwise(pl.lit("in_transit")),
        attempt_count=pl.when(pl.col("delivered_at").is_not_null())
        .then(1)
        .when(pl.col("failed_at").is_not_null())
        .then(pl.col("attempts_done"))
        .otherwise(pl.min_horizontal(pl.col("attempts_done") + 1, 3)),
    )
    shipments = conform(
        "shipments",
        shipped.select(
            "shipment_id",
            "public_id",
            _code("SYN-SHP", "shipment_id").alias("shipment_code"),
            "order_id",
            "delivery_staff_id",
            "status",
            "attempt_count",
            "cod_amount_vnd",
            pl.when(pl.col("status") == "delivered")
            .then(pl.col("cod_amount_vnd"))
            .otherwise(0)
            .alias("cod_collected_vnd"),
            "dispatched_at",
            "delivered_at",
            "failed_at",
            pl.when(pl.col("failed_at").is_not_null()).then(pl.lit(FAILURE_REASON)).alias("failure_reason"),
            pl.col("dispatched_at").alias("created_at"),
            pl.max_horizontal("dispatched_at", "delivered_at", "attempt_1_at", "attempt_2_at", "attempt_3_at").alias(
                "updated_at"
            ),
        ),
        null_columns=("notes",),
    )

    success = shipped.filter(pl.col("delivered_at").is_not_null()).select(
        "shipment_id",
        "delivery_staff_id",
        pl.lit(1).alias("attempt_number"),
        pl.lit("attempted_success").alias("status"),
        pl.lit(None, dtype=pl.String).alias("failure_reason"),
        pl.lit(False).alias("is_buyer_fault"),
        pl.col("delivered_at").alias("attempted_at"),
    )
    failures = [
        shipped.filter(pl.col(f"attempt_{n}_at").is_not_null()).select(
            "shipment_id",
            "delivery_staff_id",
            pl.lit(n).alias("attempt_number"),
            pl.lit("attempted_failed").alias("status"),
            pl.lit(FAILURE_REASON).alias("failure_reason"),
            pl.lit(True).alias("is_buyer_fault"),
            pl.col(f"attempt_{n}_at").alias("attempted_at"),
        )
        for n in (1, 2, 3)
    ]
    attempts = pl.concat([success, *failures]).sort("attempted_at", "shipment_id")
    attempts = attempts.with_columns(attempt_id=pl.Series(ids.take("shipment_attempts", attempts.height)))
    attempts = conform(
        "shipment_attempts",
        attempts.with_columns(
            public_id=uuid4_strings(rng, attempts.height),
            idempotency_key=pl.format("syn-att-{}", "attempt_id"),
            created_at=pl.col("attempted_at"),
            updated_at=pl.col("attempted_at"),
        ),
    )
    return {"shipments": shipments, "shipment_attempts": attempts}


def return_tables(
    lifecycle: pl.DataFrame, items: pl.DataFrame, reference: Reference, rng: np.random.Generator, ids: IdAllocator
) -> dict[str, pl.DataFrame]:
    """One completed return request per returning order, for one unit of one item (spec section 5)."""
    requests = lifecycle.filter(pl.col("return_action").is_not_null()).sort("order_id")
    requests = requests.with_columns(
        return_id=pl.Series(ids.take("return_requests", requests.height)),
        public_id=uuid4_strings(rng, requests.height),
    ).with_columns(return_code=_code("SYN-RET", "return_id"))
    ranked = items.with_columns(line=pl.int_range(pl.len()).over("order_id", order_by="variant_id"))
    lines = (
        requests.select(
            "return_id",
            "return_code",
            "order_id",
            "payment_id",
            "customer_id",
            "total_vnd",
            "return_action",
            "return_item_index",
            "return_requested_at",
            "return_resolved_at",
        )
        .join(
            ranked.select("order_id", "line", "order_item_id", "variant_id", "product_id", "price_vnd"),
            left_on=["order_id", "return_item_index"],
            right_on=["order_id", "line"],
            maintain_order="left",
        )
        .sort("return_id")
    )
    is_exchange = pl.col("return_action") == "exchange"
    lines = lines.with_columns(
        exchange_variant_id=_exchange_variants(lines, reference, rng),
        refund_amount_vnd=pl.when(is_exchange).then(0).otherwise(pl.min_horizontal("price_vnd", "total_vnd")),
    )

    return_requests = conform(
        "return_requests",
        requests.select(
            "return_id",
            "public_id",
            "return_code",
            "order_id",
            "customer_id",
            pl.col("return_action").alias("action_type"),
            pl.lit("completed").alias("status"),
            pl.lit(RETURN_REASON).alias("customer_reason"),
            pl.col("return_requested_at").alias("reviewed_at"),
            pl.col("return_resolved_at").alias("resolved_at"),
            pl.col("return_requested_at").alias("created_at"),
            pl.col("return_resolved_at").alias("updated_at"),
        ),
        null_columns=("image_urls", "admin_note"),
    )
    return_items = conform(
        "return_items",
        lines.select(
            pl.Series("return_item_id", ids.take("return_items", lines.height)),
            pl.Series("public_id", uuid4_strings(rng, lines.height)),
            "return_id",
            "order_item_id",
            "variant_id",
            pl.lit(1).alias("quantity"),
            "exchange_variant_id",
            "refund_amount_vnd",
            pl.lit("passed").alias("inspection_status"),
            pl.col("return_requested_at").alias("created_at"),
            pl.col("return_resolved_at").alias("updated_at"),
        ),
    )
    refunded = lines.filter(~is_exchange)
    refunds = _refunds(
        refunded.select(
            "payment_id",
            "return_id",
            pl.col("refund_amount_vnd").alias("amount_vnd"),
            pl.format("Hoàn tiền yêu cầu {}", "return_code").alias("reason"),
            pl.col("customer_id").alias("requested_by_customer_id"),
            pl.col("return_resolved_at").alias("at"),
        ),
        rng,
        ids,
    )
    ledger_rows = pl.concat(
        [
            lines.select(
                "return_id",
                "return_code",
                "variant_id",
                pl.lit("return_customer").alias("movement_type"),
                pl.lit(1).alias("quantity_delta"),
                pl.col("return_resolved_at").alias("created_at"),
            ),
            lines.filter(is_exchange).select(
                "return_id",
                "return_code",
                pl.col("exchange_variant_id").alias("variant_id"),
                pl.lit("exchange_out").alias("movement_type"),
                pl.lit(-1).alias("quantity_delta"),
                pl.col("return_resolved_at").alias("created_at"),
            ),
        ]
    ).sort("created_at", "return_id", "movement_type")
    ledger = conform(
        "inventory_transactions",
        ledger_rows.rename({"return_code": "reference_code"}).with_columns(
            transaction_id=pl.Series(ids.take("inventory_transactions", ledger_rows.height)),
            public_id=uuid4_strings(rng, ledger_rows.height),
            location_type=pl.lit("central_warehouse"),
        ),
        null_columns=("store_id", "order_id", "receipt_id", "notes"),
    )
    return {
        "return_requests": return_requests,
        "return_items": return_items,
        "refunds": refunds,
        "inventory_transactions": ledger,
    }


def _exchange_variants(lines: pl.DataFrame, reference: Reference, rng: np.random.Generator) -> pl.Series:
    """A different sellable variant of the same product for each exchange line, null for refunds."""
    options = (
        lines.filter(pl.col("return_action") == "exchange")
        .select("return_id", "product_id", pl.col("variant_id").alias("returned"))
        .join(reference.variants.select("product_id", "variant_id"), on="product_id", maintain_order="left")
        .filter(pl.col("variant_id") != pl.col("returned"))
        .sort("return_id", "variant_id")
    )
    options = options.with_columns(key=pl.Series(rng.random(options.height)))
    chosen = options.sort("return_id", "key").group_by("return_id", maintain_order=True).first()
    return (
        lines.select("return_id")
        .join(chosen.select("return_id", "variant_id"), on="return_id", how="left", maintain_order="left")["variant_id"]
        .alias("exchange_variant_id")
    )


def cancellation_refunds(lifecycle: pl.DataFrame, rng: np.random.Generator, ids: IdAllocator) -> pl.DataFrame:
    """A full refund for every VietQR order cancelled after payment; COD cancellations were never paid."""
    cancelled = lifecycle.filter(
        pl.col("cancelled_at").is_not_null() & (pl.col("payment_method") == "vietqr") & (pl.col("channel") == "online")
    ).sort("order_id")
    return _refunds(
        cancelled.select(
            "payment_id",
            pl.lit(None, dtype=pl.Int64).alias("return_id"),
            pl.col("total_vnd").alias("amount_vnd"),
            pl.col("cancel_reason").alias("reason"),
            pl.col("customer_id").alias("requested_by_customer_id"),
            pl.col("cancelled_at").alias("at"),
        ),
        rng,
        ids,
    )


def _refunds(rows: pl.DataFrame, rng: np.random.Generator, ids: IdAllocator) -> pl.DataFrame:
    rows = rows.with_columns(refund_id=pl.Series(ids.take("refunds", rows.height)))
    return conform(
        "refunds",
        rows.with_columns(
            public_id=uuid4_strings(rng, rows.height),
            refund_idempotency_key=pl.format("syn-ref-{}", "refund_id"),
            status=pl.lit("succeeded"),
            created_at=pl.col("at"),
            completed_at=pl.col("at"),
            updated_at=pl.col("at"),
        ).drop("at"),
    )


def review_table(
    lifecycle: pl.DataFrame, items: pl.DataFrame, reference: Reference, rng: np.random.Generator, ids: IdAllocator
) -> pl.DataFrame:
    """One review per reviewed order, on an item chosen uniformly; rejected reviews carry their moderation."""
    reviewed = lifecycle.filter(pl.col("review_at").is_not_null()).sort("order_id")
    counts = items.group_by("order_id").len()
    reviewed = reviewed.join(counts, on="order_id", maintain_order="left")
    reviewed = reviewed.with_columns(
        line=pl.Series(np.floor(rng.random(reviewed.height) * reviewed["len"].to_numpy()).astype(np.int64))
    )
    ranked = items.with_columns(line=pl.int_range(pl.len()).over("order_id", order_by="variant_id"))
    rows = reviewed.join(
        ranked.select("order_id", "line", "order_item_id", "product_id"), on=["order_id", "line"], maintain_order="left"
    )
    rows = rows.sort("order_id").with_columns(review_id=pl.Series(ids.take("product_reviews", rows.height)))
    rejected = pl.col("review_status") == "rejected"
    return conform(
        "product_reviews",
        rows.select(
            "review_id",
            pl.Series("public_id", uuid4_strings(rng, rows.height)),
            "order_item_id",
            "customer_id",
            "product_id",
            pl.col("review_rating").alias("rating"),
            pl.col("review_rating").replace_strict(REVIEW_CONTENT, return_dtype=pl.String).alias("content"),
            pl.col("review_status").alias("status"),
            pl.when(rejected).then(pl.lit(REJECTION_REASON)).alias("moderation_reason"),
            pl.when(rejected).then(pl.lit(reference.back_office_customer_id)).alias("moderated_by_customer_id"),
            pl.when(rejected).then(pl.col("review_moderated_at")).alias("moderated_at"),
            pl.col("review_at").alias("created_at"),
            pl.when(rejected).then(pl.col("review_moderated_at")).otherwise(pl.col("review_at")).alias("updated_at"),
        ),
    )
