"""Project order lifecycles onto the order tables and the order-driven ledger rows."""

import numpy as np
import polars as pl

from datagen.oltp.plan import IdAllocator
from datagen.oltp.reference import Reference
from datagen.oltp.tables import conform
from datagen.presets import OltpPreset
from datagen.randomness import uuid4_strings

ONLINE_HISTORY = (
    # (to_status, time column, from_status for VietQR, from_status for COD); a missing time means no row.
    ("pending_payment", "created_at", None, "-"),
    ("confirmed", "created_at", "-", None),
    ("paid", "paid_at", "pending_payment", "-"),
    ("confirmed", "confirmed_at", "paid", "-"),
    ("cancelled", "cancelled_at", "paid", "confirmed"),
    ("shipping", "dispatched_at", "confirmed", "confirmed"),
    ("delivered", "delivered_at", "shipping", "shipping"),
    ("failed_delivery", "failed_at", "shipping", "shipping"),
    ("completed", "completed_at", "delivered", "delivered"),
    ("returned", "returned_at", "completed", "completed"),
)


def order_tables(
    preset: OltpPreset,
    lifecycle: pl.DataFrame,
    items: pl.DataFrame,
    reference: Reference,
    rng: np.random.Generator,
    ids: IdAllocator,
) -> tuple[dict[str, pl.DataFrame], pl.DataFrame, pl.DataFrame]:
    """Project the lifecycle onto orders, items, payments, history, redemptions and order ledger rows.

    Returns the tables plus the lifecycle and items enriched with their new ids, for the fulfilment step.
    """
    n = lifecycle.height
    lifecycle = (
        lifecycle.sort("order_index")
        .with_columns(order_id=pl.Series(ids.take("orders", n)), payment_id=pl.Series(ids.take("payments", n)))
        .with_columns(order_number=pl.format("SYN-{}", pl.col("order_id").cast(pl.String).str.zfill(12)))
    )
    items = (
        items.join(lifecycle.select("order_index", "order_id", "created_at"), on="order_index", maintain_order="left")
        .join(reference.variants, on="variant_id", maintain_order="left")
        .sort("order_index", "variant_id")
    )
    items = items.with_columns(
        order_item_id=pl.Series(ids.take("order_items", items.height)),
        public_id=uuid4_strings(rng, items.height),
        line_total_vnd=pl.col("price_vnd") * pl.col("quantity"),
    )

    tables = {
        "orders": _orders_table(preset, lifecycle),
        "order_items": conform(
            "order_items",
            items.select(
                "order_item_id",
                "public_id",
                "order_id",
                "variant_id",
                pl.col("product_public_id").alias("product_public_id_snapshot"),
                pl.col("category_code").alias("category_code_snapshot"),
                pl.col("category_name").alias("category_name_snapshot"),
                pl.col("product_name").alias("product_name_snapshot"),
                pl.col("sku").alias("sku_snapshot"),
                pl.col("size_code").alias("size_code_snapshot"),
                pl.col("color_code").alias("color_code_snapshot"),
                pl.col("price_vnd").alias("unit_price_vnd"),
                "cost_price_vnd",
                "quantity",
                "line_total_vnd",
                "created_at",
            ),
        ),
        "payments": conform(
            "payments",
            lifecycle.select(
                "payment_id",
                pl.format("SYN-PAY-{}", pl.col("payment_id").cast(pl.String).str.zfill(12)).alias("payment_reference"),
                "order_id",
                pl.format("syn-pay-{}", "payment_id").alias("payment_idempotency_key"),
                pl.when(pl.col("paid_at").is_not_null())
                .then(pl.lit("succeeded"))
                .otherwise(pl.lit("pending"))
                .alias("status"),
                pl.col("total_vnd").alias("amount_vnd"),
                pl.col("created_at").alias("attempted_at"),
                "created_at",
                pl.coalesce("paid_at", "created_at").alias("updated_at"),
            ),
            null_columns=("failure_code",),
        ),
        "order_status_history": _history_table(lifecycle, ids),
        "coupon_redemptions": _redemptions_table(lifecycle, ids),
        "inventory_transactions": _order_ledger(lifecycle, items, rng, ids),
    }
    return tables, lifecycle, items


def _orders_table(preset: OltpPreset, lifecycle: pl.DataFrame) -> pl.DataFrame:
    has_coupon = pl.col("coupon_id").is_not_null()
    return conform(
        "orders",
        lifecycle.select(
            "order_id",
            "order_number",
            "customer_id",
            pl.format("syn-checkout-{}", "order_id").alias("checkout_idempotency_key"),
            "coupon_id",
            "status",
            "payment_method",
            "subtotal_vnd",
            pl.col("coupon_code").alias("coupon_code_snapshot"),
            pl.when(has_coupon).then(pl.lit("percentage")).alias("coupon_type_snapshot"),
            pl.col("coupon_percentage").alias("coupon_value_snapshot"),
            "discount_amount_vnd",
            "shipping_fee_vnd",
            "total_vnd",
            "receiver_name",
            "receiver_phone",
            "shipping_address_text",
            "shipping_city_id",
            pl.lit("synthetic").alias("data_origin"),
            pl.lit(preset.run_id).alias("generation_run_id"),
            "created_at",
            "updated_at",
            "paid_at",
            "confirmed_at",
            "completed_at",
            "cancelled_at",
            "store_id",
            "channel",
            "staff_id",
        ),
        null_columns=("cart_id",),
    )


def _history_table(lifecycle: pl.DataFrame, ids: IdAllocator) -> pl.DataFrame:
    is_cod = pl.col("payment_method") == "cod"
    is_pos = pl.col("channel") == "pos"
    parts = [
        lifecycle.filter(is_pos).select(
            "order_id",
            pl.lit(None, dtype=pl.String).alias("from_status"),
            pl.lit("completed").alias("to_status"),
            pl.col("created_at").alias("transitioned_at"),
            pl.lit(None, dtype=pl.String).alias("reason"),
        )
    ]
    for to_status, column, from_vietqr, from_cod in ONLINE_HISTORY:
        for kind, source in ((~is_cod, from_vietqr), (is_cod, from_cod)):
            if source == "-":
                continue
            parts.append(
                lifecycle.filter(~is_pos & kind & pl.col(column).is_not_null()).select(
                    "order_id",
                    pl.lit(source, dtype=pl.String).alias("from_status"),
                    pl.lit(to_status).alias("to_status"),
                    pl.col(column).alias("transitioned_at"),
                    pl.when(pl.lit(to_status) == "cancelled").then(pl.col("cancel_reason")).alias("reason"),
                )
            )
    history = pl.concat(parts).sort("order_id", "transitioned_at", maintain_order=True)
    history = history.with_columns(order_status_history_id=pl.Series(ids.take("order_status_history", history.height)))
    return conform(
        "order_status_history",
        history.with_columns(
            transition_source=pl.lit("generator"),
            transition_idempotency_key=pl.format("syn-osh-{}", "order_status_history_id"),
            created_at=pl.col("transitioned_at"),
        ),
    )


def _redemptions_table(lifecycle: pl.DataFrame, ids: IdAllocator) -> pl.DataFrame:
    used = lifecycle.filter(pl.col("coupon_id").is_not_null())
    released = pl.col("cancelled_at").is_not_null()
    return conform(
        "coupon_redemptions",
        used.select(
            pl.Series("coupon_redemption_id", ids.take("coupon_redemptions", used.height)),
            "coupon_id",
            "order_id",
            "customer_id",
            pl.when(released).then(pl.lit("released")).otherwise(pl.lit("redeemed")).alias("status"),
            pl.col("created_at").alias("redeemed_at"),
            pl.col("cancelled_at").alias("released_at"),
            "created_at",
            pl.coalesce("cancelled_at", "created_at").alias("updated_at"),
        ),
    )


def _order_ledger(
    lifecycle: pl.DataFrame, items: pl.DataFrame, rng: np.random.Generator, ids: IdAllocator
) -> pl.DataFrame:
    lines = items.select("order_id", "variant_id", "quantity").join(
        lifecycle.select("order_id", "order_number", "channel", "store_id", "created_at", "cancelled_at", "failed_at"),
        on="order_id",
        maintain_order="left",
    )
    is_pos = pl.col("channel") == "pos"
    parts = [
        lines.select(
            "order_id",
            "variant_id",
            "order_number",
            "store_id",
            pl.when(is_pos).then(pl.lit("outbound_pos")).otherwise(pl.lit("outbound_order")).alias("movement_type"),
            (-pl.col("quantity")).alias("quantity_delta"),
            "created_at",
        ),
        lines.filter(pl.col("cancelled_at").is_not_null()).select(
            "order_id",
            "variant_id",
            "order_number",
            "store_id",
            pl.lit("order_cancellation").alias("movement_type"),
            pl.col("quantity").alias("quantity_delta"),
            pl.col("cancelled_at").alias("created_at"),
        ),
        lines.filter(pl.col("failed_at").is_not_null()).select(
            "order_id",
            "variant_id",
            "order_number",
            "store_id",
            pl.lit("return_boom").alias("movement_type"),
            pl.col("quantity").alias("quantity_delta"),
            pl.col("failed_at").alias("created_at"),
        ),
    ]
    ledger = pl.concat(parts).sort("created_at", "order_id", "variant_id", "movement_type")
    return conform(
        "inventory_transactions",
        ledger.with_columns(
            transaction_id=pl.Series(ids.take("inventory_transactions", ledger.height)),
            public_id=uuid4_strings(rng, ledger.height),
            location_type=pl.when(pl.col("store_id").is_null())
            .then(pl.lit("central_warehouse"))
            .otherwise(pl.lit("store")),
        ).rename({"order_number": "reference_code"}),
        null_columns=("receipt_id", "return_id", "notes"),
    )
