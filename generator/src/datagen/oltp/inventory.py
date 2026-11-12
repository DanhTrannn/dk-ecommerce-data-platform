"""Monthly inbound receipts, store transfers and final balances that cover every generated movement.

Stock for a month always arrives before the month starts, so no running balance ever goes negative:
receipts land 2 hours and transfers 1 hour before local midnight of the 1st.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from datagen.oltp.plan import MICROS_PER_HOUR, IdAllocator, Timeline, utc_micros, utc_series, yearmonth_of
from datagen.oltp.reference import Reference
from datagen.oltp.tables import conform
from datagen.presets import OltpPreset
from datagen.randomness import uuid4_strings


def demand_of(preset: OltpPreset, ledger: pl.DataFrame) -> pl.DataFrame:
    """Per (variant, store, local month): units taken out, and the net change of the batch's ledger rows."""
    return (
        ledger.with_columns(yearmonth=yearmonth_of(preset, ledger["created_at"]))
        .group_by("variant_id", "store_id", "yearmonth")
        .agg(
            (-pl.col("quantity_delta").clip(upper_bound=0)).sum().alias("outbound"),
            pl.col("quantity_delta").sum().alias("net"),
        )
    )


def _month_start(preset: OltpPreset, yearmonth: int) -> int:
    local = datetime(yearmonth // 100, yearmonth % 100, 1, tzinfo=ZoneInfo(preset.business_timezone))
    return int(utc_micros(local))


def inventory_tables(
    preset: OltpPreset,
    timeline: Timeline,
    reference: Reference,
    rng: np.random.Generator,
    ids: IdAllocator,
    demand: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    policy = preset.policies.inventory
    demand = demand.group_by("variant_id", "store_id", "yearmonth").agg(pl.col("outbound").sum(), pl.col("net").sum())
    months = sorted(set(timeline.yearmonth.tolist()) | set(demand["yearmonth"].to_list()))
    starts = pl.DataFrame({"yearmonth": months, "month_start": [_month_start(preset, m) for m in months]})

    transfers = (
        demand.filter(pl.col("store_id").is_not_null() & (pl.col("outbound") > 0))
        .sort("store_id", "variant_id", "yearmonth")
        .with_columns(first=pl.int_range(pl.len()).over("store_id", "variant_id") == 0)
    )
    buffer = rng.integers(policy.store_buffer_units[0], policy.store_buffer_units[1] + 1, transfers.height)
    transfers = transfers.with_columns(
        quantity=pl.col("outbound") + pl.when(pl.col("first")).then(pl.Series(buffer)).otherwise(0)
    ).join(starts, on="yearmonth")

    central_out = demand.filter(pl.col("store_id").is_null()).select("variant_id", "yearmonth", "outbound")
    sent = transfers.group_by("variant_id", "yearmonth").agg(pl.col("quantity").sum().alias("outbound"))
    first_month = months[0]
    opening = reference.variants.select("variant_id").with_columns(
        yearmonth=pl.lit(first_month),
        outbound=pl.Series(rng.integers(policy.buffer_units[0], policy.buffer_units[1] + 1, reference.variants.height)),
    )
    needed = (
        pl.concat([central_out, sent, opening], how="vertical_relaxed")
        .group_by("variant_id", "yearmonth")
        .agg(pl.col("outbound").sum().alias("quantity"))
        .filter(pl.col("quantity") > 0)
        .join(reference.variants.select("variant_id", "cost_price_vnd"), on="variant_id")
        .sort("yearmonth", "variant_id")
    )

    receipts = needed.select("yearmonth").unique().sort("yearmonth").join(starts, on="yearmonth")
    receipts = receipts.with_columns(
        receipt_id=pl.Series(ids.take("inbound_receipts", receipts.height)),
        public_id=uuid4_strings(rng, receipts.height),
        at=utc_series("at", receipts["month_start"].to_numpy() - 2 * MICROS_PER_HOUR),
    )
    lines = needed.join(receipts.select("yearmonth", "receipt_id", "at"), on="yearmonth").sort(
        "receipt_id", "variant_id"
    )
    lines = lines.with_columns(
        item_id=pl.Series(ids.take("inbound_receipt_items", lines.height)),
        public_id=uuid4_strings(rng, lines.height),
        total_cost_vnd=pl.col("quantity") * pl.col("cost_price_vnd"),
    )
    totals = lines.group_by("receipt_id").agg(
        pl.col("quantity").sum().alias("total_items_count"), pl.col("total_cost_vnd").sum()
    )
    receipt_rows = receipts.join(totals, on="receipt_id").with_columns(
        receipt_code=pl.format("SYN-INB-{}", "yearmonth"),
    )

    transfer_at = utc_series("at", transfers["month_start"].to_numpy() - MICROS_PER_HOUR)
    transfers = transfers.with_columns(
        at=transfer_at, reference_code=pl.format("SYN-TRF-{}-{}", "yearmonth", "store_id")
    )
    movements = pl.concat(
        [
            lines.select(
                "variant_id",
                pl.lit(None, dtype=pl.Int64).alias("store_id"),
                pl.lit("inbound").alias("movement_type"),
                "quantity",
                "receipt_id",
                pl.format("SYN-INB-{}", "yearmonth").alias("reference_code"),
                "at",
            ),
            transfers.select(
                "variant_id",
                pl.lit(None, dtype=pl.Int64).alias("store_id"),
                pl.lit("transfer_to_store").alias("movement_type"),
                (-pl.col("quantity")).alias("quantity"),
                pl.lit(None, dtype=pl.Int64).alias("receipt_id"),
                "reference_code",
                "at",
            ),
            transfers.select(
                "variant_id",
                "store_id",
                pl.lit("transfer_received").alias("movement_type"),
                "quantity",
                pl.lit(None, dtype=pl.Int64).alias("receipt_id"),
                "reference_code",
                "at",
            ),
        ]
    ).sort("at", "movement_type", "variant_id", "store_id")
    ledger = conform(
        "inventory_transactions",
        movements.select(
            pl.Series("transaction_id", ids.take("inventory_transactions", movements.height)),
            pl.Series("public_id", uuid4_strings(rng, movements.height)),
            "variant_id",
            pl.when(pl.col("store_id").is_null())
            .then(pl.lit("central_warehouse"))
            .otherwise(pl.lit("store"))
            .alias("location_type"),
            "store_id",
            "movement_type",
            pl.col("quantity").alias("quantity_delta"),
            "receipt_id",
            "reference_code",
            pl.col("at").alias("created_at"),
        ),
        null_columns=("order_id", "return_id", "notes"),
    )

    anchor = preset.anchor_time
    all_rows = pl.concat(
        [
            demand.select("variant_id", "store_id", "net"),
            ledger.select("variant_id", "store_id", pl.col("quantity_delta").alias("net")),
        ]
    )
    central = all_rows.filter(pl.col("store_id").is_null()).group_by("variant_id").agg(pl.col("net").sum())
    inventory = reference.variants.select("variant_id").join(central, on="variant_id", how="left").sort("variant_id")
    stores = (
        all_rows.filter(pl.col("store_id").is_not_null()).group_by("store_id", "variant_id").agg(pl.col("net").sum())
    ).sort("store_id", "variant_id")
    stamps = {"opening_on_hand": pl.lit(0), "version": pl.lit(0), "updated_at": pl.lit(anchor)}
    return {
        "inbound_receipts": conform(
            "inbound_receipts",
            receipt_rows.select(
                "receipt_id",
                "public_id",
                "receipt_code",
                pl.format("Nhập hàng tháng {}", "yearmonth").alias("batch_name"),
                pl.lit("completed").alias("status"),
                "total_items_count",
                "total_cost_vnd",
                pl.lit(reference.back_office_customer_id).alias("created_by_customer_id"),
                pl.col("at").alias("created_at"),
                pl.col("at").alias("updated_at"),
            ),
            null_columns=("notes",),
        ),
        "inbound_receipt_items": conform(
            "inbound_receipt_items",
            lines.select(
                "item_id",
                "public_id",
                "receipt_id",
                "variant_id",
                "quantity",
                pl.col("cost_price_vnd").alias("unit_cost_vnd"),
                "total_cost_vnd",
                pl.col("cost_price_vnd").alias("previous_cost_price_vnd"),
                pl.col("cost_price_vnd").alias("new_cost_price_vnd"),
                pl.col("at").alias("created_at"),
            ),
        ),
        "inventory_transactions": ledger,
        "inventory": conform(
            "inventory",
            inventory.select("variant_id", pl.col("net").fill_null(0).alias("on_hand")).with_columns(**stamps),
        ),
        "store_inventory": conform(
            "store_inventory",
            stores.select("store_id", "variant_id", pl.col("net").alias("on_hand")).with_columns(
                store_inventory_id=pl.Series(ids.take("store_inventory", stores.height)), **stamps
            ),
        ),
    }
