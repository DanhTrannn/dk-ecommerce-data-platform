from __future__ import annotations


from pyspark.sql import DataFrame, functions as F


def build_fact_inventory_daily_snapshot(
    silver_inventory: DataFrame,
    silver_store_inventory: DataFrame,
    silver_product_variants: DataFrame,
    snapshot_date: str,
    run_id: str,
) -> DataFrame:
    wh_reserved = (
        F.col("reserved").alias("reserved_quantity")
        if "reserved" in silver_inventory.columns
        else F.lit(0).cast("int").alias("reserved_quantity")
    )
    store_reserved = (
        F.col("reserved").alias("reserved_quantity")
        if "reserved" in silver_store_inventory.columns
        else F.lit(0).cast("int").alias("reserved_quantity")
    )

    inv = silver_inventory.select(
        F.lit("warehouse").alias("location_type"),
        F.lit(0).cast("bigint").alias("location_id"),
        F.col("variant_id"),
        F.col("on_hand").alias("on_hand_quantity"),
        wh_reserved,
    )
    store_inv = silver_store_inventory.select(
        F.lit("store").alias("location_type"),
        F.col("store_id").cast("bigint").alias("location_id"),
        F.col("variant_id"),
        F.col("on_hand").alias("on_hand_quantity"),
        store_reserved,
    )
    combined = inv.unionByName(store_inv)

    v = silver_product_variants.alias("v")
    c = combined.alias("c")

    joined = c.join(v, F.col("c.variant_id") == F.col("v.variant_id"), "left")
    unit_cost = F.coalesce(F.col("v.cost_price_vnd"), F.lit(0))
    inv_value = F.col("c.on_hand_quantity") * unit_cost

    date_col = F.to_date(F.lit(snapshot_date))
    date_key = F.date_format(date_col, "yyyyMMdd").cast("int")

    return joined.select(
        date_key.alias("snapshot_date_key"),
        date_col.alias("snapshot_date"),
        F.col("c.location_type"),
        F.col("c.location_id"),
        F.col("c.variant_id").alias("variant_key"),
        F.col("v.product_id").alias("product_key"),
        F.col("c.on_hand_quantity"),
        F.col("c.reserved_quantity"),
        unit_cost.alias("unit_cost_vnd"),
        inv_value.alias("inventory_value_cost_vnd"),
        (F.col("c.on_hand_quantity") <= 5).alias("is_low_stock"),
        (F.col("c.on_hand_quantity") == 0).alias("is_out_of_stock"),
        F.current_timestamp().alias("_gold_ingested_at"),
        F.lit(run_id).alias("_source_run_id"),
    )
