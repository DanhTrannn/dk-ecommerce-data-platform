"""Validate current inventory serving without mixing daily snapshots."""

from pyspark.sql import functions as F

from lakehouse.oltp.quality.gate import QualityCheckResult


def inventory_consistency_checks(spark):
    checks = []
    source = spark.table("lakehouse.gold.fact_inventory_current").select(
        "location_type",
        "location_id",
        F.col("variant_key").alias("variant_id"),
        F.col("on_hand_quantity").alias("on_hand"),
        "reserved_quantity",
        "unit_cost_vnd",
        "inventory_value_cost_vnd",
    )
    target = spark.table("lakehouse.gold.mart_inventory_current").select(*source.columns)
    mismatch = source.exceptAll(target).count() + target.exceptAll(source).count()
    checks.append(
        QualityCheckResult(
            "current_stock_balance_parity",
            "mart_inventory_current",
            str(mismatch),
            "0",
            mismatch == 0,
            "Current SKU/location balances, reserved units and cost must match exactly; historical days are excluded",
            failed_rows=mismatch,
        )
    )
    for table, keys in (
        ("mart_inventory_current", ["location_type", "location_id", "variant_id"]),
        ("mart_store_overview", ["store_id"]),
    ):
        invalid = (
            spark.table(f"lakehouse.gold.{table}")
            .groupBy(*keys)
            .count()
            .filter("count > 1 OR " + " OR ".join(f"{key} IS NULL" for key in keys))
            .count()
        )
        checks.append(
            QualityCheckResult(
                "inventory_serving_grain",
                table,
                str(invalid),
                "0",
                invalid == 0,
                "Serving keys must be unique and non-null",
                failed_rows=invalid,
            )
        )
    summary = spark.sql("""SELECT count(*) n,count_if(s.summary_key='current'
        AND s.inventory_rows=f.n AND s.total_inventory_value_vnd <=> f.value
        AND s.warehouse_stock_units <=> f.warehouse AND s.store_stock_units <=> f.stores
        AND s.stockout_count <=> f.stockouts AND s.inbound_batches_count=r.n) valid
        FROM lakehouse.gold.mart_inventory_summary s CROSS JOIN (
            SELECT count(*) n,sum(inventory_value_cost_vnd) value,
            sum(CASE WHEN location_type='warehouse' THEN on_hand_quantity END) warehouse,
            sum(CASE WHEN location_type='store' THEN on_hand_quantity END) stores,
            CASE WHEN count(*)>0 THEN count(DISTINCT CASE WHEN is_out_of_stock THEN variant_key END) END stockouts
            FROM lakehouse.gold.fact_inventory_current
        ) f CROSS JOIN (SELECT count(*) n FROM lakehouse.silver.silver_inbound_receipts) r""").first()
    checks.append(
        QualityCheckResult(
            "inventory_summary_parity",
            "mart_inventory_summary",
            str(tuple(summary)),
            "(1, 1)",
            summary.n == 1 and summary.valid == 1,
            "One summary row must match current balances, cross-location distinct stockouts and known inbound count",
        )
    )
    stores = spark.table("lakehouse.gold.dim_store").select("store_id")
    served = spark.table("lakehouse.gold.mart_store_overview").select("store_id")
    missing = stores.exceptAll(served).count() + served.exceptAll(stores).count()
    checks.append(
        QualityCheckResult(
            "store_coverage",
            "mart_store_overview",
            str(missing),
            "0",
            missing == 0,
            "Include stores without orders or stock, keeping missing stock coverage nullable",
            failed_rows=missing,
        )
    )
    return checks
