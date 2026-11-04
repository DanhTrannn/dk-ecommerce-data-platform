"""Current inventory and store serving models, separate from daily history."""

from pyspark.sql import DataFrame, functions as F


def build_mart_inventory_current(
    inventory: DataFrame, variants: DataFrame, products: DataFrame, run_id: str
) -> DataFrame:
    return (
        inventory.alias("i")
        .join(variants.select("variant_key", "sku", "product_name"), "variant_key", "left")
        .join(products.select("product_key", "category_id", "category_name"), "product_key", "left")
        .select(
            "i.location_type",
            "i.location_id",
            F.col("i.variant_key").alias("variant_id"),
            "i.product_key",
            "sku",
            "product_name",
            "category_id",
            "category_name",
            "i.snapshot_date",
            F.col("i.on_hand_quantity").alias("on_hand"),
            "i.reserved_quantity",
            "i.unit_cost_vnd",
            "i.inventory_value_cost_vnd",
            "i.is_low_stock",
            "i.is_out_of_stock",
            F.col("i._gold_ingested_at").alias("observed_at"),
            F.current_timestamp().alias("_gold_ingested_at"),
            F.lit(run_id).alias("_source_run_id"),
        )
    )


def build_mart_inventory_summary(inventory: DataFrame, inbound: DataFrame, run_id: str) -> DataFrame:
    summary = inventory.agg(
        F.count("variant_id").alias("inventory_rows"),
        F.sum("inventory_value_cost_vnd").alias("total_inventory_value_vnd"),
        F.sum(F.when(F.col("location_type") == "warehouse", F.col("on_hand"))).alias("warehouse_stock_units"),
        F.sum(F.when(F.col("location_type") == "store", F.col("on_hand"))).alias("store_stock_units"),
        F.countDistinct(F.when(F.col("is_out_of_stock"), F.col("variant_id"))).alias("stockout_count"),
        F.max("observed_at").alias("inventory_observed_at"),
    ).withColumn("stockout_count", F.when(F.col("inventory_rows") > 0, F.col("stockout_count")))
    # A singleton retains the known inbound count independently of missing stock.
    return (
        summary.crossJoin(inbound.agg(F.count("receipt_id").alias("inbound_batches_count")))
        .withColumn("summary_key", F.lit("current"))
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )


def build_mart_store_overview(
    stores: DataFrame, orders: DataFrame, customers: DataFrame, inventory: DataFrame, run_id: str
) -> DataFrame:
    sales = orders.groupBy("store_key").agg(
        F.count("order_id").alias("total_orders"),
        F.sum((F.col("status") == "completed").cast("long")).alias("completed_orders"),
        F.sum("net_revenue_vnd").alias("total_revenue_vnd"),
    )
    staff = (
        customers.filter((F.col("role") == "store_manager") & (F.col("status") == "active"))
        .groupBy(F.col("store_id").alias("store_key"))
        .agg(F.count("customer_id").alias("active_staff"))
    )
    stock = (
        inventory.filter(F.col("location_type") == "store")
        .groupBy(F.col("location_id").alias("store_key"))
        .agg(
            F.sum(F.col("is_low_stock").cast("long")).alias("low_stock_count"),
            F.sum(F.col("is_out_of_stock").cast("long")).alias("stockout_count"),
            F.max("observed_at").alias("inventory_observed_at"),
        )
    )
    result = (
        stores.select("store_key", "store_id", "store_code", "store_name", "is_active")
        .join(sales, "store_key", "left")
        .join(staff, "store_key", "left")
        .join(stock, "store_key", "left")
    )
    # A published store with no orders/staff has a verified zero count. No stock
    # rows means stock coverage is unknown; retain nulls for those measures.
    return (
        result.fillna({"total_orders": 0, "completed_orders": 0, "total_revenue_vnd": 0, "active_staff": 0})
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )


def build_mart_inventory_health(
    fact_inventory_daily_snapshot: DataFrame,
    dim_product: DataFrame,
    run_id: str,
) -> DataFrame:
    fi = fact_inventory_daily_snapshot.alias("fi")
    dp = dim_product.alias("dp")

    joined = fi.join(dp, F.col("fi.product_key") == F.col("dp.product_key"), "left")

    return (
        joined.groupBy(
            F.col("fi.snapshot_date"),
            F.col("fi.location_type"),
            F.col("fi.location_id"),
            F.coalesce(F.col("dp.category_id"), F.lit(0)).alias("category_id"),
            F.coalesce(F.col("dp.category_name"), F.lit("Unknown")).alias("category_name"),
        )
        .agg(
            F.countDistinct("fi.variant_key").alias("total_variants"),
            F.sum(F.when(F.col("fi.is_out_of_stock"), 1).otherwise(0)).alias("out_of_stock_count"),
            F.sum(F.when(F.col("fi.is_low_stock"), 1).otherwise(0)).alias("low_stock_count"),
            F.sum("fi.on_hand_quantity").alias("total_on_hand_units"),
            F.sum("fi.inventory_value_cost_vnd").alias("total_inventory_value_vnd"),
        )
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
        .select(
            "snapshot_date",
            "location_type",
            "location_id",
            "category_id",
            "category_name",
            "total_variants",
            "out_of_stock_count",
            "low_stock_count",
            "total_on_hand_units",
            "total_inventory_value_vnd",
            "_gold_ingested_at",
            "_source_run_id",
        )
    )
