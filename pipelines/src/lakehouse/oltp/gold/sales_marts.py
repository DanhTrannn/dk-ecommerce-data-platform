"""Serving marts with additive order counts and product-level sales amounts."""

from pyspark.sql import DataFrame, functions as F


def build_mart_order_daily(
    fact_order: DataFrame,
    fact_shipment: DataFrame,
    return_requests: DataFrame,
    dim_store: DataFrame,
    run_id: str,
) -> DataFrame:
    # Collapse one-to-many relationships before joining so orders/money are not
    # multiplied by return requests or shipment attempts.
    returns = (
        return_requests.filter(F.col("status") != "cancelled")
        .groupBy("order_id")
        .agg(F.countDistinct("return_id").alias("return_requests_count"))
    )
    shipments = (
        fact_shipment.filter(F.col("status").isin("assigned", "picked_up", "in_transit"))
        .groupBy("order_id")
        .agg(F.count("shipment_id").alias("_active_shipments"))
    )
    orders = fact_order.join(returns, "order_id", "left").join(shipments, "order_id", "left")
    orders = orders.withColumn(
        "store_key", F.when((F.col("channel") == "online") | (F.col("store_key") == 0), 0).otherwise(F.col("store_key"))
    ).join(dim_store.select("store_key", "store_name"), "store_key", "left")
    orders = orders.withColumn(
        "store_name",
        F.when(F.col("store_key") == 0, "Kênh Online Toàn Quốc").otherwise(
            F.coalesce("store_name", F.lit("Chưa xác định"))
        ),
    ).withColumn(
        "_cod_shipments",
        F.when(F.col("payment_method") == "cod", F.coalesce("_active_shipments", F.lit(0))).otherwise(0),
    )
    return (
        orders.groupBy("order_date", "channel", "store_key", "store_name")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.col("is_revenue_recognized").cast("long")).alias("recognized_orders"),
            F.sum((F.col("net_revenue_vnd") > 0).cast("long")).alias("revenue_orders"),
            F.sum(F.col("is_boom").cast("long")).alias("boom_orders"),
            F.sum(F.coalesce("return_requests_count", F.lit(0))).alias("return_requests_count"),
            F.sum("order_total_vnd").alias("order_total_vnd"),
            F.sum("gross_revenue_vnd").alias("gross_revenue_vnd"),
            F.sum("refunded_amount_vnd").alias("refunded_amount_vnd"),
            F.sum("net_revenue_vnd").alias("net_revenue_vnd"),
            F.sum("total_cost_vnd").alias("cogs_vnd"),
            F.sum("gross_profit_vnd").alias("gross_profit_vnd"),
            F.sum(F.col("order_total_vnd") * F.col("_cod_shipments")).alias("cod_in_transit_vnd"),
            F.sum("_cod_shipments").alias("cod_shipments_count"),
        )
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )


def build_mart_product_sales_daily(fact_order_item: DataFrame, dim_product: DataFrame, run_id: str) -> DataFrame:
    # Status, recognized quantity and allocated money already follow the same
    # discount/refund rules as order/category totals; do not recalculate them.
    rows = fact_order_item.filter(F.col("order_status").isin("delivered", "completed", "returned"))
    return (
        rows.join(dim_product.select("product_key", "product_id", "product_name"), "product_key")
        .groupBy("order_date", "product_id", "product_name")
        .agg(
            F.sum("recognized_quantity").alias("units_sold"),
            F.sum("gross_revenue_vnd").alias("gross_revenue_vnd"),
            F.sum("refunded_amount_vnd").alias("refunded_amount_vnd"),
            F.sum("net_revenue_vnd").alias("net_revenue_vnd"),
            F.sum("item_cost_vnd").alias("cogs_vnd"),
            F.sum("item_profit_vnd").alias("gross_profit_vnd"),
        )
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )


def build_mart_sales_daily(
    fact_order: DataFrame, fact_order_item: DataFrame, dim_product: DataFrame, run_id: str
) -> DataFrame:
    # Resolve category identity by the immutable order-time category code; display
    # labels also come from the order snapshot so a rename does not rewrite history.
    categories = dim_product.select("category_id", "category_code").distinct()
    rows = (
        fact_order_item.alias("i")
        .join(fact_order.select("order_id", "is_revenue_recognized", "is_boom"), "order_id")
        .join(categories, F.col("category_code_snapshot") == F.col("category_code"), "left")
    )
    grouped = rows.groupBy(
        "order_date",
        "channel",
        "store_key",
        F.coalesce(F.col("category_id"), F.lit(0)).alias("category_id"),
        F.coalesce(F.col("category_name_snapshot"), F.lit("Unknown")).alias("category_name"),
    ).agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("is_revenue_recognized"), F.col("order_id"))).alias("successful_orders"),
        F.countDistinct(F.when(F.col("is_boom"), F.col("order_id"))).alias("boom_orders"),
        F.sum("recognized_quantity").cast("long").alias("items_sold"),
        F.sum("gross_revenue_vnd").alias("gross_revenue_vnd"),
        F.sum("refunded_amount_vnd").alias("refunded_amount_vnd"),
        F.sum("net_revenue_vnd").alias("net_revenue_vnd"),
        F.sum("item_cost_vnd").alias("cogs_vnd"),
        F.sum("item_profit_vnd").alias("gross_profit_vnd"),
    )
    return (
        grouped.withColumn(
            "gross_profit_margin_pct",
            F.when(
                F.col("net_revenue_vnd") > 0, F.round(F.col("gross_profit_vnd") / F.col("net_revenue_vnd") * 100, 2)
            ).otherwise(0.0),
        )
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )
