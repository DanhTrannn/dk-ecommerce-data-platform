"""Operations aggregates with explicit measurement time and additive grains."""

from pyspark.sql import DataFrame, functions as F


def build_mart_operations_summary(
    orders: DataFrame,
    shipments: DataFrame,
    returns: DataFrame,
    refunds: DataFrame,
    run_id: str,
    *,
    as_of=None,
) -> DataFrame:
    observed = F.current_timestamp() if as_of is None else F.lit(as_of).cast("timestamp")
    pending = F.col("status").isin("paid", "confirmed")
    age = F.when(F.col("created_at") <= observed, (observed.cast("long") - F.col("created_at").cast("long")) / 3600.0)
    order_summary = orders.agg(
        F.count("order_id").alias("order_rows"),
        F.sum(pending.cast("long")).alias("pending_orders"),
        F.sum((pending & (F.col("created_at") <= observed - F.expr("INTERVAL 2 HOURS"))).cast("long")).alias(
            "over_two_hours_orders"
        ),
        F.avg(F.when(pending, age)).alias("average_waiting_hours"),
    )
    shipping = shipments.agg(
        F.count("shipment_id").alias("shipment_rows"),
        F.sum(((F.col("attempt_count") > 1) | (F.col("status") == "failed")).cast("long")).alias(
            "problematic_shipments"
        ),
        F.sum((F.col("status") == "failed").cast("long")).alias("failed_shipments"),
    )
    requests = returns.agg(
        F.count("return_id").alias("return_rows"),
        F.sum((F.col("status") != "cancelled").cast("long")).alias("return_requests_count"),
    )
    money = refunds.agg(
        F.count("refund_id").alias("refund_rows"),
        F.sum((F.col("status") == "succeeded").cast("long")).alias("successful_refunds_count"),
        F.sum(F.when(F.col("status") == "succeeded", F.col("amount_vnd")).otherwise(0)).alias(
            "successful_refund_amount_vnd"
        ),
    )
    # Each input is collapsed independently; request lines cannot multiply
    # order counts or successful refunds. Clock-based measures freeze at build.
    return (
        order_summary.crossJoin(shipping)
        .crossJoin(requests)
        .crossJoin(money)
        .withColumn("summary_key", F.lit("current"))
        .withColumn("operations_observed_at", observed)
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )


def build_mart_cod_region_daily(shipments: DataFrame, orders: DataFrame, run_id: str) -> DataFrame:
    cod = orders.filter(F.col("payment_method") == "cod").select("order_id", "shipping_region")
    return (
        shipments.join(cod, "order_id")
        .groupBy("shipment_date", F.coalesce("shipping_region", F.lit("Khu vực khác")).alias("region"))
        .agg(
            F.count("shipment_id").alias("total_cod_shipments"),
            F.sum((F.col("status") == "failed").cast("long")).alias("failed_cod_shipments"),
        )
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )
