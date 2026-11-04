from __future__ import annotations


from lakehouse.sql import identifier, render_sql

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

try:
    from pyspark.sql import functions as F
except ImportError:
    F = None  # type: ignore

FACT_WEB_EVENTS_TABLE = "lakehouse.gold.fact_web_events"
MART_HOURLY_ROUTE_METRICS_TABLE = "lakehouse.gold.mart_hourly_route_metrics"
MART_DAILY_PRODUCT_DEMAND_TABLE = "lakehouse.gold.mart_daily_product_demand"
MART_MARKETING_FUNNEL_DAILY_TABLE = "lakehouse.gold.mart_marketing_funnel_daily"

FACT_WEB_EVENTS_DDL = render_sql(
    "spark/logs/fact_web_events.sql", fact_web_events_table=identifier(FACT_WEB_EVENTS_TABLE)
)

MART_HOURLY_ROUTE_METRICS_DDL = render_sql(
    "spark/logs/mart_hourly_route_metrics.sql",
    mart_hourly_route_metrics_table=identifier(MART_HOURLY_ROUTE_METRICS_TABLE),
)

MART_DAILY_PRODUCT_DEMAND_DDL = render_sql(
    "spark/logs/mart_daily_product_demand.sql",
    mart_daily_product_demand_table=identifier(MART_DAILY_PRODUCT_DEMAND_TABLE),
)

MART_MARKETING_FUNNEL_DAILY_DDL = render_sql(
    "spark/logs/mart_marketing_funnel_daily.sql",
    mart_marketing_funnel_daily_table=identifier(MART_MARKETING_FUNNEL_DAILY_TABLE),
)


def ensure_logs_gold_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    import re

    for target, ddl in [
        (FACT_WEB_EVENTS_TABLE, FACT_WEB_EVENTS_DDL),
        (MART_HOURLY_ROUTE_METRICS_TABLE, MART_HOURLY_ROUTE_METRICS_DDL),
        (MART_DAILY_PRODUCT_DEMAND_TABLE, MART_DAILY_PRODUCT_DEMAND_DDL),
        (MART_MARKETING_FUNNEL_DAILY_TABLE, MART_MARKETING_FUNNEL_DAILY_DDL),
    ]:
        spark.sql(ddl)
        existing = set(spark.table(target).columns)
        for name, kind in re.findall(r"^    ([a-z_]+)\s+(BIGINT|INT|STRING|DOUBLE)\b", ddl, re.M):
            if name not in existing:
                spark.sql(f"ALTER TABLE {target} ADD COLUMN {name} {kind}")


def build_mart_hourly_route_metrics(fact_df: DataFrame, run_id: str) -> DataFrame:
    percentiles = F.percentile_approx(F.col("duration_ms"), [0.5, 0.95, 0.99])

    return (
        fact_df.withColumn("metric_hour", F.hour(F.col("event_ts")))
        .groupBy("event_date", "metric_hour", "http_route", "http_request_method")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum(F.when(F.col("is_success"), 1).otherwise(0)).alias("success_2xx_count"),
            F.sum(F.when(F.col("is_client_error"), 1).otherwise(0)).alias("client_error_4xx_count"),
            F.sum(F.when(F.col("is_server_error"), 1).otherwise(0)).alias("server_error_5xx_count"),
            F.round(F.avg("duration_ms"), 3).alias("avg_duration_ms"),
            percentiles.alias("pcts"),
        )
        .withColumn(
            "error_rate_pct",
            F.round(
                ((F.col("client_error_4xx_count") + F.col("server_error_5xx_count")) / F.col("total_requests")) * 100.0,
                2,
            ),
        )
        .withColumn("p50_duration_ms", F.round(F.col("pcts")[0], 3))
        .withColumn("p95_duration_ms", F.round(F.col("pcts")[1], 3))
        .withColumn("p99_duration_ms", F.round(F.col("pcts")[2], 3))
        .withColumn("metric_date", F.col("event_date"))
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
        .select(
            "metric_date",
            "metric_hour",
            "http_route",
            "http_request_method",
            "total_requests",
            "success_2xx_count",
            "client_error_4xx_count",
            "server_error_5xx_count",
            "error_rate_pct",
            "avg_duration_ms",
            "p50_duration_ms",
            "p95_duration_ms",
            "p99_duration_ms",
            "_gold_ingested_at",
            "_source_run_id",
        )
    )


def build_mart_daily_product_demand(fact_df: DataFrame, run_id: str, *, products=None, variants=None) -> DataFrame:
    if products is not None and variants is not None:
        from lakehouse.logs.product_identity import resolve_product_events

        fact_df = resolve_product_events(fact_df, products, variants)
    else:
        fact_df = fact_df.withColumn("product_id", F.lit(None).cast("long")).withColumn(
            "product_name", F.lit(None).cast("string")
        )
    product_events = fact_df.filter(
        F.col("is_success")
        & F.col("ecommerce_action").isin(
            "product_detail",
            "cart_item_set",
            "cart_add",
            "cart_item_remove",
            "cart_remove",
            "wishlist_add",
            "checkout_quote",
        )
        & F.col("product_key").isNotNull()
        & (F.col("product_key") != "")
    )

    return (
        product_events.groupBy("event_date", "product_key", "product_id", "product_name")
        .agg(
            F.sum(F.when(F.col("ecommerce_action") == "product_detail", 1).otherwise(0)).alias("detail_view_count"),
            F.sum(F.when(F.col("ecommerce_action").isin("cart_add", "cart_item_set"), 1).otherwise(0)).alias(
                "cart_add_count"
            ),
            F.sum(F.when(F.col("ecommerce_action").isin("cart_remove", "cart_item_remove"), 1).otherwise(0)).alias(
                "cart_remove_count"
            ),
            F.sum(F.when(F.col("ecommerce_action") == "wishlist_add", 1).otherwise(0)).alias("wishlist_add_count"),
            F.sum(F.when(F.col("ecommerce_action") == "checkout_quote", 1).otherwise(0)).alias("checkout_quote_count"),
            F.countDistinct("actor_key").alias("unique_visitors_count"),
        )
        .withColumn(
            "cart_to_view_rate_pct",
            F.when(
                F.col("detail_view_count") > 0,
                F.round((F.col("cart_add_count") / F.col("detail_view_count")) * 100.0, 2),
            ),
        )
        .withColumn("metric_date", F.col("event_date"))
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
        .select(
            "metric_date",
            "product_key",
            "product_id",
            "product_name",
            "detail_view_count",
            "cart_add_count",
            "cart_remove_count",
            "wishlist_add_count",
            "checkout_quote_count",
            "unique_visitors_count",
            "cart_to_view_rate_pct",
            "_gold_ingested_at",
            "_source_run_id",
        )
    )


def build_mart_marketing_funnel_daily(fact_df: DataFrame, run_id: str) -> DataFrame:
    # These are successful request events, not unique people, sessions or orders.
    return (
        fact_df.groupBy("event_date")
        .agg(
            F.count("*").alias("observed_event_count"),
            F.sum(
                (F.col("is_success") & F.col("ecommerce_action").isin("product_detail", "catalog_search")).cast("long")
            ).alias("product_views"),
            F.sum(
                (F.col("is_success") & F.col("ecommerce_action").isin("cart_item_set", "cart_add")).cast("long")
            ).alias("cart_additions"),
            F.sum(
                (F.col("is_success") & F.col("ecommerce_action").isin("checkout_quote", "checkout_submit")).cast("long")
            ).alias("checkout_initiations"),
            F.sum((F.col("is_success") & (F.col("ecommerce_action") == "checkout_submit")).cast("long")).alias(
                "orders_completed"
            ),
            F.countDistinct("actor_key").alias("unique_visitors"),
        )
        .withColumn(
            "view_to_cart_rate_pct",
            F.when(F.col("product_views") > 0, F.col("cart_additions") * 100.0 / F.col("product_views")),
        )
        .withColumn(
            "cart_to_checkout_rate_pct",
            F.when(F.col("cart_additions") > 0, F.col("checkout_initiations") * 100.0 / F.col("cart_additions")),
        )
        .withColumn(
            "checkout_to_order_rate_pct",
            F.when(
                F.col("checkout_initiations") > 0, F.col("orders_completed") * 100.0 / F.col("checkout_initiations")
            ),
        )
        .withColumn(
            "overall_conversion_rate_pct",
            F.when(F.col("product_views") > 0, F.col("orders_completed") * 100.0 / F.col("product_views")),
        )
        .withColumn("metric_date", F.col("event_date"))
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
        .select(
            "metric_date",
            "observed_event_count",
            "product_views",
            "cart_additions",
            "checkout_initiations",
            "orders_completed",
            "view_to_cart_rate_pct",
            "cart_to_checkout_rate_pct",
            "checkout_to_order_rate_pct",
            "overall_conversion_rate_pct",
            "unique_visitors",
            "_gold_ingested_at",
            "_source_run_id",
        )
    )
