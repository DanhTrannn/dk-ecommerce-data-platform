"""Gold stage I/O; domain builders remain pure DataFrame transformations."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession, functions as F
from .dimensions import build_dim_date
from .dimensions import build_dim_product
from .dimensions import build_dim_variant
from .dimensions import build_dim_store
from .dimensions import build_dim_delivery_staff
from .dimensions import build_dim_customer
from .sales import build_fact_order
from .sales import build_fact_order_item
from .sales_marts import build_mart_sales_daily
from .sales_marts import build_mart_order_daily
from .sales_marts import build_mart_product_sales_daily
from .logistics import build_fact_shipment
from .logistics import build_mart_logistics_performance
from .returns import build_fact_return_exchange
from .returns import build_mart_product_returns
from .inventory import build_fact_inventory_daily_snapshot
from .inventory_marts import build_mart_inventory_health
from .inventory_marts import build_mart_inventory_current
from .inventory_marts import build_mart_inventory_summary
from .inventory_marts import build_mart_store_overview
from .operations_marts import build_mart_operations_summary
from .operations_marts import build_mart_cod_region_daily
from .marketing_marts import build_mart_marketing_summary
from .marketing_marts import build_mart_coupon_usage
from .marketing_marts import build_mart_product_review_summary


def run_gold_table(spark: SparkSession, table: str, run_id: str, snapshot_date: str, *, as_of=None) -> None:
    """Build exactly one Gold table from its Silver or Gold dependencies."""
    observed = as_of if as_of is not None else datetime.now(timezone.utc)
    observed_date = observed.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    print(f"[{run_id}] Building {table}")

    if table == "dim_date":
        frame = build_dim_date(spark)
    elif table == "dim_product":
        frame = build_dim_product(
            spark.table("lakehouse.silver.silver_products"), spark.table("lakehouse.silver.silver_categories"), run_id
        )
    elif table == "dim_variant":
        frame = build_dim_variant(
            spark.table("lakehouse.silver.silver_product_variants"),
            spark.table("lakehouse.silver.silver_products"),
            run_id,
        )
    elif table == "dim_store":
        frame = build_dim_store(
            spark.table("lakehouse.silver.silver_stores"), spark.table("lakehouse.silver.silver_cities"), run_id
        )
    elif table == "dim_delivery_staff":
        frame = build_dim_delivery_staff(
            spark.table("lakehouse.silver.silver_delivery_staff"), spark.table("lakehouse.silver.silver_cities"), run_id
        )
    elif table == "dim_customer":
        frame = build_dim_customer(
            spark.table("lakehouse.silver.silver_customers"), spark.table("lakehouse.silver.silver_cities"), run_id
        )
    elif table == "fact_order_item":
        frame = build_fact_order_item(
            spark.table("lakehouse.silver.silver_order_items"),
            spark.table("lakehouse.silver.silver_orders"),
            run_id,
            spark.table("lakehouse.silver.silver_product_variants"),
            silver_payments=spark.table("lakehouse.silver.silver_payments"),
            silver_refunds=spark.table("lakehouse.silver.silver_refunds"),
            silver_return_requests=spark.table("lakehouse.silver.silver_return_requests"),
            silver_return_items=spark.table("lakehouse.silver.silver_return_items"),
        )
    elif table == "fact_order":
        frame = build_fact_order(
            spark.table("lakehouse.silver.silver_orders"),
            spark.table("lakehouse.silver.silver_order_items"),
            run_id,
            spark.table("lakehouse.silver.silver_product_variants"),
            fact_order_items=spark.table("lakehouse.gold.fact_order_item"),
            silver_payments=spark.table("lakehouse.silver.silver_payments"),
        )
    elif table == "fact_shipment":
        frame = build_fact_shipment(spark.table("lakehouse.silver.silver_shipments"), run_id)
    elif table == "fact_return_exchange":
        frame = build_fact_return_exchange(
            spark.table("lakehouse.silver.silver_return_requests"),
            spark.table("lakehouse.silver.silver_return_items"),
            run_id,
        )
    elif table == "fact_inventory_daily_snapshot":
        if snapshot_date != observed_date:
            print(
                f"[{run_id}] Inventory uses observation date {observed_date}; batch interval {snapshot_date} does not reconstruct historical stock."
            )
        frame = build_fact_inventory_daily_snapshot(
            spark.table("lakehouse.silver.silver_inventory"),
            spark.table("lakehouse.silver.silver_store_inventory"),
            spark.table("lakehouse.silver.silver_product_variants"),
            observed_date,
            run_id,
        )
    elif table == "fact_inventory_current":
        frame = spark.table("lakehouse.gold.fact_inventory_daily_snapshot").filter(
            F.col("snapshot_date") == F.lit(observed_date).cast("date")
        )
    elif table == "mart_sales_daily":
        frame = build_mart_sales_daily(
            spark.table("lakehouse.gold.fact_order"),
            spark.table("lakehouse.gold.fact_order_item"),
            spark.table("lakehouse.gold.dim_product"),
            run_id,
        )
    elif table == "mart_order_daily":
        frame = build_mart_order_daily(
            spark.table("lakehouse.gold.fact_order"),
            spark.table("lakehouse.gold.fact_shipment"),
            spark.table("lakehouse.silver.silver_return_requests"),
            spark.table("lakehouse.gold.dim_store"),
            run_id,
        )
    elif table == "mart_product_sales_daily":
        frame = build_mart_product_sales_daily(
            spark.table("lakehouse.gold.fact_order_item"), spark.table("lakehouse.gold.dim_product"), run_id
        )
    elif table == "mart_logistics_performance":
        frame = build_mart_logistics_performance(
            spark.table("lakehouse.gold.fact_shipment"), spark.table("lakehouse.gold.dim_delivery_staff"), run_id
        )
    elif table == "mart_product_returns":
        frame = build_mart_product_returns(
            spark.table("lakehouse.gold.fact_return_exchange"), spark.table("lakehouse.gold.dim_variant"), run_id
        )
    elif table == "mart_inventory_health":
        frame = build_mart_inventory_health(
            spark.table("lakehouse.gold.fact_inventory_daily_snapshot"),
            spark.table("lakehouse.gold.dim_product"),
            run_id,
        )
    elif table == "mart_inventory_current":
        frame = build_mart_inventory_current(
            spark.table("lakehouse.gold.fact_inventory_current"),
            spark.table("lakehouse.gold.dim_variant"),
            spark.table("lakehouse.gold.dim_product"),
            run_id,
        ).withColumn("observed_at", F.lit(observed).cast("timestamp"))
    elif table == "mart_inventory_summary":
        frame = build_mart_inventory_summary(
            spark.table("lakehouse.gold.mart_inventory_current"),
            spark.table("lakehouse.silver.silver_inbound_receipts"),
            run_id,
        )
    elif table == "mart_store_overview":
        frame = build_mart_store_overview(
            spark.table("lakehouse.gold.dim_store"),
            spark.table("lakehouse.gold.fact_order"),
            spark.table("lakehouse.silver.silver_customers"),
            spark.table("lakehouse.gold.mart_inventory_current"),
            run_id,
        )
    elif table == "mart_operations_summary":
        frame = build_mart_operations_summary(
            spark.table("lakehouse.gold.fact_order"),
            spark.table("lakehouse.gold.fact_shipment"),
            spark.table("lakehouse.silver.silver_return_requests"),
            spark.table("lakehouse.silver.silver_refunds"),
            run_id,
            as_of=observed,
        )
    elif table == "mart_cod_region_daily":
        frame = build_mart_cod_region_daily(
            spark.table("lakehouse.gold.fact_shipment"), spark.table("lakehouse.gold.fact_order"), run_id
        )
    elif table == "mart_marketing_summary":
        frame = build_mart_marketing_summary(
            spark.table("lakehouse.silver.silver_coupons"),
            spark.table("lakehouse.silver.silver_product_reviews"),
            run_id,
            as_of=observed,
        )
    elif table == "mart_coupon_usage":
        frame = build_mart_coupon_usage(
            spark.table("lakehouse.silver.silver_coupons"),
            spark.table("lakehouse.silver.silver_coupon_redemptions"),
            run_id,
            as_of=observed,
        )
    elif table == "mart_product_review_summary":
        frame = build_mart_product_review_summary(
            spark.table("lakehouse.silver.silver_product_reviews"),
            spark.table("lakehouse.gold.dim_product"),
            run_id,
            as_of=observed,
        )
    else:
        raise ValueError(f"Unknown Gold table: {table}")

    target = f"lakehouse.gold.{table}"
    writer = frame.select(*spark.table(target).columns).writeTo(target)
    if table == "dim_date":
        writer.overwritePartitions()
    elif table == "fact_inventory_daily_snapshot":
        writer.overwrite(F.col("snapshot_date") == F.lit(observed_date).cast("date"))
    else:
        writer.overwrite(F.lit(True))
