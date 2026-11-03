"""Gold stage I/O; domain builders remain pure DataFrame transformations."""

from pyspark.sql import SparkSession, functions as F
from lakehouse.oltp.gold.inventory import inventory_observation_date
from lakehouse.oltp.gold import (
    build_dim_customer,
    build_dim_date,
    build_dim_delivery_staff,
    build_dim_product,
    build_dim_store,
    build_dim_variant,
    build_fact_inventory_daily_snapshot,
    build_fact_order,
    build_fact_order_item,
    build_fact_return_exchange,
    build_fact_shipment,
    build_mart_inventory_health,
    build_mart_logistics_performance,
    build_mart_product_returns,
    build_mart_sales_daily,
    build_mart_order_daily,
    build_mart_product_sales_daily,
    build_mart_inventory_current,
    build_mart_inventory_summary,
    build_mart_store_overview,
)


def run_gold_dimensions(spark: SparkSession, run_id: str) -> None:
    """Load Silver dependencies and build Gold conformed dimensions."""
    print(f"[{run_id}] [Dimensions] Loading Silver tables...")
    silver_products = spark.read.format("iceberg").load("lakehouse.silver.silver_products")
    silver_categories = spark.read.format("iceberg").load("lakehouse.silver.silver_categories")
    silver_product_variants = spark.read.format("iceberg").load("lakehouse.silver.silver_product_variants")
    silver_stores = spark.read.format("iceberg").load("lakehouse.silver.silver_stores")
    silver_cities = spark.read.format("iceberg").load("lakehouse.silver.silver_cities")
    silver_delivery_staff = spark.read.format("iceberg").load("lakehouse.silver.silver_delivery_staff")
    silver_customers = spark.read.format("iceberg").load("lakehouse.silver.silver_customers")

    print(f"[{run_id}] [Dimensions] Building dim_date...")
    dim_date = build_dim_date(spark)
    dim_date.writeTo("lakehouse.gold.dim_date").overwritePartitions()

    print(f"[{run_id}] [Dimensions] Building dim_product...")
    dim_product = build_dim_product(silver_products, silver_categories, run_id)
    dim_product.select(*spark.table("lakehouse.gold.dim_product").columns).writeTo(
        "lakehouse.gold.dim_product"
    ).overwrite(F.lit(True))

    print(f"[{run_id}] [Dimensions] Building dim_variant...")
    dim_variant = build_dim_variant(silver_product_variants, silver_products, run_id)
    dim_variant.writeTo("lakehouse.gold.dim_variant").overwrite(F.lit(True))

    print(f"[{run_id}] [Dimensions] Building dim_store...")
    dim_store = build_dim_store(silver_stores, silver_cities, run_id)
    dim_store.writeTo("lakehouse.gold.dim_store").overwrite(F.lit(True))

    print(f"[{run_id}] [Dimensions] Building dim_delivery_staff...")
    dim_delivery_staff = build_dim_delivery_staff(silver_delivery_staff, silver_cities, run_id)
    dim_delivery_staff.writeTo("lakehouse.gold.dim_delivery_staff").overwrite(F.lit(True))

    print(f"[{run_id}] [Dimensions] Building dim_customer...")
    dim_customer = build_dim_customer(silver_customers, silver_cities, run_id)
    dim_customer.writeTo("lakehouse.gold.dim_customer").overwrite(F.lit(True))

    print(f"[{run_id}] [Dimensions] All 6 Gold Dimensions successfully built!")


def run_gold_facts(spark: SparkSession, run_id: str, snapshot_date: str) -> None:
    """Load Silver dependencies and build Gold fact tables."""
    observed_date = inventory_observation_date()
    if snapshot_date != observed_date:
        print(
            f"[{run_id}] Inventory uses observation date {observed_date}; batch interval {snapshot_date} does not reconstruct historical stock."
        )
    print(f"[{run_id}] [Facts] Loading Silver tables...")
    silver_orders = spark.read.format("iceberg").load("lakehouse.silver.silver_orders")
    silver_order_items = spark.read.format("iceberg").load("lakehouse.silver.silver_order_items")
    silver_product_variants = spark.read.format("iceberg").load("lakehouse.silver.silver_product_variants")
    silver_shipments = spark.read.format("iceberg").load("lakehouse.silver.silver_shipments")
    silver_return_requests = spark.read.format("iceberg").load("lakehouse.silver.silver_return_requests")
    silver_return_items = spark.read.format("iceberg").load("lakehouse.silver.silver_return_items")
    silver_inventory = spark.read.format("iceberg").load("lakehouse.silver.silver_inventory")
    silver_store_inventory = spark.read.format("iceberg").load("lakehouse.silver.silver_store_inventory")

    print(f"[{run_id}] [Facts] Building canonical order items...")
    fact_order_item = build_fact_order_item(
        silver_order_items,
        silver_orders,
        run_id,
        silver_product_variants,
        silver_payments=spark.table("lakehouse.silver.silver_payments"),
        silver_refunds=spark.table("lakehouse.silver.silver_refunds"),
        silver_return_requests=silver_return_requests,
        silver_return_items=silver_return_items,
    )
    fact_order_item.select(*spark.table("lakehouse.gold.fact_order_item").columns).writeTo(
        "lakehouse.gold.fact_order_item"
    ).overwrite(F.lit(True))
    fact_order = build_fact_order(
        silver_orders,
        silver_order_items,
        run_id,
        silver_product_variants,
        fact_order_items=spark.table("lakehouse.gold.fact_order_item"),
        silver_payments=spark.table("lakehouse.silver.silver_payments"),
    )
    fact_order.select(*spark.table("lakehouse.gold.fact_order").columns).writeTo("lakehouse.gold.fact_order").overwrite(
        F.lit(True)
    )

    print(f"[{run_id}] [Facts] Building fact_shipment...")
    fact_shipment = build_fact_shipment(silver_shipments, run_id)
    fact_shipment.select(*spark.table("lakehouse.gold.fact_shipment").columns).writeTo(
        "lakehouse.gold.fact_shipment"
    ).overwrite(F.lit(True))

    print(f"[{run_id}] [Facts] Building fact_return_exchange...")
    fact_return_exchange = build_fact_return_exchange(silver_return_requests, silver_return_items, run_id)
    fact_return_exchange.select(*spark.table("lakehouse.gold.fact_return_exchange").columns).writeTo(
        "lakehouse.gold.fact_return_exchange"
    ).overwrite(F.lit(True))

    print(f"[{run_id}] [Facts] Building fact_inventory_daily_snapshot...")
    fact_inventory_daily_snapshot = build_fact_inventory_daily_snapshot(
        silver_inventory, silver_store_inventory, silver_product_variants, observed_date, run_id
    )
    fact_inventory_daily_snapshot.select(*spark.table("lakehouse.gold.fact_inventory_daily_snapshot").columns).writeTo(
        "lakehouse.gold.fact_inventory_daily_snapshot"
    ).overwrite(F.col("snapshot_date") == F.lit(observed_date).cast("date"))
    fact_inventory_daily_snapshot.select(*spark.table("lakehouse.gold.fact_inventory_current").columns).writeTo(
        "lakehouse.gold.fact_inventory_current"
    ).overwrite(F.lit(True))

    print(f"[{run_id}] [Facts] All 6 Gold Facts successfully built!")


def run_gold_marts(spark: SparkSession, run_id: str) -> None:
    """Load Gold Facts and Dimensions to rollup Data Marts."""
    print(f"[{run_id}] [Marts] Loading Gold Facts & Dimensions...")
    fact_order = spark.read.format("iceberg").load("lakehouse.gold.fact_order")
    fact_order_item = spark.read.format("iceberg").load("lakehouse.gold.fact_order_item")
    dim_product = spark.read.format("iceberg").load("lakehouse.gold.dim_product")
    fact_shipment = spark.read.format("iceberg").load("lakehouse.gold.fact_shipment")
    dim_delivery_staff = spark.read.format("iceberg").load("lakehouse.gold.dim_delivery_staff")
    fact_return_exchange = spark.read.format("iceberg").load("lakehouse.gold.fact_return_exchange")
    dim_variant = spark.read.format("iceberg").load("lakehouse.gold.dim_variant")
    fact_inventory_daily_snapshot = spark.read.format("iceberg").load("lakehouse.gold.fact_inventory_daily_snapshot")

    print(f"[{run_id}] [Marts] Building mart_sales_daily...")
    mart_sales_daily = build_mart_sales_daily(fact_order, fact_order_item, dim_product, run_id)
    mart_sales_daily.select(*spark.table("lakehouse.gold.mart_sales_daily").columns).writeTo(
        "lakehouse.gold.mart_sales_daily"
    ).overwrite(F.lit(True))

    mart_order_daily = build_mart_order_daily(
        fact_order,
        fact_shipment,
        spark.table("lakehouse.silver.silver_return_requests"),
        spark.table("lakehouse.gold.dim_store"),
        run_id,
    )
    mart_order_daily.select(*spark.table("lakehouse.gold.mart_order_daily").columns).writeTo(
        "lakehouse.gold.mart_order_daily"
    ).overwrite(F.lit(True))
    mart_products = build_mart_product_sales_daily(fact_order_item, dim_product, run_id)
    mart_products.select(*spark.table("lakehouse.gold.mart_product_sales_daily").columns).writeTo(
        "lakehouse.gold.mart_product_sales_daily"
    ).overwrite(F.lit(True))

    print(f"[{run_id}] [Marts] Building mart_logistics_performance...")
    mart_logistics_performance = build_mart_logistics_performance(fact_shipment, dim_delivery_staff, run_id)
    mart_logistics_performance.select(*spark.table("lakehouse.gold.mart_logistics_performance").columns).writeTo(
        "lakehouse.gold.mart_logistics_performance"
    ).overwrite(F.lit(True))

    print(f"[{run_id}] [Marts] Building mart_product_returns...")
    mart_product_returns = build_mart_product_returns(fact_return_exchange, dim_variant, run_id)
    mart_product_returns.select(*spark.table("lakehouse.gold.mart_product_returns").columns).writeTo(
        "lakehouse.gold.mart_product_returns"
    ).overwrite(F.lit(True))

    print(f"[{run_id}] [Marts] Building mart_inventory_health...")
    mart_inventory_health = build_mart_inventory_health(fact_inventory_daily_snapshot, dim_product, run_id)
    mart_inventory_health.select(*spark.table("lakehouse.gold.mart_inventory_health").columns).writeTo(
        "lakehouse.gold.mart_inventory_health"
    ).overwrite(F.lit(True))

    current = build_mart_inventory_current(
        spark.table("lakehouse.gold.fact_inventory_current"), dim_variant, dim_product, run_id
    )
    current.select(*spark.table("lakehouse.gold.mart_inventory_current").columns).writeTo(
        "lakehouse.gold.mart_inventory_current"
    ).overwrite(F.lit(True))
    summary = build_mart_inventory_summary(
        spark.table("lakehouse.gold.mart_inventory_current"),
        spark.table("lakehouse.silver.silver_inbound_receipts"),
        run_id,
    )
    summary.select(*spark.table("lakehouse.gold.mart_inventory_summary").columns).writeTo(
        "lakehouse.gold.mart_inventory_summary"
    ).overwrite(F.lit(True))
    store = build_mart_store_overview(
        spark.table("lakehouse.gold.dim_store"),
        fact_order,
        spark.table("lakehouse.silver.silver_customers"),
        spark.table("lakehouse.gold.mart_inventory_current"),
        run_id,
    )
    store.select(*spark.table("lakehouse.gold.mart_store_overview").columns).writeTo(
        "lakehouse.gold.mart_store_overview"
    ).overwrite(F.lit(True))
    print(f"[{run_id}] [Marts] All 9 Gold Data Marts successfully built!")
