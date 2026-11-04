from __future__ import annotations

from lakehouse.sql import load_sql
import re


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

GOLD_TABLE_DDL: dict[str, str] = {}

# ==========================================
# 1. CONFORMED DIMENSIONS
# ==========================================

GOLD_TABLE_DDL["dim_date"] = load_sql("spark/gold/dim_date.sql")

GOLD_TABLE_DDL["dim_product"] = load_sql("spark/gold/dim_product.sql")

GOLD_TABLE_DDL["dim_variant"] = load_sql("spark/gold/dim_variant.sql")

GOLD_TABLE_DDL["dim_store"] = load_sql("spark/gold/dim_store.sql")

GOLD_TABLE_DDL["dim_delivery_staff"] = load_sql("spark/gold/dim_delivery_staff.sql")

GOLD_TABLE_DDL["dim_customer"] = load_sql("spark/gold/dim_customer.sql")

# ==========================================
# 2. CORE FACTS
# ==========================================

GOLD_TABLE_DDL["fact_order_item"] = load_sql("spark/gold/fact_order_item.sql")

GOLD_TABLE_DDL["fact_order"] = load_sql("spark/gold/fact_order.sql")

GOLD_TABLE_DDL["fact_shipment"] = load_sql("spark/gold/fact_shipment.sql")

GOLD_TABLE_DDL["fact_return_exchange"] = load_sql("spark/gold/fact_return_exchange.sql")

GOLD_TABLE_DDL["fact_inventory_daily_snapshot"] = load_sql("spark/gold/fact_inventory_daily_snapshot.sql")
GOLD_TABLE_DDL["fact_inventory_current"] = load_sql("spark/gold/fact_inventory_current.sql")

# ==========================================
# 3. DATA MARTS
# ==========================================

GOLD_TABLE_DDL["mart_sales_daily"] = load_sql("spark/gold/mart_sales_daily.sql")
GOLD_TABLE_DDL["mart_order_daily"] = load_sql("spark/gold/mart_order_daily.sql")
GOLD_TABLE_DDL["mart_product_sales_daily"] = load_sql("spark/gold/mart_product_sales_daily.sql")

GOLD_TABLE_DDL["mart_logistics_performance"] = load_sql("spark/gold/mart_logistics_performance.sql")

GOLD_TABLE_DDL["mart_product_returns"] = load_sql("spark/gold/mart_product_returns.sql")

GOLD_TABLE_DDL["mart_inventory_health"] = load_sql("spark/gold/mart_inventory_health.sql")
GOLD_TABLE_DDL["mart_inventory_current"] = load_sql("spark/gold/mart_inventory_current.sql")
GOLD_TABLE_DDL["mart_inventory_summary"] = load_sql("spark/gold/mart_inventory_summary.sql")
GOLD_TABLE_DDL["mart_store_overview"] = load_sql("spark/gold/mart_store_overview.sql")
GOLD_TABLE_DDL["mart_operations_summary"] = load_sql("spark/gold/mart_operations_summary.sql")
GOLD_TABLE_DDL["mart_cod_region_daily"] = load_sql("spark/gold/mart_cod_region_daily.sql")


GOLD_TABLE_DDL["mart_marketing_summary"] = load_sql("spark/gold/mart_marketing_summary.sql")
GOLD_TABLE_DDL["mart_coupon_usage"] = load_sql("spark/gold/mart_coupon_usage.sql")
GOLD_TABLE_DDL["mart_product_review_summary"] = load_sql("spark/gold/mart_product_review_summary.sql")


def ensure_oltp_gold_tables(spark: SparkSession, tables=None) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    for table_name in tables if tables is not None else GOLD_TABLE_DDL:
        ddl = GOLD_TABLE_DDL[table_name]
        spark.sql(ddl)
        # Additive repair for environments created before the module refactor.
        # Existing rows/snapshots are preserved; the next Gold build fills fields.
        existing = {field.name.lower() for field in spark.table(f"lakehouse.gold.{table_name}").schema.fields}
        for name, kind in re.findall(
            r"^    ([a-z_]+)\s+(BIGINT|INT|STRING|BOOLEAN|TIMESTAMP|DATE|DOUBLE)\b", ddl, re.M
        ):
            if name not in existing:
                spark.sql(f"ALTER TABLE lakehouse.gold.{table_name} ADD COLUMN {name} {kind}")
        if table_name == "fact_order_item":
            columns = {field.name.lower() for field in spark.table("lakehouse.gold.fact_order_item").schema.fields}
            if "order_date" not in columns:
                raise RuntimeError(
                    "fact_order_item requires an explicit schema migration; automatic table deletion is disabled"
                )
