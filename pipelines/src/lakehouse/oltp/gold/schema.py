"""Reviewed analytical table names and Iceberg column contracts."""

from lakehouse.utils.sql import load_sql
from lakehouse.oltp.polars.schema import ddl_schema

TABLES = (
    "dim_date",
    "dim_product",
    "dim_variant",
    "dim_store",
    "dim_delivery_staff",
    "dim_customer",
    "fact_order_item",
    "fact_order",
    "fact_shipment",
    "fact_return_exchange",
    "fact_inventory_daily_snapshot",
    "fact_inventory_current",
    "mart_sales_daily",
    "mart_order_daily",
    "mart_product_sales_daily",
    "mart_logistics_performance",
    "mart_product_returns",
    "mart_inventory_health",
    "mart_inventory_current",
    "mart_inventory_summary",
    "mart_store_overview",
    "mart_operations_summary",
    "mart_cod_region_daily",
    "mart_marketing_summary",
    "mart_coupon_usage",
    "mart_product_review_summary",
    "mart_admin_overview",
)
GOLD_TABLE_DDL = {name: load_sql("contracts/gold/" + name + ".sql") for name in TABLES}


def schema(name):
    if name not in TABLES:
        raise ValueError("Unknown Gold table")
    return ddl_schema("contracts/gold/" + name + ".sql")
