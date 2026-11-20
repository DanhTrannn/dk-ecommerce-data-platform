"""Sales PoC datasets over the current Gold marts, and their metric and chart definitions.

Gold is only rebuilt after the batch checks pass, so datasets read the current tables directly.
"""

import hashlib
import json

TABLES = {
    "orders": "lakehouse.gold.mart_orders_daily",
    "sales": "lakehouse.gold.mart_sales_daily",
    "products": "lakehouse.gold.mart_product_sales_daily",
}
# Orders count on the day they were placed; revenue on the day it was recognized (completed and paid).
DATE_COLUMNS = {"orders": "order_date", "sales": "revenue_date", "products": "revenue_date"}
METRICS = {
    "orders": ("Số đơn đặt", "SUM(orders)", ",.0f"),
    "order_value_vnd": ("Giá trị đặt hàng (VND)", "SUM(order_value_vnd)", ",.0f"),
    "net_revenue_vnd": ("Doanh thu thuần (VND)", "SUM(net_revenue_vnd)", ",.0f"),
    "gross_profit_vnd": ("Lợi nhuận gộp (VND)", "SUM(gross_profit_vnd)", ",.0f"),
    "revenue_vnd": ("Doanh thu sau giảm giá (VND)", "SUM(revenue_vnd)", ",.0f"),
}
COLUMNS = {
    "orders": {
        "order_date": "DATE",
        "channel": "VARCHAR",
        "store_name": "VARCHAR",
        "city_name": "VARCHAR",
        "payment_method": "VARCHAR",
        "orders": "BIGINT",
        "order_value_vnd": "BIGINT",
    },
    "sales": {
        "revenue_date": "DATE",
        "channel": "VARCHAR",
        "store_name": "VARCHAR",
        "city_name": "VARCHAR",
        "net_revenue_vnd": "BIGINT",
        "gross_profit_vnd": "BIGINT",
    },
    "products": {
        "revenue_date": "DATE",
        "channel": "VARCHAR",
        "product_name": "VARCHAR",
        "category_name": "VARCHAR",
        "revenue_vnd": "BIGINT",
        "gross_profit_vnd": "BIGINT",
    },
}
# Online orders belong to no store; label them instead of showing an empty group.
ONLINE_STORE = "Online"


def dataset_sql(name):
    columns = [
        f"coalesce(store_name, '{ONLINE_STORE}') AS store_name" if column == "store_name" else column
        for column in COLUMNS[name]
    ]
    return "SELECT " + ", ".join(columns) + " FROM " + TABLES[name]


def chart_specs():
    specs = [
        dict(key="orders", title="Số đơn đặt", dataset="orders", viz="big_number_total", metric="orders"),
        dict(key="order_value", title="Giá trị đặt hàng (VND)", dataset="orders", viz="big_number_total", metric="order_value_vnd"),
        dict(key="net_revenue", title="Doanh thu thuần (VND)", dataset="sales", viz="big_number_total", metric="net_revenue_vnd"),
        dict(key="gross_profit", title="Lợi nhuận gộp (VND)", dataset="sales", viz="big_number_total", metric="gross_profit_vnd"),
        dict(
            key="trend",
            title="Doanh thu thuần và lợi nhuận gộp theo ngày ghi nhận (VND)",
            dataset="sales",
            viz="echarts_timeseries_line",
            metrics=["net_revenue_vnd", "gross_profit_vnd"],
        ),
        dict(
            key="channel",
            title="Doanh thu thuần theo kênh (VND)",
            dataset="sales",
            viz="echarts_timeseries_bar",
            groupby=["channel"],
            metrics=["net_revenue_vnd"],
        ),
        dict(
            key="store",
            title="Doanh thu thuần theo điểm bán (VND)",
            dataset="sales",
            viz="echarts_timeseries_bar",
            groupby=["store_name"],
            metrics=["net_revenue_vnd"],
        ),
        dict(
            key="product",
            title="Top 10 sản phẩm theo doanh thu sau giảm giá (VND)",
            dataset="products",
            viz="echarts_timeseries_bar",
            groupby=["product_name"],
            metrics=["revenue_vnd"],
        ),
    ]
    for spec in specs:
        spec["date_column"] = DATE_COLUMNS[spec["dataset"]]
        if "metric" in spec:
            spec["value_format"] = METRICS[spec["metric"]][2]
    return specs


def fingerprint():
    """Identify the asset definitions; a changed definition creates new Superset assets."""
    definition = dict(tables=TABLES, columns=COLUMNS, metrics=METRICS, charts=chart_specs())
    return hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()[:24]
