"""Pure, publication-pinned datasets and metric definitions for the sales PoC."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

ORDER_TABLE = "lakehouse.gold.mart_order_daily"
PRODUCT_TABLE = "lakehouse.gold.mart_product_sales_daily"
TABLES = {"orders": ORDER_TABLE, "products": PRODUCT_TABLE}
METRICS = {
    "total_orders": ("Tổng số đơn", "SUM(total_orders)", ",.0f"),
    "order_total_vnd": ("Giá trị đặt hàng (VND)", "SUM(order_total_vnd)", ",.0f"),
    "net_revenue_vnd": ("Doanh thu thuần (VND)", "SUM(net_revenue_vnd)", ",.0f"),
    "gross_profit_vnd": ("Lợi nhuận gộp (VND)", "SUM(gross_profit_vnd)", ",.0f"),
}
ORDER_COLUMNS = {
    "order_date": "DATE",
    "channel": "VARCHAR",
    "store_key": "BIGINT",
    "store_name": "VARCHAR",
    **{name: "BIGINT" for name in METRICS},
}
PRODUCT_COLUMNS = {
    "order_date": "DATE",
    "product_id": "BIGINT",
    "product_name": "VARCHAR",
    "net_revenue_vnd": "BIGINT",
    "gross_profit_vnd": "BIGINT",
}
COLUMNS = {"orders": ORDER_COLUMNS, "products": PRODUCT_COLUMNS}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class Publication:
    run_id: str
    published_at: str
    fingerprint: str
    snapshots: dict

    @classmethod
    def parse(cls, row):
        if row.get("schema_version") != 3 or row.get("source_mode") != "batch":
            raise ValueError("A completed schema-v3 OLTP batch publication is required")
        run_id = row.get("run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("Publication run ID is required")
        snapshots = json.loads(row["snapshots_json"])
        if not isinstance(snapshots, dict):
            raise ValueError("Publication snapshots must be an object")
        for table in TABLES.values():
            if table not in snapshots:
                raise ValueError("Missing published sales table: " + table)
            snapshot = snapshots[table]
            if snapshot is not None and (type(snapshot) is not int or snapshot <= 0):
                raise ValueError("Invalid snapshot ID: " + table)
        published = str(row["published_at"])
        datetime.fromisoformat(published.replace(" UTC", "+00:00"))
        fingerprint = hashlib.sha256(
            json.dumps({"run_id": run_id, "snapshots": snapshots}, sort_keys=True).encode()
        ).hexdigest()[:24]
        return cls(run_id, published, fingerprint, snapshots)

    def dataset_sql(self, name):
        table = TABLES[name]
        snapshot = self.snapshots[table]
        source = (
            table + f" FOR VERSION AS OF {snapshot}" if snapshot is not None else f"(SELECT * FROM {table} WHERE false)"
        )
        return (
            "SELECT "
            + ", ".join(COLUMNS[name])
            + ", "
            + literal(self.run_id)
            + " AS publication_run_id, "
            + literal(self.published_at)
            + " AS publication_at_utc FROM "
            + source
        )


def chart_specs():
    specs = [
        dict(key=name, title=label, dataset="orders", viz="big_number_total", metric=name, value_format=fmt)
        for name, (label, _, fmt) in METRICS.items()
    ]
    specs.extend(
        [
            dict(
                key="trend",
                title="Doanh thu và lợi nhuận theo ngày (VND)",
                dataset="orders",
                viz="echarts_timeseries_line",
                metrics=["net_revenue_vnd", "gross_profit_vnd"],
            ),
            dict(
                key="channel",
                title="Doanh thu thuần theo kênh (VND)",
                dataset="orders",
                viz="echarts_timeseries_bar",
                groupby=["channel"],
                metrics=["net_revenue_vnd"],
            ),
            dict(
                key="store",
                title="Doanh thu thuần theo điểm bán (VND)",
                dataset="orders",
                viz="echarts_timeseries_bar",
                groupby=["store_name"],
                metrics=["net_revenue_vnd"],
            ),
            dict(
                key="product",
                title="Top 10 sản phẩm theo doanh thu thuần (VND)",
                dataset="products",
                viz="echarts_timeseries_bar",
                groupby=["product_name"],
                metrics=["net_revenue_vnd"],
            ),
        ]
    )
    return specs
