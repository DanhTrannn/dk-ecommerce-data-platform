import hashlib
import json
from dataclasses import asdict, dataclass
from types import SimpleNamespace

from dashboard import chart_payload, layout
from sales import COLUMNS, DATE_COLUMNS, METRICS, chart_specs, dataset_sql


@dataclass(frozen=True)
class Dataset:
    sql: str
    date: str
    columns: dict
    metrics: dict


@dataclass(frozen=True)
class DashboardSpec:
    title: str
    description: str
    rows: tuple

    @property
    def charts(self):
        return [
            {**chart, "date_column": DATASETS[chart["dataset"]].date}
            for row in self.rows for chart in row
        ]

    def fingerprint(self):
        charts = self.charts
        datasets = {chart["dataset"]: asdict(DATASETS[chart["dataset"]]) for chart in charts}
        placeholders = [
            SimpleNamespace(id=i, uuid=i, slice_name=chart["title"], viz_type=chart["viz"])
            for i, chart in enumerate(charts)
        ]
        definition = {
            "dashboard": asdict(self), "datasets": datasets,
            "chart_payloads": [chart_payload(chart, 0) for chart in charts],
            "layout": layout(placeholders, title=self.title, description=self.description, rows=self.rows),
        }
        return hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()[:24]


def sums(**labels):
    return {key: (label, f"SUM({key})", ",.0f") for key, label in labels.items()}


def ratio(label, numerator, denominator):
    return label, f"CAST({numerator} AS double) / NULLIF({denominator}, 0)", ".1%"


def dataset(table, date, metrics, *, source=None, **columns):
    relation = source or "lakehouse.gold.mart_" + table
    columns = {date: "DATE", **columns}
    return Dataset(f"SELECT {', '.join(columns)} FROM {relation}", date, columns, metrics)


DATASETS = {
    name: Dataset(dataset_sql(name), DATE_COLUMNS[name], columns.copy(), {
        key: value for key, value in METRICS.items() if key in columns
    })
    for name, columns in COLUMNS.items()
}
order_status = sums(
    completed_orders="Hoàn tất", cancelled_orders="Đã hủy",
    failed_orders="Thất bại", processing_orders="Đang xử lý",
)
orders = DATASETS["orders"]
DATASETS["orders"] = Dataset(
    orders.sql.replace(" FROM ", f", {', '.join(order_status)} FROM ", 1),
    orders.date, {**orders.columns, **dict.fromkeys(order_status, "BIGINT")}, {**orders.metrics, **order_status},
)
products = DATASETS["products"]
DATASETS["products"] = Dataset(
    products.sql.replace(" FROM ", ", refund_vnd FROM ", 1),
    products.date,
    {**products.columns, "refund_vnd": "BIGINT"},
    {
        **products.metrics,
        "product_net_revenue": ("Doanh thu thuần", "SUM(revenue_vnd) - SUM(refund_vnd)", ",.0f"),
        "gross_margin": ratio("Biên lợi nhuận gộp", "SUM(gross_profit_vnd)", "SUM(revenue_vnd) - SUM(refund_vnd)"),
    },
)
DATASETS.update(
    profit_bridge=dataset(
        "sales_daily", "revenue_date", sums(profit_delta="Biến động (VND)"),
        source=(
            "lakehouse.gold.mart_sales_daily CROSS JOIN UNNEST("
            "ARRAY['1. Doanh thu hàng hóa', '2. Giảm giá', '3. Hoàn tiền', '4. Giá vốn', '5. Giá vốn hàng trả'], "
            "ARRAY[gross_revenue_vnd, -discount_vnd, -refund_vnd, -cogs_vnd, returned_cogs_vnd]"
            ") AS steps(profit_step, profit_delta)"
        ),
        profit_step="VARCHAR", profit_delta="BIGINT",
    ),
    refunds=dataset("sales_daily", "revenue_date", sums(refund_vnd="Hoàn tiền (VND)"), refund_vnd="BIGINT"),
    cancellations=dataset(
        "cancellations_daily", "order_date", sums(cancelled_orders="Đơn hủy"),
        source="(SELECT order_date, coalesce(cancel_reason, 'Chưa rõ') AS cancel_reason, cancelled_orders "
        "FROM lakehouse.gold.mart_cancellations_daily) cancelled",
        cancel_reason="VARCHAR", cancelled_orders="BIGINT",
    ),
    returns=dataset(
        "returns_daily", "request_date", sums(quantity="Số lượng yêu cầu đổi/trả"),
        product_name="VARCHAR", quantity="BIGINT",
    ),
    delivery=dataset(
        "delivery_daily", "end_date", {
            "on_time_rate": ratio("Tỷ lệ giao đúng hạn", "SUM(on_time_shipments)", "SUM(delivered_shipments)"),
        },
        city_name="VARCHAR", on_time_shipments="BIGINT", delivered_shipments="BIGINT",
    ),
    backlog=dataset(
        "fulfilment_backlog_daily", "backlog_date", {
            f"backlog_{key}": (label, f"SUM(CASE WHEN age_bucket_days = '{bucket}' THEN orders ELSE 0 END)", ",.0f")
            for key, label, bucket in [
                ("0_4", "0–4 ngày", "0-4"), ("5_8", "5–8 ngày", "5-8"), ("9_plus", "Từ 9 ngày", "9+"),
            ]
        },
        stage="VARCHAR", age_bucket_days="VARCHAR", orders="BIGINT",
    ),
    inventory=dataset(
        "inventory_current", "inventory_date", sums(stock_value_vnd="Giá trị tồn (VND)"),
        source="(SELECT inventory_date, coalesce(store_name, 'Kho trung tâm') AS location, sku, "
        "product_name, on_hand_qty, round(days_of_cover, 1) AS days_of_cover, stock_value_vnd, "
        "CASE WHEN on_hand_qty = 0 THEN 'Hết hàng' WHEN days_of_cover IS NULL THEN 'Không có bán trong 28 ngày' "
        "WHEN days_of_cover <= 14 THEN 'Đủ bán tối đa 14 ngày' "
        "ELSE 'Đủ bán trên 14 ngày' END AS stock_status FROM lakehouse.gold.mart_inventory_current) stock",
        location="VARCHAR", sku="VARCHAR", product_name="VARCHAR", on_hand_qty="BIGINT",
        days_of_cover="DOUBLE", stock_value_vnd="BIGINT", stock_status="VARCHAR",
    ),
    stock_health=dataset(
        "inventory_health_daily", "inventory_date", {
            "out_of_stock_rate": ratio(
                "Tỷ lệ vị trí × SKU hết hàng", "SUM(variants_out_of_stock)",
                "SUM(variants_in_stock) + SUM(variants_out_of_stock)",
            ),
        },
        variants_in_stock="BIGINT", variants_out_of_stock="BIGINT",
    ),
    pos=dataset(
        "pos_hourly", "order_date", sums(orders="Đơn POS", revenue_vnd="Doanh thu sau giảm giá (VND)"),
        order_hour="INTEGER", store_name="VARCHAR", sales_staff_name="VARCHAR", orders="BIGINT", revenue_vnd="BIGINT",
    ),
    cohort=dataset(
        "customer_cohort_monthly", "cohort_month", {
            "retention_rate": ratio("Tỷ lệ hoạt động", "MAX(active_customers)", "MAX(cohort_size)"),
        },
        source="(SELECT cohort_month, months_since_first_order, active_customers, "
        "MAX(CASE WHEN months_since_first_order = 0 THEN active_customers END) OVER (PARTITION BY cohort_month) "
        "AS cohort_size FROM lakehouse.gold.mart_customer_cohort_monthly) cohorts",
        months_since_first_order="BIGINT", active_customers="BIGINT", cohort_size="BIGINT",
    ),
    customers=dataset(
        "customer_value", "first_order_date", {
            "repeat_rate": ratio("Tỷ lệ khách mua lặp lại", "COUNT_IF(is_repeat_customer)", "COUNT(*)"),
        },
        is_repeat_customer="BOOLEAN",
    ),
    coupons=dataset(
        "coupon_daily", "order_date", sums(discount_vnd="Giảm giá (VND)"),
        coupon_code="VARCHAR", discount_vnd="BIGINT",
    ),
    reviews=dataset(
        "reviews_daily", "review_date", sums(**{f"rating_{n}": f"{n} sao" for n in range(1, 6)}),
        delivery_timeliness="VARCHAR", **dict.fromkeys((f"rating_{n}" for n in range(1, 6)), "BIGINT"),
    ),
)


def chart(key, title, source, viz, **options):
    return {"key": key, "title": title, "dataset": source, "viz": viz, **options}


def bar(key, title, source, group, *metrics, **options):
    return chart(key, title, source, "echarts_timeseries_bar", groupby=[group], metrics=list(metrics), **options)


def trend(key, title, source, *metrics, stack=False, **options):
    viz = "echarts_timeseries_bar" if stack else "echarts_timeseries_line"
    return chart(key, title, source, viz, metrics=list(metrics), stack=stack, **options)


def heatmap(key, title, source, x, y, metric, **options):
    return chart(key, title, source, "heatmap_v2", x_axis=x, y_axis=y, metric=metric, **options)


def backlog_chart(stage, title):
    return trend(
        stage, title, "backlog", "backlog_0_4", "backlog_5_8", "backlog_9_plus", stack=True,
        filters=[{"col": "stage", "op": "==", "val": stage}],
    )


sales = chart_specs()
DASHBOARDS = {
    "sales": DashboardSpec(
        "Doanh thu bán lẻ",
        "Đơn theo ngày đặt. Doanh thu theo ngày ghi nhận và tiền hoàn theo ngày hoàn. Đơn vị tiền tệ là VND.",
        (
            tuple(sales[:4]),
            (sales[4],),
            (trend("order_status", "Số đơn theo ngày và trạng thái", "orders", *order_status, stack=True),),
            (chart("profit_bridge", "Từ doanh thu hàng hóa đến lợi nhuận gộp", "profit_bridge",
                   "waterfall", x_axis="profit_step", metric="profit_delta"),),
            tuple(sales[5:7]),
            (sales[7], bar("category_sales", "Doanh thu thuần theo danh mục (VND)",
                           "products", "category_name", "product_net_revenue")),
            (bar("category_margin", "Biên lợi nhuận gộp theo danh mục",
                 "products", "category_name", "gross_margin", value_format=".1%", row_limit=100),),
        ),
    ),
    "operations": DashboardSpec(
        "Vận hành bán lẻ",
        "Tồn hiện tại là ảnh chụp ngày batch. Tỷ lệ giao đúng hạn tính trên các vận đơn đã giao.",
        (
            (bar("stock_value", "Giá trị tồn hiện tại theo kho và cửa hàng (VND)",
                 "inventory", "location", "stock_value_vnd", row_limit=100),
             trend("out_of_stock", "Tỷ lệ hết hàng theo ngày", "stock_health", "out_of_stock_rate", value_format=".1%")),
            (chart("low_stock", "SKU theo số ngày đủ bán và lượng tồn tại từng vị trí", "inventory", "table",
                   columns=["sku", "product_name", "location", "on_hand_qty", "days_of_cover", "stock_status"],
                   sort_by=["days_of_cover", "on_hand_qty", "sku", "location"], row_limit=25),),
            (bar("on_time", "Tỷ lệ giao đúng hạn theo thành phố", "delivery", "city_name",
                 "on_time_rate", value_format=".1%", row_limit=100),),
            (backlog_chart("awaiting_dispatch", "Đơn chờ xuất kho theo tuổi"),
             backlog_chart("in_delivery", "Đơn đang giao theo tuổi")),
            (bar("cancel_reasons", "Top lý do hủy đơn", "cancellations", "cancel_reason", "cancelled_orders"),
             bar("returned_products", "Top sản phẩm theo số lượng yêu cầu đổi/trả", "returns", "product_name", "quantity")),
            (trend("refunds", "Tiền hoàn theo ngày hoàn thành công (VND)", "refunds", "refund_vnd"),),
        ),
    ),
    "pos": DashboardSpec(
        "Bán hàng tại cửa hàng (POS)",
        "Số đơn và doanh thu của đơn POS đã ghi nhận. Giờ đặt theo múi giờ Việt Nam.",
        (
            (heatmap("pos_hours", "Số đơn theo giờ và cửa hàng", "pos", "order_hour", "store_name", "orders"),),
            (bar("pos_staff", "Top nhân viên theo doanh thu POS (VND)",
                 "pos", "sales_staff_name", "revenue_vnd"),),
        ),
    ),
    "customers": DashboardSpec(
        "Khách hàng và marketing",
        "Lọc khách theo tháng mua đầu tiên. Tỷ lệ mua lặp lại tính trên toàn bộ lịch sử của nhóm khách đã chọn. "
        "Heatmap để trống các tháng thiếu dữ liệu hoạt động.",
        (
            (chart("repeat_rate", "Tỷ lệ khách mua lặp lại", "customers", "big_number_total",
                   metric="repeat_rate", value_format=".1%"),),
            (heatmap("cohort_retention", "Tỷ lệ quay lại theo tháng mua đầu tiên",
                     "cohort", "months_since_first_order", "cohort_month", "retention_rate", value_format=".1%"),),
            (bar("coupon_cost", "Top coupon theo tổng giảm giá (VND)", "coupons", "coupon_code", "discount_vnd"),
             bar("review_stars", "Số lượt đánh giá theo mức sao và tình trạng giao hàng",
                 "reviews", "delivery_timeliness", *(f"rating_{n}" for n in range(1, 6)), stack=True)),
        ),
    ),
}
