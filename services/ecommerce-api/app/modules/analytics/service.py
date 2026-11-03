"""Analytical serving through Trino; MySQL is used only for source reconciliation.

Business queries use one completed batch or CDC publication, including typed Silver
snapshots for signals not yet modeled as dedicated Gold marts.
"""

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.errors import FORBIDDEN, VALIDATION_ERROR, AppError
from app.models.customer import Customer
from app.models.order import Order
from app.modules.admin.schemas import AdminOverviewResponse
from app.modules.analytics.reader import serve
from app.modules.analytics.inventory_service import (
    get_active_stores_list as get_active_stores_list,
    get_store_overview as get_store_overview,
    get_store_metrics, get_inventory_metrics,
)
from app.modules.analytics.schemas import (
    CashInTransitSignal,
    CategoryShareMetric,
    DailySalesTrendPoint,
    ExecutiveMetricsResponse,
    FulfillmentBottleneckSignal,
    FunnelStep,
    MarketingMetricsResponse,
    NegativeReviewSpikeSignal,
    OperationsMetricsResponse,
    ReconciliationVariance,
    RegionalBoomRateSignal,
    SalesMetricsResponse,
    SalesTrendResponse,
    StoreContribution,
    SystemMetricsResponse,
    TopProductMetric,
    VoucherBurnRateSignal,
)
from app.modules.analytics.trino_client import TrinoClient

ROLE_CANONICAL_MAP = {
    "executive": "executive",
    "admin": "executive",
    "sales": "sales",
    "sales_manager": "sales",
    "marketing": "marketing",
    "marketing_manager": "marketing",
    "store": "store",
    "store_manager": "store",
    "inventory": "inventory",
    "inventory_manager": "inventory",
    "operations": "operations",
    "operations_manager": "operations",
    "system": "system",
    "system_admin": "system",
}
VN_TODAY = "CAST(current_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh' AS date)"
NOW_UTC = "CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)"


def validate_role_access(actor: Customer, target_role: str, store_id: int | None = None) -> None:
    target = ROLE_CANONICAL_MAP.get(target_role.strip().lower())
    if target is None:
        raise AppError(VALIDATION_ERROR, "Vai trò báo cáo không hợp lệ.", status_code=400)
    if actor.role == "admin":
        return
    if ROLE_CANONICAL_MAP.get(actor.role) != target:
        raise AppError(FORBIDDEN, "Bạn không có quyền xem góc nhìn này.", status_code=403)
    if actor.role == "store_manager" and (
        actor.store_id is None or (store_id is not None and store_id != actor.store_id)
    ):
        raise AppError(FORBIDDEN, "Bạn chỉ được xem cửa hàng được phân công.", status_code=403)


def resolve_effective_store_id(actor: Customer, target_role: str, store_id: int | None) -> int | None:
    if ROLE_CANONICAL_MAP.get(target_role.strip().lower()) == "store" and actor.role == "store_manager":
        if actor.store_id is None:
            raise AppError(FORBIDDEN, "Tài khoản chưa được gán cửa hàng.", status_code=403)
        return actor.store_id
    return store_id


def _n(row, key):
    return int(row.get(key) or 0)


def _pct(part, total):
    return round(part / total * 100, 2) if total > 0 else 0.0


def _mart_ratio(part, total):
    return round(part / total * 100, 2) if total is not None and total > 0 else None


def get_executive_metrics(db: Session | None = None, trino_client: TrinoClient | None = None):
    def build(reader):
        row = reader.one("""SELECT count(*) group_count,
            SUM(order_total_vnd) gmv, SUM(net_revenue_vnd) revenue,
            SUM(cogs_vnd) cogs, SUM(gross_profit_vnd) profit, SUM(total_orders) orders,
            SUM(recognized_orders) recognized_orders, SUM(boom_orders) boom,
            SUM(return_requests_count) returns, SUM(cod_in_transit_vnd) cod,
            SUM(cod_shipments_count) shipments FROM lakehouse.gold.mart_order_daily""")
        if not row or not row["group_count"]:
            return ExecutiveMetricsResponse()
        return ExecutiveMetricsResponse(
            gmv_vnd=row["gmv"],
            net_revenue_vnd=row["revenue"],
            cogs_vnd=row["cogs"],
            gross_profit_vnd=row["profit"],
            total_orders=row["orders"],
            gross_margin_percent=_mart_ratio(row["profit"], row["revenue"]),
            aov_vnd=round(row["revenue"] / row["recognized_orders"]) if row["recognized_orders"] else None,
            boom_rate_percent=_mart_ratio(row["boom"], row["orders"]),
            return_rate_percent=_mart_ratio(row["returns"], row["orders"]),
            cash_in_transit=CashInTransitSignal(
                total_cod_amount_vnd=row["cod"], dispatched_shipments_count=row["shipments"]
            ),
        )

    return serve(("executive",), build, trino_client)


def get_sales_metrics(db: Session | None = None, trino_client: TrinoClient | None = None):
    def build(reader):
        stores = reader.query("""SELECT store_key store_id,store_name,
            SUM(net_revenue_vnd) revenue_vnd,SUM(revenue_orders) order_count
            FROM lakehouse.gold.mart_order_daily GROUP BY 1,2 ORDER BY revenue_vnd DESC,store_id""")
        products = reader.query("""SELECT product_id,product_name,SUM(units_sold) units_sold,
            SUM(net_revenue_vnd) revenue_vnd FROM lakehouse.gold.mart_product_sales_daily
            GROUP BY 1,2 ORDER BY revenue_vnd DESC,product_id LIMIT 10""")
        cats = reader.query("""SELECT category_id,category_name,SUM(net_revenue_vnd) revenue_vnd
            FROM lakehouse.gold.mart_sales_daily GROUP BY 1,2 ORDER BY revenue_vnd DESC,category_id""")
        total = sum(c["revenue_vnd"] for c in cats)
        return SalesMetricsResponse(
            store_contributions=[StoreContribution(**{**r, "store_id": r["store_id"] or None}) for r in stores],
            top_selling_products=[TopProductMetric(**r) for r in products],
            category_shares=[
                CategoryShareMetric(**r, share_percent=_mart_ratio(r["revenue_vnd"], total)) for r in cats
            ],
        )

    return serve(("sales",), build, trino_client)


def get_marketing_metrics(db: Session | None = None, trino_client: TrinoClient | None = None):
    def build(reader):
        row = reader.one("""SELECT
            count_if(ecommerce_action IN ('product_detail','catalog_search') OR http_route LIKE '/api/v1/products%') views,
            count_if(ecommerce_action IN ('cart_item_set','cart_add') OR http_route LIKE '/api/v1/cart%') carts,
            count_if(ecommerce_action IN ('checkout_quote','checkout_submit') OR http_route LIKE '/api/v1/checkout%') checkouts,
            count_if(ecommerce_action IN ('order_complete','order_confirm_internal') OR (ecommerce_action='checkout_submit' AND is_success)) purchases
            FROM lakehouse.gold.fact_web_events WHERE event_date >= current_date - INTERVAL '29' DAY""")
        counts = [_n(row, k) for k in ("views", "carts", "checkouts", "purchases")]
        labels = [
            "Lượt xem sản phẩm (Product Views)",
            "Thêm vào giỏ (Add to Cart)",
            "Tiến hành thanh toán (Checkout)",
            "Đặt hàng thành công (Purchased)",
        ]
        coupons = reader.query(f"""SELECT c.coupon_id,c.code_normalized,c.used_count,c.total_usage_limit,
            count_if(r.redeemed_at >= {NOW_UTC} - INTERVAL '15' MINUTE) recent
            FROM lakehouse.silver.silver_coupons c LEFT JOIN lakehouse.silver.silver_coupon_redemptions r ON c.coupon_id=r.coupon_id
            WHERE c.is_active GROUP BY c.coupon_id,c.code_normalized,c.used_count,c.total_usage_limit
            ORDER BY c.used_count DESC,c.coupon_id LIMIT 1""")
        voucher = None
        if coupons:
            c = coupons[0]
            limit = _n(c, "total_usage_limit")
            used = _n(c, "used_count")
            voucher = VoucherBurnRateSignal(
                coupon_code=c["code_normalized"],
                used_count=used,
                usage_limit=limit,
                burn_rate_per_minute=round(_n(c, "recent") / 15, 1),
                budget_burn_percent=_pct(used, limit),
                budget_warning=limit > 0 and used / limit >= 0.8,
            )
        reviews = reader.query(f"""SELECT r.product_id,p.product_name,count(*) negative_count
            FROM lakehouse.silver.silver_product_reviews r JOIN lakehouse.gold.dim_product p ON r.product_id=p.product_id
            WHERE r.rating<=2 AND r.created_at >= {NOW_UTC} - INTERVAL '45' MINUTE
            GROUP BY r.product_id,p.product_name ORDER BY negative_count DESC LIMIT 5""")
        return MarketingMetricsResponse(
            funnel_steps=[
                FunnelStep(step_name=label, count=count, conversion_rate_percent=_pct(count, counts[0]))
                for label, count in zip(labels, counts, strict=True)
            ],
            total_visitors=counts[0],
            total_purchases=counts[3],
            conversion_rate_percent=_pct(counts[3], counts[0]),
            funnel_window_days=30,
            voucher_burn_rate=voucher,
            negative_review_spikes=[NegativeReviewSpikeSignal(**r, window_minutes=45) for r in reviews],
        )

    return serve(("marketing",), build, trino_client)


def get_operations_metrics(db=None, trino_client=None):
    def build(reader):
        row = reader.one(f"""SELECT count(*) pending,count_if(created_at <= {NOW_UTC}-INTERVAL '2' HOUR) stale,
            COALESCE(AVG(date_diff('second',created_at,{NOW_UTC})/3600.0),0) waiting
            FROM lakehouse.gold.fact_order WHERE status IN ('paid','confirmed')""")
        shipping = reader.one("""SELECT count_if(attempt_count>1 OR status='failed') problematic,
            count_if(status='failed') failed FROM lakehouse.gold.fact_shipment""")
        returns = reader.one(
            "SELECT count(DISTINCT return_id) n FROM lakehouse.silver.silver_return_requests WHERE status<>'cancelled'"
        )
        regions = reader.query("""SELECT COALESCE(o.shipping_region,'Khu vực khác') region,count(*) total_cod_shipments,
            count_if(s.status='failed') failed_cod_shipments FROM lakehouse.gold.fact_shipment s
            JOIN lakehouse.gold.fact_order o ON s.order_id=o.order_id WHERE o.payment_method='cod'
            GROUP BY 1 ORDER BY failed_cod_shipments DESC""")
        return OperationsMetricsResponse(
            pending_fulfillment_count=_n(row, "pending"),
            shipping_sla_violations_count=_n(shipping, "problematic"),
            boom_orders_count=_n(shipping, "failed"),
            return_requests_count=_n(returns, "n"),
            regional_boom_rates=[
                RegionalBoomRateSignal(
                    **r, boom_rate_percent=_pct(_n(r, "failed_cod_shipments"), _n(r, "total_cod_shipments"))
                )
                for r in regions
            ],
            fulfillment_bottleneck=FulfillmentBottleneckSignal(
                paid_unfulfilled_orders=_n(row, "pending"),
                stale_unfulfilled_orders=_n(row, "stale"),
                average_waiting_hours=round(float(row.get("waiting") or 0), 2),
                bottleneck_warning=_n(row, "stale") > 0 or _n(row, "pending") >= 20,
            ),
        )

    return serve(("operations",), build, trino_client)


def _timestamp(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if value is None:
        return None
    text = str(value).replace(" UTC", "+00:00").replace("Z", "+00:00")
    return (
        datetime.fromisoformat(text).replace(tzinfo=UTC)
        if "+" not in text
        else datetime.fromisoformat(text).astimezone(UTC)
    )


def get_system_metrics(db=None, trino_client=None):
    def build(reader):
        row = reader.one(
            "SELECT count(DISTINCT order_id) orders,COALESCE(SUM(order_total_vnd),0) revenue FROM lakehouse.gold.fact_order"
        )
        comparisons = []
        if db is not None:
            count = db.scalar(select(func.count(Order.order_id))) or 0
            revenue = db.scalar(select(func.coalesce(func.sum(Order.total_vnd), 0))) or 0
            comparisons = [
                ReconciliationVariance(
                    metric_name=name,
                    oltp_value=float(a),
                    lakehouse_value=float(b),
                    variance_percent=round(abs(a - b) / a * 100, 2) if a else (100 if b else 0),
                )
                for name, a, b in [
                    ("total_orders", count, _n(row, "orders")),
                    ("order_total_vnd", revenue, _n(row, "revenue")),
                ]
            ]
        mode = reader.publication.get("source_mode", "cdc")
        states = reader.client.execute_query(
            "SELECT * FROM lakehouse.system.cdc_pipeline_state WHERE consumer_id='oltp-cdc'"
        ) if mode == "cdc" else []
        state = states[0] if states else {}
        now = datetime.now(UTC)
        checked = _timestamp(state.get("checked_at"))
        pending_since = _timestamp(state.get("pending_since"))
        age = (now - checked).total_seconds() if checked else float("inf")
        lag = max(0, int((now - pending_since).total_seconds())) if pending_since else 0
        variance = max((r.variance_percent for r in comparisons), default=0)
        status = (
            "unhealthy"
            if age > 180 or state.get("status") == "failed" or variance > 20
            else ("degraded" if lag > 120 or variance > 5 else "healthy")
        )
        published = _timestamp(reader.publication["published_at"])
        freshness = max(0, int((now - published).total_seconds() / 60))
        freshness_limit = 5 if mode == "cdc" else 1500
        if mode == "batch":
            status = "healthy" if freshness <= freshness_limit else "degraded"
        return SystemMetricsResponse(
            pipeline_status=status,
            data_freshness_minutes=freshness,
            data_freshness_sla_minutes=freshness_limit,
            reconciliation_variance=comparisons,
            cdc_apply_status=state.get("status", "unknown") if mode == "cdc" else "not_applicable",
            cdc_heartbeat_at=str(state.get("checked_at")) if checked else None,
            cdc_pending_events=_n(state, "pending_events"),
            cdc_apply_lag_seconds=lag,
        )

    return serve(("system",), build, trino_client)


def get_role_metrics_data(target_role, store_id=None, db=None, trino_client=None):
    role = ROLE_CANONICAL_MAP[target_role.strip().lower()]
    functions = {
        "executive": get_executive_metrics,
        "sales": get_sales_metrics,
        "marketing": get_marketing_metrics,
        "store": get_store_metrics,
        "inventory": get_inventory_metrics,
        "operations": get_operations_metrics,
        "system": get_system_metrics,
    }
    kwargs = {"trino_client": trino_client}
    if role == "store":
        kwargs["store_id"] = store_id
    if role == "system":
        kwargs["db"] = db
    return functions[role](**kwargs)


def get_sales_trend(days=30, db=None, trino_client=None):
    days = max(1, min(int(days), 365))

    def build(reader):
        rows = reader.query(f"""SELECT CAST(order_date AS varchar) date,
            SUM(net_revenue_vnd) revenue_vnd,SUM(cogs_vnd) cogs_vnd,
            SUM(gross_profit_vnd) profit_vnd,SUM(total_orders) orders_count
            FROM lakehouse.gold.mart_order_daily
            WHERE order_date BETWEEN {VN_TODAY}-INTERVAL '{days - 1}' DAY AND {VN_TODAY}
            GROUP BY order_date ORDER BY order_date""")
        return SalesTrendResponse(days=days, points=[DailySalesTrendPoint(**r) for r in rows])

    return serve(("sales-trend", days), build, trino_client)


def get_admin_overview():
    def build(reader):
        row = reader.one(f"""SELECT
            (SELECT count(*) FROM lakehouse.silver.silver_products WHERE is_active) products,
            (SELECT count(*) FROM lakehouse.silver.silver_product_variants v JOIN lakehouse.silver.silver_products p ON v.product_id=p.product_id WHERE v.is_active AND p.is_active) variants,
            (SELECT count(*) FROM lakehouse.gold.fact_inventory_current i JOIN lakehouse.gold.dim_variant v ON i.variant_key=v.variant_key JOIN lakehouse.gold.dim_product p ON i.product_key=p.product_key WHERE i.location_type='warehouse' AND i.is_low_stock AND v.is_active AND p.is_active) low,
            (SELECT count(*) FROM lakehouse.silver.silver_customers WHERE role='customer') customers,
            (SELECT count(*) FROM lakehouse.silver.silver_product_reviews) reviews,
            (SELECT count(*) FROM lakehouse.silver.silver_coupons WHERE is_active AND starts_at<={NOW_UTC} AND ends_at>{NOW_UTC}) coupons,
            (SELECT COALESCE(SUM(gross_revenue_vnd),0) FROM lakehouse.gold.fact_order) revenue,
            (SELECT COALESCE(SUM(refunded_amount_vnd),0) FROM lakehouse.gold.fact_order) refunds,
            (SELECT COALESCE(SUM(total_cost_vnd),0) FROM lakehouse.gold.fact_order) cogs""")
        counts = {
            r["status"]: _n(r, "n")
            for r in reader.query("SELECT status,count(*) n FROM lakehouse.gold.fact_order GROUP BY status")
        }
        revenue = _n(row, "revenue")
        refunds = _n(row, "refunds")
        net = revenue - refunds
        profit = net - _n(row, "cogs")
        return AdminOverviewResponse(
            active_products=_n(row, "products"),
            active_variants=_n(row, "variants"),
            low_stock_variants=_n(row, "low"),
            customers=_n(row, "customers"),
            total_reviews=_n(row, "reviews"),
            active_coupons=_n(row, "coupons"),
            gross_revenue_vnd=revenue,
            refunded_amount_vnd=refunds,
            net_revenue_vnd=net,
            cogs_vnd=_n(row, "cogs"),
            gross_profit_vnd=profit,
            gross_margin_percent=_pct(profit, net),
            paid_orders=counts.get("paid", 0),
            confirmed_orders=counts.get("confirmed", 0),
            completed_orders=counts.get("completed", 0),
            cancelled_orders=counts.get("cancelled", 0),
            boom_orders_count=counts.get("failed_delivery", 0),
            return_orders_count=counts.get("returned", 0),
            shipping_orders=counts.get("shipping", 0),
            delivered_orders=counts.get("delivered", 0),
        )

    return serve(("admin-overview",), build)
