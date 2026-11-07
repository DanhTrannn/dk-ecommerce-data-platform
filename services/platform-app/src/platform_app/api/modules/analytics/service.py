"""Analytical serving through Trino; MySQL is used only for source reconciliation.

Business queries use completed publications. Marketing reads independent log and
OLTP mart sets; System reads order marts and observes MySQL only for reconciliation.
"""


from sqlalchemy.orm import Session

from platform_app.api.core.errors import FORBIDDEN, VALIDATION_ERROR, AppError
from platform_app.api.models.customer import Customer
from platform_app.api.modules.analytics.reader import serve
from platform_app.api.modules.analytics.admin_service import get_admin_overview as get_admin_overview
from platform_app.api.modules.analytics.system_service import get_system_metrics
from platform_app.api.modules.analytics.operations_service import get_operations_metrics
from platform_app.api.modules.analytics.marketing_service import get_marketing_metrics
from platform_app.api.modules.analytics.inventory_service import (
    get_active_stores_list as get_active_stores_list,
    get_store_overview as get_store_overview,
    get_store_metrics, get_inventory_metrics,
)
from platform_app.api.modules.analytics.schemas import (
    CashInTransitSignal,
    CategoryShareMetric,
    DailySalesTrendPoint,
    ExecutiveMetricsResponse,
    SalesMetricsResponse,
    SalesTrendResponse,
    StoreContribution,
    TopProductMetric,
)
from platform_app.api.modules.analytics.trino_client import TrinoClient

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
