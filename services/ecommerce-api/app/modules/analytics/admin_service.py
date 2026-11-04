"""Admin counts and finance served from one published mart set."""

from datetime import UTC, datetime

from app.core.errors import INTERNAL_ERROR, AppError
from app.modules.admin.schemas import AdminOverviewResponse
from app.modules.analytics.reader import serve


def get_admin_overview(trino_client=None):
    def build(reader):
        summary = reader.one("SELECT * FROM lakehouse.gold.mart_admin_overview WHERE summary_key='current'")
        if not summary:
            raise AppError(INTERNAL_ERROR, "Mart tổng quan quản trị chưa có quan sát.", status_code=503)
        money = reader.one("""SELECT SUM(total_orders) total_orders,SUM(gross_revenue_vnd) gross_revenue_vnd,
            SUM(refunded_amount_vnd) refunded_amount_vnd,SUM(net_revenue_vnd) net_revenue_vnd,
            SUM(cogs_vnd) cogs_vnd,SUM(gross_profit_vnd) gross_profit_vnd
            FROM lakehouse.gold.mart_order_daily""")
        if (money.get("total_orders") or 0) != summary["order_rows"]:
            raise AppError(INTERNAL_ERROR, "Các mart đơn hàng chưa nhất quán.", status_code=503)
        observed = datetime.fromisoformat(str(summary["admin_observed_at"]).replace(" UTC", "+00:00"))
        observed = observed.replace(tzinfo=UTC) if observed.tzinfo is None else observed.astimezone(UTC)
        revenue, profit = money.get("net_revenue_vnd"), money.get("gross_profit_vnd")
        return AdminOverviewResponse(
            **{
                name: summary[name]
                for name in [
                    "active_products",
                    "active_variants",
                    "low_stock_variants",
                    "customers",
                    "total_reviews",
                    "active_coupons",
                    "pending_payment_orders",
                    "paid_orders",
                    "confirmed_orders",
                    "shipping_orders",
                    "delivered_orders",
                    "completed_orders",
                    "cancelled_orders",
                ]
            },
            **{
                name: money.get(name)
                for name in [
                    "total_orders",
                    "gross_revenue_vnd",
                    "refunded_amount_vnd",
                    "net_revenue_vnd",
                    "cogs_vnd",
                    "gross_profit_vnd",
                ]
            },
            boom_orders_count=summary["failed_delivery_orders"],
            return_orders_count=summary["returned_orders"],
            gross_margin_percent=profit * 100.0 / revenue
            if profit is not None and revenue is not None and revenue > 0
            else None,
            admin_observed_at=observed.isoformat(),
        )

    return serve(("admin-overview",), build, trino_client)
