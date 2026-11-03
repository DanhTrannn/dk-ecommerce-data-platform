"""Store and inventory dashboards served only from published marts."""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from pydantic import BaseModel

from app.core.errors import FORBIDDEN, INTERNAL_ERROR, VALIDATION_ERROR, AppError
from app.modules.analytics.reader import serve
from app.modules.analytics.schemas import (
    InventoryHistoryPoint,
    InventoryMetricsResponse,
    InventoryStockAlert,
    StoreItemResponse,
    StoreMetricsResponse,
    StoreStockoutSignal,
)
from app.modules.store.schemas import StoreDashboardResponse

VN_TODAY = "CAST(current_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh' AS date)"


def _timestamp(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value).replace(" UTC", "+00:00").replace("Z", "+00:00"))
    return (parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed).astimezone(UTC)


def _iso(value):
    parsed = _timestamp(value)
    return parsed.isoformat() if parsed else None


def _store(reader, store_id):
    where = f"store_id={int(store_id)}" if store_id is not None else "is_active AND store_id>0"
    store = reader.one(f"SELECT * FROM lakehouse.gold.mart_store_overview WHERE {where} ORDER BY store_id LIMIT 1")
    if not store:
        raise AppError(VALIDATION_ERROR, "Cửa hàng chưa có trong dữ liệu OLAP đã công bố.", status_code=404)
    return store


def _store_values(reader, store):
    store_id = int(store["store_id"])
    row = reader.one(f"""SELECT count(*) day_rows,SUM(net_revenue_vnd) revenue,SUM(total_orders) orders
        FROM lakehouse.gold.mart_order_daily WHERE store_key={store_id} AND order_date={VN_TODAY}""")
    zone = ZoneInfo("Asia/Ho_Chi_Minh")
    published = _timestamp(reader.publication["published_at"])
    # A publication from an older day cannot certify zero activity today.
    empty_value = 0 if published.astimezone(zone).date() == datetime.now(zone).date() else None
    stocks = reader.query(f"""SELECT variant_id,sku,product_name,on_hand
        FROM lakehouse.gold.mart_inventory_current
        WHERE location_type='store' AND location_id={store_id} AND is_out_of_stock ORDER BY variant_id LIMIT 10""")
    return StoreMetricsResponse(
        store_id=store_id,
        store_name=store["store_name"],
        store_revenue_today_vnd=row["revenue"] if row["day_rows"] else empty_value,
        store_orders_count=row["orders"] if row["day_rows"] else empty_value,
        low_stock_at_store_count=store["low_stock_count"],
        stockout_at_store_count=store["stockout_count"],
        inventory_observed_at=_iso(store["inventory_observed_at"]),
        store_stockouts=[StoreStockoutSignal(**r) for r in stocks],
    )


def get_store_metrics(store_id=None, db=None, trino_client=None):
    return serve(("store", store_id), lambda reader: _store_values(reader, _store(reader, store_id)), trino_client)


class _StoreList(BaseModel):
    items: list[StoreItemResponse]


def get_active_stores_list(actor, db=None):
    def build(reader):
        if actor.role == "store_manager" and actor.store_id is None:
            raise AppError(FORBIDDEN, "Tài khoản chưa được gán cửa hàng.", status_code=403)
        where = f"AND store_id={int(actor.store_id)}" if actor.role == "store_manager" else ""
        rows = reader.query(f"""SELECT store_id,store_name name,store_code code
            FROM lakehouse.gold.mart_store_overview WHERE is_active {where} ORDER BY store_id""")
        return _StoreList(items=[StoreItemResponse(**r) for r in rows])

    return serve(("stores", actor.role, actor.store_id), build).items


def get_inventory_metrics(db=None, trino_client=None):
    def build(reader):
        summary = reader.one("SELECT * FROM lakehouse.gold.mart_inventory_summary WHERE summary_key='current'")
        if not summary:
            raise AppError(INTERNAL_ERROR, "Mart tổng quan tồn kho chưa được xây dựng.", status_code=503)

        def alerts(condition):
            return [
                InventoryStockAlert(**r)
                for r in reader.query(f"""SELECT variant_id,sku,product_name,on_hand
                FROM lakehouse.gold.mart_inventory_current WHERE location_type='warehouse'
                AND {condition} ORDER BY variant_id LIMIT 10""")
            ]

        history = reader.query(f"""SELECT CAST(snapshot_date AS varchar) date,
            SUM(CASE WHEN location_type='warehouse' THEN total_on_hand_units END) warehouse_stock_units,
            SUM(CASE WHEN location_type='store' THEN total_on_hand_units END) store_stock_units,
            SUM(total_inventory_value_vnd) total_inventory_value_vnd
            FROM lakehouse.gold.mart_inventory_health
            WHERE snapshot_date BETWEEN {VN_TODAY}-INTERVAL '29' DAY AND {VN_TODAY}
            GROUP BY snapshot_date ORDER BY snapshot_date""")
        return InventoryMetricsResponse(
            **{
                k: summary[k]
                for k in (
                    "total_inventory_value_vnd",
                    "warehouse_stock_units",
                    "store_stock_units",
                    "inbound_batches_count",
                    "stockout_count",
                )
            },
            inventory_observed_at=_iso(summary["inventory_observed_at"]),
            critical_stockout_alerts=alerts("is_out_of_stock"),
            low_stock_alerts=alerts("is_low_stock AND NOT is_out_of_stock"),
            inventory_history=[InventoryHistoryPoint(**r) for r in history],
        )

    return serve(("inventory",), build, trino_client)


def get_store_overview(store_id, trino_client=None):
    def build(reader):
        store = _store(reader, store_id)
        metrics = _store_values(reader, store)
        return StoreDashboardResponse(
            store_name=store["store_name"],
            today_orders=metrics.store_orders_count,
            today_revenue_vnd=metrics.store_revenue_today_vnd,
            total_orders=store["total_orders"],
            completed_orders=store["completed_orders"],
            total_revenue_vnd=store["total_revenue_vnd"],
            low_stock_items=store["low_stock_count"],
            active_staff=store["active_staff"],
            inventory_observed_at=metrics.inventory_observed_at,
        )

    return serve(("store-overview", store_id), build, trino_client)
