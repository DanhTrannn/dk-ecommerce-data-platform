"""Additive sales, logistics and return marts; collapse one-to-many inputs first."""

import polars as pl

from .core import query, round_money_rate

SQL = {
    "mart_order_daily": """WITH r AS (SELECT order_id,count(DISTINCT return_id) requests FROM return_requests
        WHERE status<>'cancelled' GROUP BY order_id),s AS (SELECT order_id,count(shipment_id) active
        FROM fact_shipment WHERE status IN ('assigned','picked_up','in_transit') GROUP BY order_id), base AS (
        SELECT o.*,coalesce(r.requests,0) requests,CASE WHEN o.channel='online' OR o.store_key=0 THEN 0 ELSE o.store_key END served_store,
        CASE WHEN o.payment_method='cod' THEN coalesce(s.active,0) ELSE 0 END cod_shipments FROM fact_order o
        LEFT JOIN r ON o.order_id=r.order_id LEFT JOIN s ON o.order_id=s.order_id), labels AS (
        SELECT b.*,CASE WHEN b.served_store=0 THEN 'Kênh Online Toàn Quốc' ELSE coalesce(d.store_name,'Chưa xác định') END store_name
        FROM base b LEFT JOIN dim_store d ON b.served_store=d.store_key)
        SELECT order_date,channel,served_store store_key,store_name,count(order_id) total_orders,
        sum(CAST(is_revenue_recognized AS BIGINT)) recognized_orders,sum(CAST(net_revenue_vnd>0 AS BIGINT)) revenue_orders,
        sum(CAST(is_boom AS BIGINT)) boom_orders,sum(requests) return_requests_count,sum(order_total_vnd) order_total_vnd,
        sum(gross_revenue_vnd) gross_revenue_vnd,sum(refunded_amount_vnd) refunded_amount_vnd,sum(net_revenue_vnd) net_revenue_vnd,
        sum(total_cost_vnd) cogs_vnd,sum(gross_profit_vnd) gross_profit_vnd,sum(order_total_vnd*cod_shipments) cod_in_transit_vnd,
        sum(cod_shipments) cod_shipments_count FROM labels GROUP BY order_date,channel,served_store,store_name""",
    "mart_product_sales_daily": """SELECT i.order_date,p.product_id,p.product_name,sum(i.recognized_quantity) units_sold,
        sum(i.gross_revenue_vnd) gross_revenue_vnd,sum(i.refunded_amount_vnd) refunded_amount_vnd,
        sum(i.net_revenue_vnd) net_revenue_vnd,sum(i.item_cost_vnd) cogs_vnd,sum(i.item_profit_vnd) gross_profit_vnd
        FROM fact_order_item i JOIN dim_product p ON i.product_key=p.product_key WHERE i.order_status IN ('delivered','completed','returned')
        GROUP BY i.order_date,p.product_id,p.product_name""",
    "mart_sales_daily": """WITH categories AS (SELECT DISTINCT category_id,category_code FROM dim_product), base AS (
        SELECT i.*,o.is_revenue_recognized,o.is_boom,coalesce(c.category_id,0) category_id,
        coalesce(i.category_name_snapshot,'Unknown') category_name FROM fact_order_item i
        JOIN fact_order o ON i.order_id=o.order_id LEFT JOIN categories c ON i.category_code_snapshot=c.category_code)
        SELECT order_date,channel,store_key,category_id,category_name,count(DISTINCT order_id) total_orders,
        count(DISTINCT CASE WHEN is_revenue_recognized THEN order_id END) successful_orders,
        count(DISTINCT CASE WHEN is_boom THEN order_id END) boom_orders,sum(recognized_quantity) items_sold,
        sum(gross_revenue_vnd) gross_revenue_vnd,sum(refunded_amount_vnd) refunded_amount_vnd,
        sum(net_revenue_vnd) net_revenue_vnd,sum(item_cost_vnd) cogs_vnd,sum(item_profit_vnd) gross_profit_vnd
        FROM base GROUP BY order_date,channel,store_key,category_id,category_name""",
    "fact_return_exchange": """SELECT i.return_item_id,r.return_id,r.return_code return_number,r.order_id,i.order_item_id,
        CAST(r.created_at AS DATE) return_date,r.action_type,r.status return_status,i.inspection_status,r.customer_reason reason,
        i.variant_id returned_variant_key,i.exchange_variant_id exchanged_variant_key,i.quantity returned_quantity,
        coalesce(i.refund_amount_vnd,0) refund_amount_vnd FROM return_items i JOIN return_requests r ON i.return_id=r.return_id""",
    "mart_product_returns": """SELECT f.return_date,f.returned_variant_key,coalesce(v.product_id,0) product_id,
        coalesce(v.product_name,'Unknown') product_name,coalesce(v.sku,'Unknown') sku,coalesce(v.size,'Unknown') size,
        coalesce(v.color,'Unknown') color,f.action_type,coalesce(f.reason,'Unknown') reason,
        count(DISTINCT f.return_id) total_return_requests,count(f.return_item_id) return_item_count,
        sum(f.returned_quantity) total_returned_quantity,
        sum(CASE WHEN f.return_status='completed' AND f.inspection_status='passed' THEN f.returned_quantity ELSE 0 END) accepted_return_quantity,
        sum(f.refund_amount_vnd) requested_refund_amount_vnd,sum(f.refund_amount_vnd) total_refund_amount_vnd
        FROM fact_return_exchange f LEFT JOIN dim_variant v ON f.returned_variant_key=v.variant_key
        WHERE f.return_status<>'cancelled' GROUP BY f.return_date,f.returned_variant_key,product_id,product_name,sku,size,color,f.action_type,reason""",
    "fact_shipment": """SELECT shipment_id,shipment_code,order_id,delivery_staff_id staff_key,CAST(created_at AS DATE) shipment_date,
        status,dispatched_at,delivered_at,failed_at,attempt_count,status='failed' is_boom,status='delivered' is_delivered,
        cod_amount_vnd cod_amount_expected_vnd,coalesce(cod_collected_vnd,0) cod_amount_collected_vnd
        FROM shipments""",
    "mart_logistics_performance": """WITH labeled AS (SELECT s.*,coalesce(d.staff_code,'Unknown') staff_code,
        coalesce(d.assigned_city_id,0) assigned_city_id,coalesce(d.assigned_city_name,'Unknown') assigned_city_name
        FROM fact_shipment s LEFT JOIN dim_delivery_staff d ON s.staff_key=d.staff_key)
        SELECT shipment_date,staff_key,staff_code,assigned_city_id,assigned_city_name,count(*) total_shipments,
        sum(CASE WHEN status='delivered' THEN 1 ELSE 0 END) delivered_count,sum(CASE WHEN is_boom THEN 1 ELSE 0 END) boom_count,
        sum(CASE WHEN status='delivered' AND is_on_time IS NOT NULL THEN 1 ELSE 0 END) on_time_eligible_count,
        sum(CASE WHEN status='delivered' AND is_on_time THEN 1 ELSE 0 END) on_time_count,
        sum(CASE WHEN attempt_count>1 OR status='failed' THEN 1 ELSE 0 END) problematic_shipments,
        count(delivery_duration_minutes) duration_sample_count,sum(delivery_duration_minutes) duration_minutes_sum,
        avg(delivery_duration_minutes) avg_duration_minutes,sum(coalesce(cod_amount_collected_vnd,0)) total_cod_collected_vnd
        FROM labeled GROUP BY shipment_date,staff_key,staff_code,assigned_city_id,assigned_city_name""",
}


def build_mart(name, frames):
    frame = query(frames, SQL[name])
    if name == "mart_sales_daily":
        frame = frame.with_columns(
            pl.when(pl.col("net_revenue_vnd") > 0)
            .then(pl.col("gross_profit_vnd") / pl.col("net_revenue_vnd") * 100)
            .otherwise(0.0)
            .alias("gross_profit_margin_pct")
        )
        frame = round_money_rate(frame, "gross_profit_margin_pct")
    if name == "fact_return_exchange":
        frame = frame.with_columns(pl.col("return_date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("return_date_key"))
    if name == "fact_shipment":
        end = pl.coalesce("delivered_at", "failed_at")
        duration = (end.dt.epoch("s") - pl.col("dispatched_at").dt.epoch("s")) / 60.0
        frame = frame.with_columns(
            pl.col("shipment_date").dt.strftime("%Y%m%d").cast(pl.Int32).alias("shipment_date_key"),
            pl.when(duration >= 0).then(duration).alias("delivery_duration_minutes"),
            pl.lit(None, dtype=pl.Boolean).alias("is_on_time"),
        )
        frame = round_money_rate(frame, "delivery_duration_minutes")
    if name == "mart_logistics_performance":
        frame = frame.with_columns(
            pl.when(pl.col("total_shipments") > 0)
            .then(pl.col("boom_count") / pl.col("total_shipments") * 100)
            .otherwise(0.0)
            .alias("boom_rate_pct"),
            pl.when(pl.col("on_time_eligible_count") > 0)
            .then(pl.col("on_time_count") / pl.col("on_time_eligible_count") * 100)
            .alias("on_time_rate_pct"),
        )
        frame = round_money_rate(frame, "boom_rate_pct", "on_time_rate_pct", "avg_duration_minutes")
    return frame
