"""Inventory observations and serving grains, retaining unknown stock coverage."""

import polars as pl

from .core import query

SQL = {
    "fact_inventory_daily_snapshot": """WITH balances AS (
        SELECT 'warehouse' location_type,CAST(0 AS BIGINT) location_id,variant_id,on_hand on_hand_quantity,0 reserved_quantity FROM inventory
        UNION ALL SELECT 'store',store_id,variant_id,on_hand,0 FROM store_inventory)
        SELECT b.*,b.variant_id variant_key,v.product_id product_key,coalesce(v.cost_price_vnd,0) unit_cost_vnd,
        b.on_hand_quantity*coalesce(v.cost_price_vnd,0) inventory_value_cost_vnd,
        b.on_hand_quantity<=5 is_low_stock,b.on_hand_quantity=0 is_out_of_stock
        FROM balances b LEFT JOIN product_variants v ON b.variant_id=v.variant_id""",
    "mart_inventory_current": """SELECT i.location_type,i.location_id,i.variant_key variant_id,i.product_key,
        v.sku,v.product_name,p.category_id,p.category_name,i.snapshot_date,i.on_hand_quantity on_hand,
        i.reserved_quantity,i.unit_cost_vnd,i.inventory_value_cost_vnd,i.is_low_stock,i.is_out_of_stock,c.observed_at
        FROM fact_inventory_current i LEFT JOIN dim_variant v ON i.variant_key=v.variant_key
        LEFT JOIN dim_product p ON i.product_key=p.product_key CROSS JOIN clock c""",
    "mart_inventory_health": """SELECT i.snapshot_date,i.location_type,i.location_id,coalesce(p.category_id,0) category_id,
        coalesce(p.category_name,'Unknown') category_name,count(DISTINCT i.variant_key) total_variants,
        sum(CASE WHEN i.is_out_of_stock THEN 1 ELSE 0 END) out_of_stock_count,
        sum(CASE WHEN i.is_low_stock THEN 1 ELSE 0 END) low_stock_count,sum(i.on_hand_quantity) total_on_hand_units,
        sum(i.inventory_value_cost_vnd) total_inventory_value_vnd FROM fact_inventory_daily_snapshot i
        LEFT JOIN dim_product p ON i.product_key=p.product_key
        GROUP BY i.snapshot_date,i.location_type,i.location_id,category_id,category_name""",
    "mart_inventory_summary": """WITH i AS (SELECT count(variant_id) inventory_rows,
        sum(inventory_value_cost_vnd) total_inventory_value_vnd,
        sum(CASE WHEN location_type='warehouse' THEN on_hand END) warehouse_stock_units,
        sum(CASE WHEN location_type='store' THEN on_hand END) store_stock_units,
        CASE WHEN count(*)>0 THEN count(DISTINCT CASE WHEN is_out_of_stock THEN variant_id END) END stockout_count,
        max(observed_at) inventory_observed_at FROM mart_inventory_current)
        SELECT 'current' summary_key,i.*,r.inbound_batches_count FROM i CROSS JOIN
        (SELECT count(receipt_id) inbound_batches_count FROM inbound_receipts) r""",
    "mart_store_overview": """WITH sales AS (
        SELECT store_key,count(order_id) total_orders,sum(CAST(status='completed' AS BIGINT)) completed_orders,
        sum(net_revenue_vnd) total_revenue_vnd FROM fact_order GROUP BY store_key),
        staff AS (SELECT store_id store_key,count(customer_id) active_staff FROM customers
        WHERE role='store_manager' AND status='active' GROUP BY store_id),
        stock AS (SELECT location_id store_key,sum(CAST(is_low_stock AS BIGINT)) low_stock_count,
        sum(CAST(is_out_of_stock AS BIGINT)) stockout_count,max(observed_at) inventory_observed_at
        FROM mart_inventory_current WHERE location_type='store' GROUP BY location_id)
        SELECT d.store_key,d.store_id,d.store_code,d.store_name,d.is_active,coalesce(s.total_orders,0) total_orders,
        coalesce(s.completed_orders,0) completed_orders,coalesce(s.total_revenue_vnd,0) total_revenue_vnd,
        coalesce(t.active_staff,0) active_staff,i.low_stock_count,i.stockout_count,i.inventory_observed_at
        FROM dim_store d LEFT JOIN sales s ON d.store_key=s.store_key LEFT JOIN staff t ON d.store_key=t.store_key
        LEFT JOIN stock i ON d.store_key=i.store_key""",
}


def build_inventory(name, frames, observed_date):
    if name == "fact_inventory_current":
        return frames["fact_inventory_daily_snapshot"].filter(pl.col("snapshot_date") == observed_date)
    frame = query(frames, SQL[name])
    if name == "fact_inventory_daily_snapshot":
        frame = frame.with_columns(
            pl.lit(observed_date).alias("snapshot_date"),
            pl.lit(int(observed_date.strftime("%Y%m%d"))).alias("snapshot_date_key"),
        )
    return frame
