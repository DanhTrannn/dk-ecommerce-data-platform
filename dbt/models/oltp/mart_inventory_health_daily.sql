{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('inventory_date', inventory_days()) }}", post_hook="{{ advance_mart(['fact_inventory_daily', 'dim_product', 'dim_store'], inventory_last_day()) }}") }}
-- Inbound value comes from receipt lines; receipts always stock the central warehouse.
WITH stock AS (
  SELECT
    f.inventory_date,
    f.location_type,
    f.store_key,
    p.category_key,
    p.category_name,
    count_if(f.closing_qty > 0) AS variants_in_stock,
    count_if(f.closing_qty <= 0) AS variants_out_of_stock,
    sum(f.closing_qty) AS stock_qty,
    sum(f.closing_value_vnd) AS stock_value_vnd,
    sum(f.inbound_qty) AS inbound_qty,
    sum(f.sold_online_qty + f.sold_pos_qty) AS sold_qty
  FROM {{ ref('fact_inventory_daily') }} f
  LEFT JOIN {{ ref('dim_product') }} p ON p.product_key = f.product_key
  WHERE {{ filter_in('f.inventory_date', inventory_days()) }}
  GROUP BY 1, 2, 3, 4, 5
),
inbound AS (
  SELECT {{ vn_date('i.created_at') }} AS inventory_date, p.category_key, sum(i.total_cost_vnd) AS inbound_value_vnd
  FROM {{ ref('inbound_receipt_items') }} i
  LEFT JOIN {{ ref('dim_variant') }} v ON v.variant_key = i.variant_id
  LEFT JOIN {{ ref('dim_product') }} p ON p.product_key = v.product_key
  WHERE i.created_at <= {{ as_of() }} AND {{ filter_in(vn_date('i.created_at'), inventory_days()) }}
  GROUP BY 1, 2
)
SELECT
  s.inventory_date,
  s.location_type,
  s.store_key,
  st.store_name,
  s.category_key,
  s.category_name,
  s.variants_in_stock,
  s.variants_out_of_stock,
  s.stock_qty,
  s.stock_value_vnd,
  s.inbound_qty,
  coalesce(i.inbound_value_vnd, 0) AS inbound_value_vnd,
  s.sold_qty
FROM stock s
LEFT JOIN inbound i
  ON s.location_type = 'central_warehouse' AND i.inventory_date = s.inventory_date AND i.category_key IS NOT DISTINCT FROM s.category_key
LEFT JOIN {{ ref('dim_store') }} st ON st.store_key = s.store_key
