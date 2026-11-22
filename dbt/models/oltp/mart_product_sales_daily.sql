{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('revenue_date', sales_days()) }}", post_hook="{{ advance_mart(['fact_order', 'fact_order_item', 'fact_return', 'dim_store', 'dim_city', 'dim_product']) }}") }}
-- Product sales on the revenue day; returned quantity (accepted at inspection) on the day the return resolved.
-- A refunded return reduces the product's revenue and, for goods back on the shelf, its cost of goods on the
-- refund day (same rule as mart_sales_daily).
WITH
{% if is_incremental() %}
days AS (
  {{ sales_days() }}
),
{% endif %}
events AS (
  SELECT
    revenue_date, channel, store_key, product_key, quantity, revenue_vnd, cost_vnd,
    0 AS returned_quantity, 0 AS refund_vnd, 0 AS returned_cogs_vnd
  FROM {{ ref('fact_order_item') }}
  WHERE revenue_date IS NOT NULL AND {{ filter_in('revenue_date', 'SELECT * FROM days') }}
  UNION ALL
  SELECT r.resolved_date, o.channel, o.store_key, r.product_key, 0, 0, 0, r.accepted_quantity, 0, 0
  FROM {{ ref('fact_return') }} r
  JOIN {{ ref('fact_order') }} o ON o.order_id = r.order_id
  WHERE r.status = 'completed' AND r.resolved_date IS NOT NULL AND {{ filter_in('r.resolved_date', 'SELECT * FROM days') }}
  UNION ALL
  SELECT
    {{ vn_date('r.refunded_at') }}, o.channel, o.store_key, r.product_key, 0, 0, 0, 0,
    r.refund_paid_vnd, r.accepted_quantity * (i.cost_vnd / i.quantity)
  FROM {{ ref('fact_return') }} r
  JOIN {{ ref('fact_order_item') }} i ON i.order_item_id = r.order_item_id
  JOIN {{ ref('fact_order') }} o ON o.order_id = r.order_id
  WHERE r.refunded_at IS NOT NULL AND o.revenue_date IS NOT NULL AND {{ filter_in(vn_date('r.refunded_at'), 'SELECT * FROM days') }}
)
SELECT
  e.revenue_date,
  e.channel,
  e.store_key,
  s.store_name,
  e.product_key,
  p.product_name,
  p.category_name,
  sum(e.quantity) AS quantity,
  sum(e.revenue_vnd) AS revenue_vnd,
  sum(e.cost_vnd) AS cogs_vnd,
  sum(e.refund_vnd) AS refund_vnd,
  sum(e.returned_cogs_vnd) AS returned_cogs_vnd,
  sum(e.revenue_vnd - e.refund_vnd - e.cost_vnd + e.returned_cogs_vnd) AS gross_profit_vnd,
  sum(e.returned_quantity) AS returned_quantity
FROM events e
LEFT JOIN {{ ref('dim_store') }} s ON s.store_key = e.store_key
LEFT JOIN {{ ref('dim_product') }} p ON p.product_key = e.product_key
GROUP BY 1, 2, 3, 4, 5, 6, 7
