{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('revenue_date', sales_days()) }}", post_hook="{{ advance_mart(['fact_order', 'fact_order_item', 'fact_return', 'dim_store', 'dim_city', 'dim_product']) }}") }}
-- Recognized orders count on their revenue day; refunds of recognized orders reduce revenue on the day they
-- succeed (design §5). Refunds of orders that were never recognized (paid, then cancelled) are not revenue.
-- Goods a refunded return puts back on the shelf (accepted at inspection) take their sold cost back out of
-- cost of goods on the refund day, so a fully returned order nets to zero profit.
WITH
{% if is_incremental() %}
days AS (
  {{ sales_days() }}
),
{% endif %}
items AS (
  SELECT
    order_id,
    sum(line_total_vnd) AS gross_revenue_vnd,
    sum(allocated_discount_vnd) AS discount_vnd,
    sum(cost_vnd) AS cogs_vnd
  FROM {{ ref('fact_order_item') }}
  WHERE revenue_date IS NOT NULL AND {{ filter_in('revenue_date', 'SELECT * FROM days') }}
  GROUP BY order_id
),
events AS (
  SELECT
    o.revenue_date, o.channel, o.store_key, o.city_key,
    1 AS recognized_orders, o.item_quantity AS items_sold,
    i.gross_revenue_vnd, i.discount_vnd, i.cogs_vnd, 0 AS refund_vnd, 0 AS returned_cogs_vnd
  FROM {{ ref('fact_order') }} o
  JOIN items i ON i.order_id = o.order_id
  WHERE {{ filter_in('o.revenue_date', 'SELECT * FROM days') }}
  UNION ALL
  SELECT
    {{ vn_date('r.completed_at') }}, o.channel, o.store_key, o.city_key,
    0, 0, 0, 0, 0, r.amount_vnd, 0
  FROM {{ ref('refunds') }} r
  JOIN {{ ref('payments') }} p ON p.payment_id = r.payment_id
  JOIN {{ ref('fact_order') }} o ON o.order_id = p.order_id
  WHERE r.status = 'succeeded' AND r.completed_at IS NOT NULL AND o.revenue_date IS NOT NULL
    AND {{ filter_in(vn_date('r.completed_at'), 'SELECT * FROM days') }}
  UNION ALL
  SELECT
    {{ vn_date('r.refunded_at') }}, o.channel, o.store_key, o.city_key,
    0, 0, 0, 0, 0, 0, r.accepted_quantity * (i.cost_vnd / i.quantity)
  FROM {{ ref('fact_return') }} r
  JOIN {{ ref('fact_order_item') }} i ON i.order_item_id = r.order_item_id
  JOIN {{ ref('fact_order') }} o ON o.order_id = r.order_id
  WHERE r.refunded_at IS NOT NULL AND r.accepted_quantity > 0 AND o.revenue_date IS NOT NULL
    AND {{ filter_in(vn_date('r.refunded_at'), 'SELECT * FROM days') }}
)
SELECT
  e.revenue_date,
  e.channel,
  e.store_key,
  s.store_name,
  e.city_key,
  c.city_name,
  sum(e.recognized_orders) AS recognized_orders,
  sum(e.items_sold) AS items_sold,
  sum(e.gross_revenue_vnd) AS gross_revenue_vnd,
  sum(e.discount_vnd) AS discount_vnd,
  sum(e.cogs_vnd) AS cogs_vnd,
  sum(e.returned_cogs_vnd) AS returned_cogs_vnd,
  sum(e.refund_vnd) AS refund_vnd,
  sum(e.gross_revenue_vnd - e.discount_vnd - e.refund_vnd) AS net_revenue_vnd,
  sum(e.gross_revenue_vnd - e.discount_vnd - e.refund_vnd - e.cogs_vnd + e.returned_cogs_vnd) AS gross_profit_vnd
FROM events e
LEFT JOIN {{ ref('dim_store') }} s ON s.store_key = e.store_key
LEFT JOIN {{ ref('dim_city') }} c ON c.city_key = e.city_key
GROUP BY 1, 2, 3, 4, 5, 6
