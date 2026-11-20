-- Recognized orders count on their revenue day; refunds of recognized orders reduce revenue on the day they
-- succeed (design §5). Refunds of orders that were never recognized (paid, then cancelled) are not revenue.
WITH items AS (
  SELECT
    order_id,
    sum(line_total_vnd) AS gross_revenue_vnd,
    sum(allocated_discount_vnd) AS discount_vnd,
    sum(cost_vnd) AS cogs_vnd
  FROM {{ ref('fact_order_item') }}
  WHERE revenue_date IS NOT NULL
  GROUP BY order_id
),
events AS (
  SELECT
    o.revenue_date, o.channel, o.store_key, o.city_key,
    1 AS recognized_orders, o.item_quantity AS items_sold,
    i.gross_revenue_vnd, i.discount_vnd, i.cogs_vnd, 0 AS refund_vnd
  FROM {{ ref('fact_order') }} o
  JOIN items i ON i.order_id = o.order_id
  UNION ALL
  SELECT
    {{ vn_date('r.completed_at') }}, o.channel, o.store_key, o.city_key,
    0, 0, 0, 0, 0, r.amount_vnd
  FROM {{ source('silver', 'refunds') }} r
  JOIN {{ source('silver', 'payments') }} p ON p.payment_id = r.payment_id
  JOIN {{ ref('fact_order') }} o ON o.order_id = p.order_id
  WHERE r.status = 'succeeded' AND r.completed_at IS NOT NULL AND o.revenue_date IS NOT NULL
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
  sum(e.gross_revenue_vnd - e.discount_vnd - e.cogs_vnd) AS gross_profit_vnd,
  sum(e.refund_vnd) AS refund_vnd,
  sum(e.gross_revenue_vnd - e.discount_vnd - e.refund_vnd) AS net_revenue_vnd
FROM events e
LEFT JOIN {{ ref('dim_store') }} s ON s.store_key = e.store_key
LEFT JOIN {{ ref('dim_city') }} c ON c.city_key = e.city_key
GROUP BY 1, 2, 3, 4, 5, 6
