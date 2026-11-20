-- Product sales on the revenue day; returned quantity (accepted at inspection) on the day the return resolved.
WITH events AS (
  SELECT revenue_date, channel, store_key, product_key, quantity, revenue_vnd, cost_vnd, 0 AS returned_quantity
  FROM {{ ref('fact_order_item') }}
  WHERE revenue_date IS NOT NULL
  UNION ALL
  SELECT r.resolved_date, o.channel, o.store_key, r.product_key, 0, 0, 0, r.accepted_quantity
  FROM {{ ref('fact_return') }} r
  JOIN {{ ref('fact_order') }} o ON o.order_id = r.order_id
  WHERE r.status = 'completed' AND r.resolved_date IS NOT NULL
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
  sum(e.revenue_vnd - e.cost_vnd) AS gross_profit_vnd,
  sum(e.returned_quantity) AS returned_quantity
FROM events e
LEFT JOIN {{ ref('dim_store') }} s ON s.store_key = e.store_key
LEFT JOIN {{ ref('dim_product') }} p ON p.product_key = e.product_key
GROUP BY 1, 2, 3, 4, 5, 6, 7
