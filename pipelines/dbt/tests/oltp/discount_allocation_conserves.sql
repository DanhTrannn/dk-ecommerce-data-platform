-- Allocated line discounts add up to the order discount (capped at the order's line total).
SELECT o.order_id
FROM {{ ref('fact_order') }} o
JOIN (
  SELECT order_id, sum(allocated_discount_vnd) AS allocated, sum(line_total_vnd) AS lines
  FROM {{ ref('fact_order_item') }}
  GROUP BY order_id
) i ON i.order_id = o.order_id
WHERE i.allocated <> least(o.discount_vnd, i.lines)
