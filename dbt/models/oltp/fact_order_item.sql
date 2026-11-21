{{ config(properties={"partitioning": "ARRAY['month(order_date)']"}) }}
-- The order discount is split over its lines in proportion to line totals; cumulative flooring makes the
-- allocated amounts add up to the order discount exactly.
WITH running AS (
  SELECT
    order_item_id,
    order_id,
    variant_id,
    quantity,
    unit_price_vnd,
    line_total_vnd,
    cost_price_vnd,
    sum(line_total_vnd) OVER (PARTITION BY order_id) AS order_lines_vnd,
    sum(line_total_vnd) OVER (PARTITION BY order_id ORDER BY order_item_id ROWS UNBOUNDED PRECEDING) AS running_vnd
  FROM {{ ref('order_items') }}
),
lines AS (
  SELECT
    r.*,
    {{ floor_allocate('least(o.discount_vnd, r.order_lines_vnd)', 'r.running_vnd', 'r.order_lines_vnd') }}
      - {{ floor_allocate('least(o.discount_vnd, r.order_lines_vnd)', 'r.running_vnd - r.line_total_vnd', 'r.order_lines_vnd') }}
      AS allocated_discount_vnd,
    o.channel,
    o.store_key,
    o.customer_key,
    o.order_date,
    o.revenue_date
  FROM running r
  JOIN {{ ref('fact_order') }} o ON o.order_id = r.order_id
)
SELECT
  l.order_item_id,
  l.order_id,
  l.variant_id AS variant_key,
  v.product_id AS product_key,
  l.channel,
  l.store_key,
  l.customer_key,
  l.quantity,
  l.unit_price_vnd,
  l.line_total_vnd,
  l.allocated_discount_vnd,
  l.line_total_vnd - l.allocated_discount_vnd AS revenue_vnd,
  -- Cost of goods uses the cost captured on the order line (design §5).
  l.cost_price_vnd * l.quantity AS cost_vnd,
  l.order_date,
  l.revenue_date
FROM lines l
LEFT JOIN {{ ref('product_variants') }} v ON v.variant_id = l.variant_id
