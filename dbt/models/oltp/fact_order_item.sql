{{ config(materialized='incremental', unique_key='order_item_id', properties={"partitioning": "ARRAY['month(order_date)']"}) }}
-- The order discount is split over its lines in proportion to line totals; cumulative flooring makes the
-- allocated amounts add up to the order discount exactly. A changed order recomputes all of its lines; a line
-- deleted at the source is kept, flagged, with no share of the discount.
WITH
{% if is_incremental() %}
changed AS (
  SELECT order_id AS key FROM {{ ref('fact_order') }} WHERE _ingested_at > {{ watermark() }}
  UNION
  SELECT i.order_id
  FROM {{ ref('order_items') }} i
  JOIN {{ ref('product_variants') }} v ON v.variant_id = i.variant_id
  WHERE v._ingested_at > {{ watermark() }}
),
{% endif %}
running AS (
  SELECT
    order_item_id,
    order_id,
    variant_id,
    quantity,
    unit_price_vnd,
    line_total_vnd,
    cost_price_vnd,
    _is_deleted,
    _deleted_at,
    _ingested_at,
    sum(CASE WHEN NOT _is_deleted THEN line_total_vnd ELSE 0 END) OVER (PARTITION BY order_id) AS order_lines_vnd,
    sum(CASE WHEN NOT _is_deleted THEN line_total_vnd ELSE 0 END)
      OVER (PARTITION BY order_id ORDER BY order_item_id ROWS UNBOUNDED PRECEDING) AS running_vnd
  FROM {{ ref('order_items') }}
  WHERE {{ in_changed('order_id') }}
),
lines AS (
  SELECT
    r.*,
    {{ floor_allocate('least(o.discount_vnd, r.order_lines_vnd)', 'r.running_vnd', 'r.order_lines_vnd') }}
      - {{ floor_allocate('least(o.discount_vnd, r.order_lines_vnd)', 'r.running_vnd - CASE WHEN NOT r._is_deleted THEN r.line_total_vnd ELSE 0 END', 'r.order_lines_vnd') }}
      AS allocated_discount_vnd,
    o.channel,
    o.store_key,
    o.customer_key,
    o.order_date,
    o.revenue_date,
    o._ingested_at AS order_ingested_at
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
  l.revenue_date,
  l._is_deleted,
  l._deleted_at,
  {{ latest('l._ingested_at', 'l.order_ingested_at', 'v._ingested_at') }} AS _ingested_at,
  {{ previous_dates('prev.order_date', 'prev.revenue_date') }} AS _previous_dates
FROM lines l
LEFT JOIN {{ ref('product_variants') }} v ON v.variant_id = l.variant_id
{% if is_incremental() %}
LEFT JOIN {{ this }} prev ON prev.order_item_id = l.order_item_id
{% endif %}
