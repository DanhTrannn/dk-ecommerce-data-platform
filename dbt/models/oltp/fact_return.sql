{{ config(materialized='incremental', unique_key='return_item_id') }}
-- A return's succeeded refund pays exactly the refund amounts of its items.
WITH
{% if is_incremental() %}
changed AS (
  SELECT return_item_id AS key FROM {{ ref('return_items') }} WHERE _ingested_at > {{ watermark() }}
  UNION
  SELECT i.return_item_id
  FROM {{ ref('return_items') }} i
  JOIN {{ ref('return_requests') }} r ON r.return_id = i.return_id
  LEFT JOIN {{ ref('product_variants') }} v ON v.variant_id = i.variant_id
  WHERE r._ingested_at > {{ watermark() }}
    OR v._ingested_at > {{ watermark() }}
    OR i.return_id IN (SELECT return_id FROM {{ ref('refunds') }} WHERE _ingested_at > {{ watermark() }})
),
{% endif %}
items AS (
  SELECT * FROM {{ ref('return_items') }} WHERE {{ in_changed('return_item_id') }}
),
refunds AS (
  SELECT
    return_id,
    max(completed_at) FILTER (WHERE status = 'succeeded') AS refunded_at,
    count(*) FILTER (WHERE status = 'succeeded') > 0 AS is_refunded,
    max(_ingested_at) AS _ingested_at
  FROM {{ ref('refunds') }}
  WHERE return_id IN (SELECT return_id FROM items)
  GROUP BY return_id
)
SELECT
  i.return_item_id,
  i.return_id,
  r.return_code,
  r.order_id,
  i.order_item_id,
  i.variant_id AS variant_key,
  v.product_id AS product_key,
  i.exchange_variant_id AS exchange_variant_key,
  r.action_type,
  r.status,
  i.inspection_status,
  i.quantity,
  CASE WHEN i.inspection_status = 'passed' THEN i.quantity ELSE 0 END AS accepted_quantity,
  coalesce(i.refund_amount_vnd, 0) AS refund_requested_vnd,
  CASE WHEN f.is_refunded THEN coalesce(i.refund_amount_vnd, 0) ELSE 0 END AS refund_paid_vnd,
  r.created_at AS requested_at,
  r.resolved_at,
  f.refunded_at,
  {{ vn_date('r.created_at') }} AS request_date,
  {{ vn_date('r.resolved_at') }} AS resolved_date,
  {{ latest('i._ingested_at', 'r._ingested_at', 'v._ingested_at', 'f._ingested_at') }} AS _ingested_at,
  {{ previous_dates('prev.request_date', 'prev.resolved_date', vn_date('prev.refunded_at')) }} AS _previous_dates
FROM items i
JOIN {{ ref('return_requests') }} r ON r.return_id = i.return_id
LEFT JOIN {{ ref('product_variants') }} v ON v.variant_id = i.variant_id
LEFT JOIN refunds f ON f.return_id = i.return_id
{% if is_incremental() %}
LEFT JOIN {{ this }} prev ON prev.return_item_id = i.return_item_id
{% endif %}
