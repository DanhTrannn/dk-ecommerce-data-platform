{{ config(materialized='incremental', unique_key='review_id') }}
WITH
{% if is_incremental() %}
changed AS (
  SELECT review_id AS key FROM {{ ref('product_reviews') }} WHERE _ingested_at > {{ watermark() }}
  UNION
  SELECT r.review_id
  FROM {{ ref('product_reviews') }} r
  JOIN {{ ref('order_items') }} i ON i.order_item_id = r.order_item_id
  WHERE i._ingested_at > {{ watermark() }}
    OR i.order_id IN (SELECT order_id FROM {{ ref('fact_shipment') }} WHERE _ingested_at > {{ watermark() }})
),
{% endif %}
reviews AS (
  SELECT r.*, i.order_id, i._ingested_at AS item_ingested_at
  FROM {{ ref('product_reviews') }} r
  LEFT JOIN {{ ref('order_items') }} i ON i.order_item_id = r.order_item_id
  WHERE {{ in_changed('r.review_id') }}
),
deliveries AS (
  SELECT
    order_id,
    max_by(is_on_time, delivered_at) FILTER (WHERE delivered_at IS NOT NULL AND NOT _is_deleted) AS is_on_time,
    max(_ingested_at) AS _ingested_at
  FROM {{ ref('fact_shipment') }}
  WHERE order_id IN (SELECT order_id FROM reviews)
  GROUP BY order_id
)
SELECT
  r.review_id,
  r.product_id AS product_key,
  r.customer_id AS customer_key,
  r.order_item_id,
  r.order_id,
  r.rating,
  r.status,
  r.created_at AS reviewed_at,
  {{ vn_date('r.created_at') }} AS review_date,
  CASE
    WHEN d.is_on_time THEN 'on_time'
    WHEN NOT d.is_on_time THEN 'late'
    ELSE 'not_shipped'
  END AS delivery_timeliness,
  r._is_deleted,
  r._deleted_at,
  {{ latest('r._ingested_at', 'r.item_ingested_at', 'd._ingested_at') }} AS _ingested_at,
  {{ previous_dates('prev.review_date') }} AS _previous_dates
FROM reviews r
LEFT JOIN deliveries d ON d.order_id = r.order_id
{% if is_incremental() %}
LEFT JOIN {{ this }} prev ON prev.review_id = r.review_id
{% endif %}
