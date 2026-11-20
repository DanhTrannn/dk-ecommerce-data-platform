WITH deliveries AS (
  SELECT order_id, max_by(is_on_time, delivered_at) AS is_on_time
  FROM {{ ref('fact_shipment') }}
  WHERE delivered_at IS NOT NULL
  GROUP BY order_id
)
SELECT
  r.review_id,
  r.product_id AS product_key,
  r.customer_id AS customer_key,
  r.order_item_id,
  i.order_id,
  r.rating,
  r.status,
  r.created_at AS reviewed_at,
  {{ vn_date('r.created_at') }} AS review_date,
  CASE
    WHEN d.is_on_time THEN 'on_time'
    WHEN NOT d.is_on_time THEN 'late'
    ELSE 'not_shipped'
  END AS delivery_timeliness
FROM {{ source('silver', 'product_reviews') }} r
LEFT JOIN {{ source('silver', 'order_items') }} i ON i.order_item_id = r.order_item_id
LEFT JOIN deliveries d ON d.order_id = i.order_id
