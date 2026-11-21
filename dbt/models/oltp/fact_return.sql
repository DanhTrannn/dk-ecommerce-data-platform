-- A return's succeeded refund pays exactly the refund amounts of its items.
WITH refunds AS (
  SELECT return_id, max(completed_at) AS refunded_at
  FROM {{ source('silver', 'refunds') }}
  WHERE status = 'succeeded' AND return_id IS NOT NULL
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
  CASE WHEN f.return_id IS NOT NULL THEN coalesce(i.refund_amount_vnd, 0) ELSE 0 END AS refund_paid_vnd,
  r.created_at AS requested_at,
  r.resolved_at,
  f.refunded_at,
  {{ vn_date('r.created_at') }} AS request_date,
  {{ vn_date('r.resolved_at') }} AS resolved_date
FROM {{ source('silver', 'return_items') }} i
JOIN {{ source('silver', 'return_requests') }} r ON r.return_id = i.return_id
LEFT JOIN {{ source('silver', 'product_variants') }} v ON v.variant_id = i.variant_id
LEFT JOIN refunds f ON f.return_id = i.return_id
