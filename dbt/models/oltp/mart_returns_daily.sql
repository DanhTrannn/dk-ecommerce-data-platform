-- Return requests by request day; a request's processing time counts once per variant it covers.
WITH requests AS (
  SELECT
    request_date,
    return_id,
    variant_key,
    product_key,
    action_type,
    sum(quantity) AS quantity,
    sum(accepted_quantity) AS accepted_quantity,
    sum(refund_requested_vnd) AS refund_requested_vnd,
    sum(refund_paid_vnd) AS refund_paid_vnd,
    max(date_diff('second', requested_at, resolved_at)) AS processing_seconds
  FROM {{ live('fact_return') }}
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  r.request_date,
  r.variant_key,
  v.sku,
  r.product_key,
  p.product_name,
  r.action_type,
  count(*) AS return_requests,
  sum(r.quantity) AS quantity,
  sum(r.accepted_quantity) AS accepted_quantity,
  sum(r.refund_requested_vnd) AS refund_requested_vnd,
  sum(r.refund_paid_vnd) AS refund_paid_vnd,
  count(r.processing_seconds) AS resolved_requests,
  sum(r.processing_seconds) / 3600e0 AS processing_hours
FROM requests r
LEFT JOIN {{ live('dim_variant') }} v ON v.variant_key = r.variant_key
LEFT JOIN {{ live('dim_product') }} p ON p.product_key = r.product_key
GROUP BY 1, 2, 3, 4, 5, 6
