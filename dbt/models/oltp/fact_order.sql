{{ config(properties={"partitioning": "ARRAY['month(order_date)']"}) }}
-- Revenue is recognized on the day an order is both completed and paid (the later of the two, design §5).
WITH cancellations AS (
  SELECT order_id, max_by(reason, transitioned_at) AS cancel_reason
  FROM {{ ref('order_status_history') }}
  WHERE to_status = 'cancelled'
  GROUP BY order_id
),
items AS (
  SELECT order_id, sum(quantity) AS item_quantity
  FROM {{ ref('order_items') }}
  GROUP BY order_id
),
refunds AS (
  SELECT p.order_id, sum(r.amount_vnd) AS refunded_vnd
  FROM {{ ref('refunds') }} r
  JOIN {{ ref('payments') }} p ON p.payment_id = r.payment_id
  WHERE r.status = 'succeeded'
  GROUP BY p.order_id
)
SELECT
  o.order_id,
  o.order_number,
  o.channel,
  o.store_id AS store_key,
  -- Online orders ship to shipping_city_id; POS orders are handed over in the store's city.
  coalesce(o.shipping_city_id, s.city_id) AS city_key,
  o.customer_id AS customer_key,
  CASE WHEN o.staff_id IS NOT NULL THEN 'sales-' || CAST(o.staff_id AS varchar) END AS sales_staff_key,
  o.payment_method,
  o.coupon_id,
  o.coupon_code_snapshot AS coupon_code,
  CASE WHEN o.coupon_type_snapshot = 'percentage' THEN o.coupon_value_snapshot END AS coupon_discount_pct,
  -- The generator names sale-campaign coupons SALE-<event>.
  CASE WHEN o.coupon_code_snapshot LIKE 'SALE-%' THEN substr(o.coupon_code_snapshot, 6) END AS sale_event,
  o.status,
  CASE
    WHEN o.status IN ('completed', 'returned') THEN 'completed'
    WHEN o.status = 'cancelled' THEN 'cancelled'
    WHEN o.status IN ('failed_delivery', 'payment_failed') THEN 'failed'
    ELSE 'processing'
  END AS status_group,
  o.created_at,
  o.paid_at,
  o.confirmed_at,
  o.completed_at,
  o.cancelled_at,
  {{ vn_date('o.created_at') }} AS order_date,
  CASE WHEN o.completed_at IS NOT NULL AND o.paid_at IS NOT NULL THEN {{ vn_date('greatest(o.completed_at, o.paid_at)') }} END AS revenue_date,
  {{ vn_date('o.cancelled_at') }} AS cancelled_date,
  c.cancel_reason,
  coalesce(i.item_quantity, 0) AS item_quantity,
  o.subtotal_vnd,
  o.discount_amount_vnd AS discount_vnd,
  o.shipping_fee_vnd,
  o.total_vnd,
  coalesce(r.refunded_vnd, 0) AS refunded_vnd
FROM {{ ref('orders') }} o
LEFT JOIN {{ ref('stores') }} s ON s.store_id = o.store_id
LEFT JOIN cancellations c ON c.order_id = o.order_id
LEFT JOIN items i ON i.order_id = o.order_id
LEFT JOIN refunds r ON r.order_id = o.order_id
