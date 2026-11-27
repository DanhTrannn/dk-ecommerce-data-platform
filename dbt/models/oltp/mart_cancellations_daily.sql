SELECT
  order_date,
  channel,
  cancel_reason,
  payment_method,
  count(*) AS cancelled_orders,
  sum(total_vnd) AS cancelled_value_vnd
FROM {{ live('fact_order') }}
WHERE status_group = 'cancelled'
GROUP BY 1, 2, 3, 4
