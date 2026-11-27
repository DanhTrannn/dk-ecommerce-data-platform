-- Registered customers' recognized orders (same basis as mart_sales_daily); walk-in POS sales have no customer.
-- Net revenue per order: merchandise after discount minus the order's succeeded refunds.
SELECT
  o.customer_key,
  c.city_key,
  c.city_name,
  min(o.revenue_date) AS first_order_date,
  max(o.revenue_date) AS last_order_date,
  count(*) AS orders,
  sum(o.subtotal_vnd - o.discount_vnd - o.refunded_vnd) AS net_revenue_vnd,
  sum(o.subtotal_vnd - o.discount_vnd - o.refunded_vnd) / CAST(count(*) AS double) AS avg_order_value_vnd,
  count_if(o.channel = 'online') AS online_orders,
  count_if(o.channel = 'pos') AS pos_orders,
  count_if(o.channel = 'online') / CAST(count(*) AS double) AS online_share,
  count(*) > 1 AS is_repeat_customer
FROM {{ live('fact_order') }} o
JOIN {{ live('dim_customer') }} c ON c.customer_key = o.customer_key
WHERE o.revenue_date IS NOT NULL
GROUP BY 1, 2, 3
