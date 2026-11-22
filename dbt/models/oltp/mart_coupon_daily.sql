{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('order_date', order_days()) }}", post_hook="{{ advance_mart(['fact_order', 'dim_store', 'dim_city', 'dim_staff']) }}") }}
SELECT
  order_date,
  coupon_id,
  coupon_code,
  coupon_discount_pct,
  sale_event,
  count(*) AS orders,
  sum(discount_vnd) AS discount_vnd,
  sum(total_vnd) AS order_value_vnd,
  count_if(status_group = 'cancelled') AS cancelled_orders
FROM {{ live('fact_order') }}
WHERE coupon_id IS NOT NULL AND {{ filter_in('order_date', order_days()) }}
GROUP BY 1, 2, 3, 4, 5
