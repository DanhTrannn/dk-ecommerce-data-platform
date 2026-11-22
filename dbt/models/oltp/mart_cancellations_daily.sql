{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('order_date', order_days()) }}", post_hook="{{ advance_mart(['fact_order', 'dim_store', 'dim_city', 'dim_staff']) }}") }}
SELECT
  order_date,
  channel,
  cancel_reason,
  payment_method,
  count(*) AS cancelled_orders,
  sum(total_vnd) AS cancelled_value_vnd
FROM {{ live('fact_order') }}
WHERE status_group = 'cancelled' AND {{ filter_in('order_date', order_days()) }}
GROUP BY 1, 2, 3, 4
