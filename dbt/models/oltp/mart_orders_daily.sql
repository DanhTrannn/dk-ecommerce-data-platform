{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('order_date', order_days()) }}", post_hook="{{ advance_mart(['fact_order', 'dim_store', 'dim_city', 'dim_staff']) }}") }}
-- Orders by the day they were placed; status_group splits them into completed, cancelled, failed and processing.
SELECT
  o.order_date,
  o.channel,
  o.store_key,
  s.store_name,
  o.city_key,
  c.city_name,
  o.payment_method,
  count(*) AS orders,
  sum(o.total_vnd) AS order_value_vnd,
  count_if(o.status_group = 'completed') AS completed_orders,
  sum(CASE WHEN o.status_group = 'completed' THEN o.total_vnd ELSE 0 END) AS completed_value_vnd,
  count_if(o.status_group = 'cancelled') AS cancelled_orders,
  sum(CASE WHEN o.status_group = 'cancelled' THEN o.total_vnd ELSE 0 END) AS cancelled_value_vnd,
  count_if(o.status_group = 'failed') AS failed_orders,
  sum(CASE WHEN o.status_group = 'failed' THEN o.total_vnd ELSE 0 END) AS failed_value_vnd,
  count_if(o.status_group = 'processing') AS processing_orders,
  sum(CASE WHEN o.status_group = 'processing' THEN o.total_vnd ELSE 0 END) AS processing_value_vnd
FROM {{ ref('fact_order') }} o
LEFT JOIN {{ ref('dim_store') }} s ON s.store_key = o.store_key
LEFT JOIN {{ ref('dim_city') }} c ON c.city_key = o.city_key
WHERE {{ filter_in('o.order_date', order_days()) }}
GROUP BY 1, 2, 3, 4, 5, 6, 7
