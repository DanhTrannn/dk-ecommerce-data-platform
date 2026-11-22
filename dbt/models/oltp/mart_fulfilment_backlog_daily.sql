{{ config(materialized='incremental', incremental_strategy='append', pre_hook="{{ delete_in('backlog_date', backlog_days()) }}", post_hook="{{ advance_mart(['fact_order', 'fact_shipment'], backlog_last_day()) }}") }}
-- Online orders still open at the end of each day: confirmed but not dispatched, or dispatched but not yet
-- delivered or failed. Age buckets follow the delivery SLAs (seed delivery_sla): within the shortest (4 days),
-- within the longest (8 days), past every SLA.
WITH shipments AS (
  SELECT order_id, min(dispatched_at) AS dispatched_at, max(coalesce(delivered_at, failed_at)) AS ended_at
  FROM {{ live('fact_shipment') }}
  GROUP BY order_id
),
stages AS (
  SELECT
    o.order_id, o.order_date, o.payment_method, o.total_vnd, 'awaiting_dispatch' AS stage,
    {{ vn_date('o.confirmed_at') }} AS first_day,
    coalesce({{ vn_date('least(s.dispatched_at, o.cancelled_at)') }}, {{ vn_date('s.dispatched_at') }}, {{ vn_date('o.cancelled_at') }}, date_add('day', 1, {{ backlog_last_day() }})) AS end_day
  FROM {{ live('fact_order') }} o
  LEFT JOIN shipments s ON s.order_id = o.order_id
  WHERE o.channel = 'online' AND o.confirmed_at IS NOT NULL
  UNION ALL
  SELECT
    o.order_id, o.order_date, o.payment_method, o.total_vnd, 'in_delivery',
    {{ vn_date('s.dispatched_at') }},
    coalesce({{ vn_date('s.ended_at') }}, date_add('day', 1, {{ backlog_last_day() }}))
  FROM {{ live('fact_order') }} o
  JOIN shipments s ON s.order_id = o.order_id
  WHERE o.channel = 'online' AND s.dispatched_at IS NOT NULL
),
open_days AS (
  SELECT stage, day AS backlog_date, date_diff('day', order_date, day) AS age_days, payment_method, total_vnd
  FROM stages
  CROSS JOIN UNNEST(sequence(first_day, least(date_add('day', -1, end_day), {{ backlog_last_day() }}))) AS calendar(day)
  WHERE first_day < end_day AND first_day <= {{ backlog_last_day() }}
  {% if is_incremental() %}
    AND end_day > (SELECT min(day) FROM ({{ backlog_days() }}))
  {% endif %}
)
SELECT
  backlog_date,
  stage,
  CASE WHEN age_days <= 4 THEN '0-4' WHEN age_days <= 8 THEN '5-8' ELSE '9+' END AS age_bucket_days,
  count(*) AS orders,
  sum(age_days) AS age_days_total,
  sum(CASE WHEN payment_method = 'cod' THEN total_vnd ELSE 0 END) AS cod_pending_vnd
FROM open_days
WHERE {{ filter_in('backlog_date', backlog_days()) }}
GROUP BY 1, 2, 3
