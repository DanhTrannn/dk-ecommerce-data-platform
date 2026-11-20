-- Online orders still open at the end of each day: confirmed but not dispatched, or dispatched but not yet
-- delivered or failed. Age buckets follow the SLAs (2 and 5 days).
WITH shipments AS (
  SELECT order_id, min(dispatched_at) AS dispatched_at, max(coalesce(delivered_at, failed_at)) AS ended_at
  FROM {{ ref('fact_shipment') }}
  GROUP BY order_id
),
stages AS (
  SELECT
    o.order_id, o.order_date, o.payment_method, o.total_vnd, 'awaiting_dispatch' AS stage,
    {{ vn_date('o.confirmed_at') }} AS first_day,
    coalesce({{ vn_date('least(s.dispatched_at, o.cancelled_at)') }}, {{ vn_date('s.dispatched_at') }}, {{ vn_date('o.cancelled_at') }}, date_add('day', 1, {{ business_date() }})) AS end_day
  FROM {{ ref('fact_order') }} o
  LEFT JOIN shipments s ON s.order_id = o.order_id
  WHERE o.channel = 'online' AND o.confirmed_at IS NOT NULL
  UNION ALL
  SELECT
    o.order_id, o.order_date, o.payment_method, o.total_vnd, 'in_delivery',
    {{ vn_date('s.dispatched_at') }},
    coalesce({{ vn_date('s.ended_at') }}, date_add('day', 1, {{ business_date() }}))
  FROM {{ ref('fact_order') }} o
  JOIN shipments s ON s.order_id = o.order_id
  WHERE o.channel = 'online' AND s.dispatched_at IS NOT NULL
),
open_days AS (
  SELECT stage, day AS backlog_date, date_diff('day', order_date, day) AS age_days, payment_method, total_vnd
  FROM stages
  CROSS JOIN UNNEST(sequence(first_day, least(date_add('day', -1, end_day), {{ business_date() }}))) AS calendar(day)
  WHERE first_day < end_day AND first_day <= {{ business_date() }}
)
SELECT
  backlog_date,
  stage,
  CASE WHEN age_days <= 2 THEN '0-2' WHEN age_days <= 5 THEN '3-5' ELSE '6+' END AS age_bucket_days,
  count(*) AS orders,
  sum(age_days) AS age_days_total,
  sum(CASE WHEN payment_method = 'cod' THEN total_vnd ELSE 0 END) AS cod_pending_vnd
FROM open_days
GROUP BY 1, 2, 3
