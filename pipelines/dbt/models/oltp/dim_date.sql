-- Whole calendar years covering every order, stock movement and the run's as-of date.
WITH bounds AS (
  SELECT
    date_trunc('year', least(coalesce(o.first_day, {{ business_date() }}), coalesce(t.first_day, {{ business_date() }}), {{ business_date() }})) AS first_day,
    date_add('day', -1, date_add('year', 1, date_trunc('year',
      greatest(coalesce(o.last_day, {{ business_date() }}), coalesce(t.last_day, {{ business_date() }}), {{ business_date() }})))) AS last_day
  FROM (SELECT min({{ vn_date('created_at') }}) AS first_day, max({{ vn_date('created_at') }}) AS last_day FROM {{ source('silver', 'orders') }}) o
  CROSS JOIN (SELECT min({{ vn_date('created_at') }}) AS first_day, max({{ vn_date('created_at') }}) AS last_day FROM {{ source('silver', 'inventory_transactions') }}) t
),
days AS (
  SELECT day AS date_day FROM bounds CROSS JOIN UNNEST(sequence(bounds.first_day, bounds.last_day)) AS calendar(day)
),
events AS (
  SELECT
    event_date,
    max(CASE WHEN event_type = 'holiday' THEN event_name END) AS holiday_name,
    max(CASE WHEN event_type = 'sale' THEN event_name END) AS sale_event
  FROM {{ ref('calendar_events') }}
  GROUP BY event_date
)
SELECT
  d.date_day,
  year(d.date_day) AS year,
  quarter(d.date_day) AS quarter,
  month(d.date_day) AS month,
  date_format(CAST(d.date_day AS timestamp), '%M') AS month_name,
  week_of_year(d.date_day) AS iso_week,
  day_of_month(d.date_day) AS day_of_month,
  day_of_week(d.date_day) AS iso_day_of_week,
  date_format(CAST(d.date_day AS timestamp), '%W') AS day_name,
  day_of_week(d.date_day) IN (6, 7) AS is_weekend,
  e.holiday_name IS NOT NULL AS is_holiday,
  e.holiday_name,
  e.sale_event IS NOT NULL AS is_sale_day,
  e.sale_event
FROM days d
LEFT JOIN events e ON e.event_date = d.date_day
