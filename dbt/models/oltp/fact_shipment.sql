{{ config(materialized='incremental', unique_key='shipment_id') }}
-- On time: delivered within the receiving city's SLA counted from order placement (design §5).
WITH
{% if is_incremental() %}
changed AS (
  SELECT shipment_id AS key FROM {{ ref('shipments') }} WHERE _ingested_at > {{ watermark() }}
  UNION
  SELECT s.shipment_id
  FROM {{ ref('shipments') }} s
  JOIN {{ ref('fact_order') }} o ON o.order_id = s.order_id
  LEFT JOIN {{ ref('dim_city') }} c ON c.city_key = o.city_key
  WHERE o._ingested_at > {{ watermark() }} OR c._ingested_at > {{ watermark() }}
),
{% endif %}
shipments AS (
  SELECT * FROM {{ ref('shipments') }} WHERE {{ in_changed('shipment_id') }}
)
SELECT
  s.shipment_id,
  s.shipment_code,
  s.order_id,
  'delivery-' || CAST(s.delivery_staff_id AS varchar) AS delivery_staff_key,
  o.city_key,
  o.payment_method,
  s.status,
  s.attempt_count,
  o.created_at AS order_created_at,
  s.created_at,
  s.dispatched_at,
  s.delivered_at,
  s.failed_at,
  {{ vn_date('coalesce(s.delivered_at, s.failed_at)') }} AS end_date,
  s.cod_amount_vnd,
  coalesce(s.cod_collected_vnd, 0) AS cod_collected_vnd,
  c.sla_days,
  CASE
    WHEN s.delivered_at IS NOT NULL
    THEN {{ vn_date('s.delivered_at') }} <= date_add('day', c.sla_days, {{ vn_date('o.created_at') }})
  END AS is_on_time,
  {{ latest('s._ingested_at', 'o._ingested_at', 'c._ingested_at') }} AS _ingested_at,
  {{ previous_dates('prev.end_date', vn_date('prev.created_at'), vn_date('prev.dispatched_at'), vn_date('prev.order_created_at')) }} AS _previous_dates
FROM shipments s
JOIN {{ ref('fact_order') }} o ON o.order_id = s.order_id
LEFT JOIN {{ ref('dim_city') }} c ON c.city_key = o.city_key
{% if is_incremental() %}
LEFT JOIN {{ this }} prev ON prev.shipment_id = s.shipment_id
{% endif %}
