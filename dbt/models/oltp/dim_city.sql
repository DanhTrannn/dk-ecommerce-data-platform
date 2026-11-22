{{ config(materialized='incremental', unique_key='city_key') }}
SELECT
  c.city_id AS city_key,
  c.code AS city_code,
  c.name AS city_name,
  c.is_active,
  sla.delivery_zone,
  sla.carrier_days,
  sla.sla_days,
  c._ingested_at
FROM {{ ref('cities') }} c
LEFT JOIN {{ ref('delivery_sla') }} sla ON sla.city_code = c.code
{% if is_incremental() %}
WHERE c._ingested_at > {{ watermark() }}
{% endif %}
