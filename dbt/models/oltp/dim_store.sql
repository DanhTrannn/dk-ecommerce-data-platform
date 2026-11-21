{{ config(materialized='incremental', unique_key='store_key') }}
SELECT
  s.store_id AS store_key,
  s.code AS store_code,
  s.name AS store_name,
  s.city_id AS city_key,
  c.name AS city_name,
  s.address,
  s.phone,
  s.is_active,
  {{ latest('s._ingested_at', 'c._ingested_at') }} AS _ingested_at,
  {{ gold_run() }} AS _gold_run
FROM {{ ref('stores') }} s
LEFT JOIN {{ ref('cities') }} c ON c.city_id = s.city_id
{% if is_incremental() %}
WHERE s._ingested_at > {{ watermark() }} OR c._ingested_at > {{ watermark() }}
{% endif %}
