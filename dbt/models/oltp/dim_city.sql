SELECT
  c.city_id AS city_key,
  c.code AS city_code,
  c.name AS city_name,
  c.is_active,
  sla.delivery_zone,
  sla.carrier_days,
  sla.sla_days
FROM {{ ref('cities') }} c
LEFT JOIN {{ ref('delivery_sla') }} sla ON sla.city_code = c.code
