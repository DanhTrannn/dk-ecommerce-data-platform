SELECT
  c.city_id AS city_key,
  c.code AS city_code,
  c.name AS city_name,
  c.is_active,
  sla.delivery_zone,
  sla.sla_days
FROM {{ source('silver', 'cities') }} c
JOIN {{ ref('delivery_sla') }} sla
  ON sla.delivery_zone = CASE WHEN c.code = '{{ var("central_warehouse_city_code") }}' THEN 'intra_city' ELSE 'inter_province' END
