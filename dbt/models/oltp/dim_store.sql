SELECT
  s.store_id AS store_key,
  s.code AS store_code,
  s.name AS store_name,
  s.city_id AS city_key,
  c.name AS city_name,
  s.address,
  s.phone,
  s.is_active
FROM {{ ref('stores') }} s
LEFT JOIN {{ ref('cities') }} c ON c.city_id = s.city_id
