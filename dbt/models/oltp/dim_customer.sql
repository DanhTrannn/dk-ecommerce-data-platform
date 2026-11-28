SELECT
  c.customer_id AS customer_key,
  c.city_id AS city_key,
  city.name AS city_name,
  c.status,
  c.data_origin,
  c.is_cod_blocked,
  c.boom_count,
  c.created_at,
  {{ vn_date('c.created_at') }} AS created_date,
  c._is_deleted,
  c._deleted_at,
  {{ latest('c._ingested_at', 'city._ingested_at') }} AS _ingested_at
FROM {{ ref('customers') }} c
LEFT JOIN {{ ref('cities') }} city ON city.city_id = c.city_id
WHERE c.role = 'customer'
