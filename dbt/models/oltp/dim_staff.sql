-- Delivery staff and the staff accounts that sold POS orders; keys are prefixed per source.
WITH sellers AS (
  SELECT DISTINCT staff_id
  FROM {{ ref('orders') }}
  WHERE staff_id IS NOT NULL
)
SELECT
  'delivery-' || CAST(staff_id AS varchar) AS staff_key,
  'delivery' AS staff_role,
  staff_id AS source_id,
  full_name AS staff_name,
  CAST(NULL AS bigint) AS store_key,
  is_active,
  _is_deleted,
  _deleted_at,
  _ingested_at
FROM {{ ref('delivery_staff') }}
UNION ALL
SELECT
  'sales-' || CAST(customer_id AS varchar),
  'sales',
  customer_id,
  display_name,
  store_id,
  status = 'active',
  _is_deleted,
  _deleted_at,
  _ingested_at
FROM {{ ref('customers') }}
WHERE role <> 'customer' AND customer_id IN (SELECT staff_id FROM sellers)
