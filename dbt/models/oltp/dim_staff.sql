{{ config(materialized='incremental', unique_key='staff_key') }}
-- Delivery staff and the staff accounts that sold POS orders; keys are prefixed per source. An incremental run
-- adds the sellers of new orders and refreshes the staff already in the table.
WITH sellers AS (
  SELECT DISTINCT staff_id
  FROM {{ ref('orders') }}
  WHERE staff_id IS NOT NULL
  {% if is_incremental() %}
    AND _ingested_at > {{ watermark() }}
  {% endif %}
)
SELECT
  'delivery-' || CAST(staff_id AS varchar) AS staff_key,
  'delivery' AS staff_role,
  staff_id AS source_id,
  full_name AS staff_name,
  CAST(NULL AS bigint) AS store_key,
  is_active,
  _ingested_at,
  {{ gold_run() }} AS _gold_run
FROM {{ ref('delivery_staff') }}
{% if is_incremental() %}
WHERE _ingested_at > {{ watermark() }}
{% endif %}
UNION ALL
SELECT
  'sales-' || CAST(customer_id AS varchar),
  'sales',
  customer_id,
  display_name,
  store_id,
  status = 'active',
  _ingested_at,
  {{ gold_run() }}
FROM {{ ref('customers') }}
WHERE role <> 'customer'
  AND (
    customer_id IN (SELECT staff_id FROM sellers)
    {% if is_incremental() %}
      OR (_ingested_at > {{ watermark() }}
          AND customer_id IN (SELECT source_id FROM {{ this }} WHERE staff_role = 'sales'))
    {% endif %}
  )
