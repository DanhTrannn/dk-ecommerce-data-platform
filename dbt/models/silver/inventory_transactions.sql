{{ config(unique_key='transaction_id') }}

SELECT
  CAST(transaction_id AS bigint) AS transaction_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(variant_id AS bigint) AS variant_id,
  CAST(location_type AS varchar) AS location_type,
  CAST(store_id AS bigint) AS store_id,
  CAST(movement_type AS varchar) AS movement_type,
  CAST(quantity_delta AS integer) AS quantity_delta,
  CAST(order_id AS bigint) AS order_id,
  CAST(receipt_id AS bigint) AS receipt_id,
  CAST(return_id AS bigint) AS return_id,
  CAST(reference_code AS varchar) AS reference_code,
  CAST(notes AS varchar) AS notes,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'inventory_transactions') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
