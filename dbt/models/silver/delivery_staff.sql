{{ config(unique_key='staff_id') }}

SELECT
  CAST(staff_id AS bigint) AS staff_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(full_name AS varchar) AS full_name,
  CAST(vehicle_plate AS varchar) AS vehicle_plate,
  CAST(is_active AS boolean) AS is_active,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'delivery_staff') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
