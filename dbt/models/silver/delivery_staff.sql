{{ config(materialized='table') }}

SELECT
  CAST(staff_id AS bigint) AS staff_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(full_name AS varchar) AS full_name,
  CAST(vehicle_plate AS varchar) AS vehicle_plate,
  CAST(is_active AS boolean) AS is_active,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'delivery_staff') }}
