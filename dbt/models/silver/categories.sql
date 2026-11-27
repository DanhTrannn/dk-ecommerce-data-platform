{{ config(materialized='table') }}

SELECT
  CAST(category_id AS bigint) AS category_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(parent_category_id AS bigint) AS parent_category_id,
  CAST(code AS varchar) AS code,
  CAST(name AS varchar) AS name,
  CAST(is_active AS boolean) AS is_active,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'categories') }}
