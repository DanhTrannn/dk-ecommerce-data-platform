{{ config(materialized='table') }}

SELECT
  CAST(product_id AS bigint) AS product_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(category_id AS bigint) AS category_id,
  CAST(slug AS varchar) AS slug,
  CAST(name AS varchar) AS name,
  CAST(description AS varchar) AS description,
  CAST(image_url AS varchar) AS image_url,
  CAST(is_active AS boolean) AS is_active,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'products') }}
