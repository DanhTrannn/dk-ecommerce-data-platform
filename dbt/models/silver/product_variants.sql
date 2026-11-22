{{ config(unique_key='variant_id') }}

SELECT
  CAST(variant_id AS bigint) AS variant_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(product_id AS bigint) AS product_id,
  CAST(sku AS varchar) AS sku,
  CAST(size_code AS varchar) AS size_code,
  CAST(color_code AS varchar) AS color_code,
  CAST(price_vnd AS bigint) AS price_vnd,
  CAST(cost_price_vnd AS bigint) AS cost_price_vnd,
  CAST(is_active AS boolean) AS is_active,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'product_variants') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
