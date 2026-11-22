{{ config(unique_key='order_item_id') }}

SELECT
  CAST(order_item_id AS bigint) AS order_item_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(order_id AS bigint) AS order_id,
  CAST(variant_id AS bigint) AS variant_id,
  from_hex(replace(product_public_id_snapshot, '-', '')) AS product_public_id_snapshot,
  CAST(category_code_snapshot AS varchar) AS category_code_snapshot,
  CAST(category_name_snapshot AS varchar) AS category_name_snapshot,
  CAST(product_name_snapshot AS varchar) AS product_name_snapshot,
  CAST(sku_snapshot AS varchar) AS sku_snapshot,
  CAST(size_code_snapshot AS varchar) AS size_code_snapshot,
  CAST(color_code_snapshot AS varchar) AS color_code_snapshot,
  CAST(unit_price_vnd AS bigint) AS unit_price_vnd,
  CAST(cost_price_vnd AS bigint) AS cost_price_vnd,
  CAST(quantity AS integer) AS quantity,
  CAST(line_total_vnd AS bigint) AS line_total_vnd,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'order_items') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
