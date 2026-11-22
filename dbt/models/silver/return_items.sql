{{ config(unique_key='return_item_id') }}

SELECT
  CAST(return_item_id AS bigint) AS return_item_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(return_id AS bigint) AS return_id,
  CAST(order_item_id AS bigint) AS order_item_id,
  CAST(variant_id AS bigint) AS variant_id,
  CAST(quantity AS integer) AS quantity,
  CAST(exchange_variant_id AS bigint) AS exchange_variant_id,
  CAST(refund_amount_vnd AS bigint) AS refund_amount_vnd,
  CAST(inspection_status AS varchar) AS inspection_status,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'return_items') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
