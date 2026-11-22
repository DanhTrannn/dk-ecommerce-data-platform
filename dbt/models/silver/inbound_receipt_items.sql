{{ config(unique_key='item_id') }}

SELECT
  CAST(item_id AS bigint) AS item_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(receipt_id AS bigint) AS receipt_id,
  CAST(variant_id AS bigint) AS variant_id,
  CAST(quantity AS integer) AS quantity,
  CAST(unit_cost_vnd AS bigint) AS unit_cost_vnd,
  CAST(total_cost_vnd AS bigint) AS total_cost_vnd,
  CAST(previous_cost_price_vnd AS bigint) AS previous_cost_price_vnd,
  CAST(new_cost_price_vnd AS bigint) AS new_cost_price_vnd,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'inbound_receipt_items') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
