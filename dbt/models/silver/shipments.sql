{{ config(unique_key='shipment_id') }}

SELECT
  CAST(shipment_id AS bigint) AS shipment_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(shipment_code AS varchar) AS shipment_code,
  CAST(order_id AS bigint) AS order_id,
  CAST(delivery_staff_id AS bigint) AS delivery_staff_id,
  CAST(status AS varchar) AS status,
  CAST(attempt_count AS integer) AS attempt_count,
  CAST(cod_amount_vnd AS bigint) AS cod_amount_vnd,
  CAST(cod_collected_vnd AS bigint) AS cod_collected_vnd,
  CAST(dispatched_at AS timestamp(6) with time zone) AS dispatched_at,
  CAST(delivered_at AS timestamp(6) with time zone) AS delivered_at,
  CAST(failed_at AS timestamp(6) with time zone) AS failed_at,
  CAST(failure_reason AS varchar) AS failure_reason,
  CAST(notes AS varchar) AS notes,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'shipments') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
