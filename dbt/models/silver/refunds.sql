{{ config(unique_key='refund_id') }}

SELECT
  CAST(refund_id AS bigint) AS refund_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(payment_id AS bigint) AS payment_id,
  CAST(return_id AS bigint) AS return_id,
  CAST(refund_idempotency_key AS varchar) AS refund_idempotency_key,
  CAST(status AS varchar) AS status,
  CAST(amount_vnd AS bigint) AS amount_vnd,
  CAST(reason AS varchar) AS reason,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(completed_at AS timestamp(6) with time zone) AS completed_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'refunds') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
