{{ config(unique_key='payment_id') }}

SELECT
  CAST(payment_id AS bigint) AS payment_id,
  CAST(payment_reference AS varchar) AS payment_reference,
  CAST(order_id AS bigint) AS order_id,
  CAST(payment_idempotency_key AS varchar) AS payment_idempotency_key,
  CAST(status AS varchar) AS status,
  CAST(amount_vnd AS bigint) AS amount_vnd,
  CAST(failure_code AS varchar) AS failure_code,
  CAST(attempted_at AS timestamp(6) with time zone) AS attempted_at,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'payments') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
