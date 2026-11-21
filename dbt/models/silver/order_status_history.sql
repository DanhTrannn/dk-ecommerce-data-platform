{{ config(unique_key='order_status_history_id') }}

SELECT
  CAST(order_status_history_id AS bigint) AS order_status_history_id,
  CAST(order_id AS bigint) AS order_id,
  CAST(from_status AS varchar) AS from_status,
  CAST(to_status AS varchar) AS to_status,
  CAST(transition_source AS varchar) AS transition_source,
  CAST(reason AS varchar) AS reason,
  CAST(transition_idempotency_key AS varchar) AS transition_idempotency_key,
  CAST(transitioned_at AS timestamp(6) with time zone) AS transitioned_at,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'order_status_history') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
