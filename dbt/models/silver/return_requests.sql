{{ config(unique_key='return_id') }}

SELECT
  CAST(return_id AS bigint) AS return_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(return_code AS varchar) AS return_code,
  CAST(order_id AS bigint) AS order_id,
  CAST(customer_id AS bigint) AS customer_id,
  CAST(action_type AS varchar) AS action_type,
  CAST(status AS varchar) AS status,
  CAST(customer_reason AS varchar) AS customer_reason,
  CAST(admin_note AS varchar) AS admin_note,
  CAST(reviewed_at AS timestamp(6) with time zone) AS reviewed_at,
  CAST(resolved_at AS timestamp(6) with time zone) AS resolved_at,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'return_requests') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
