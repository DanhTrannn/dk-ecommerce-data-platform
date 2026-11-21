{{ config(unique_key='customer_id') }}

SELECT
  CAST(customer_id AS bigint) AS customer_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(role AS varchar) AS role,
  CAST(display_name AS varchar) AS display_name,
  CAST(status AS varchar) AS status,
  CAST(city_id AS bigint) AS city_id,
  CAST(store_id AS bigint) AS store_id,
  CAST(is_cod_blocked AS boolean) AS is_cod_blocked,
  CAST(boom_count AS integer) AS boom_count,
  CAST(data_origin AS varchar) AS data_origin,
  CAST(generation_run_id AS varchar) AS generation_run_id,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'customers') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
