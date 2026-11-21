{{ config(unique_key='city_id') }}

SELECT
  CAST(city_id AS bigint) AS city_id,
  CAST(code AS varchar) AS code,
  CAST(name AS varchar) AS name,
  CAST(is_active AS boolean) AS is_active,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'cities') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
