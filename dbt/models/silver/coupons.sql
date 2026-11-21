{{ config(unique_key='coupon_id') }}

SELECT
  CAST(coupon_id AS bigint) AS coupon_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(code_normalized AS varchar) AS code_normalized,
  CAST(discount_type AS varchar) AS discount_type,
  CAST(discount_value AS bigint) AS discount_value,
  CAST(minimum_subtotal_vnd AS bigint) AS minimum_subtotal_vnd,
  CAST(starts_at AS timestamp(6) with time zone) AS starts_at,
  CAST(ends_at AS timestamp(6) with time zone) AS ends_at,
  CAST(is_active AS boolean) AS is_active,
  CAST(total_usage_limit AS integer) AS total_usage_limit,
  CAST(per_customer_usage_limit AS integer) AS per_customer_usage_limit,
  CAST(used_count AS integer) AS used_count,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'coupons') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
