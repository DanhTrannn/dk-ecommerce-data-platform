{{ config(unique_key='order_id') }}

SELECT
  CAST(order_id AS bigint) AS order_id,
  CAST(order_number AS varchar) AS order_number,
  CAST(cart_id AS bigint) AS cart_id,
  CAST(customer_id AS bigint) AS customer_id,
  CAST(checkout_idempotency_key AS varchar) AS checkout_idempotency_key,
  CAST(coupon_id AS bigint) AS coupon_id,
  CAST(status AS varchar) AS status,
  CAST(payment_method AS varchar) AS payment_method,
  CAST(subtotal_vnd AS bigint) AS subtotal_vnd,
  CAST(coupon_code_snapshot AS varchar) AS coupon_code_snapshot,
  CAST(coupon_type_snapshot AS varchar) AS coupon_type_snapshot,
  CAST(coupon_value_snapshot AS bigint) AS coupon_value_snapshot,
  CAST(discount_amount_vnd AS bigint) AS discount_amount_vnd,
  CAST(shipping_fee_vnd AS bigint) AS shipping_fee_vnd,
  CAST(total_vnd AS bigint) AS total_vnd,
  CAST(shipping_city_id AS bigint) AS shipping_city_id,
  CAST(data_origin AS varchar) AS data_origin,
  CAST(generation_run_id AS varchar) AS generation_run_id,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  CAST(paid_at AS timestamp(6) with time zone) AS paid_at,
  CAST(confirmed_at AS timestamp(6) with time zone) AS confirmed_at,
  CAST(completed_at AS timestamp(6) with time zone) AS completed_at,
  CAST(cancelled_at AS timestamp(6) with time zone) AS cancelled_at,
  CAST(store_id AS bigint) AS store_id,
  CAST(channel AS varchar) AS channel,
  CAST(staff_id AS bigint) AS staff_id,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'orders') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
