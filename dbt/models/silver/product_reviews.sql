{{ config(unique_key='review_id') }}

SELECT
  CAST(review_id AS bigint) AS review_id,
  from_hex(replace(public_id, '-', '')) AS public_id,
  CAST(order_item_id AS bigint) AS order_item_id,
  CAST(customer_id AS bigint) AS customer_id,
  CAST(product_id AS bigint) AS product_id,
  CAST(rating AS integer) AS rating,
  CAST(content AS varchar) AS content,
  CAST(status AS varchar) AS status,
  CAST(moderation_reason AS varchar) AS moderation_reason,
  CAST(moderated_at AS timestamp(6) with time zone) AS moderated_at,
  CAST(created_at AS timestamp(6) with time zone) AS created_at,
  CAST(updated_at AS timestamp(6) with time zone) AS updated_at,
  coalesce(CAST(_is_deleted AS boolean), false) AS _is_deleted,
  CAST(_deleted_at AS timestamp(6) with time zone) AS _deleted_at,
  CAST(_ingested_at AS timestamp(6) with time zone) AS _ingested_at
FROM {{ source('bronze', 'product_reviews') }}
{% if is_incremental() %}
-- Only the Bronze rows written since the last merge; Bronze already dropped older versions.
WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), TIMESTAMP '1970-01-01 00:00:00 UTC') FROM {{ this }})
{% endif %}
