-- cost_price_vnd is the current cost; stock is valued at it (design §5).
SELECT
  variant_id AS variant_key,
  product_id AS product_key,
  sku,
  size_code,
  color_code,
  price_vnd,
  cost_price_vnd,
  is_active
FROM {{ source('silver', 'product_variants') }}
