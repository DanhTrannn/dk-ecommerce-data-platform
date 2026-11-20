SELECT
  p.product_id AS product_key,
  p.name AS product_name,
  p.slug,
  p.category_id AS category_key,
  c.code AS category_code,
  c.name AS category_name,
  p.is_active,
  p.created_at
FROM {{ source('silver', 'products') }} p
LEFT JOIN {{ source('silver', 'categories') }} c ON c.category_id = p.category_id
