{{ config(materialized='ephemeral') }}
WITH products AS (
  SELECT product_id,name,slug,
    CASE WHEN length(public_id)=16 THEN lower(to_hex(public_id)) ELSE replace(lower(from_utf8(public_id)),'-','') END uuid
  FROM {{ pinned('products') }}
), tokens AS (
  SELECT product_id,name,slug token FROM products
  UNION ALL SELECT product_id,name,uuid FROM products
  UNION ALL SELECT product_id,name,CAST(product_id AS varchar) FROM products
), product_map AS (SELECT DISTINCT product_id,name,token FROM tokens), variants AS (
  SELECT v.product_id,p.name,
    CASE WHEN length(v.public_id)=16 THEN lower(to_hex(v.public_id)) ELSE replace(lower(from_utf8(v.public_id)),'-','') END token
  FROM {{ pinned('product_variants') }} v LEFT JOIN products p ON v.product_id=p.product_id
), keyed AS (
  SELECT e.*,
    CASE WHEN regexp_like(lower(product_key),'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      THEN replace(lower(product_key),'-','') ELSE lower(product_key) END product_token,
    replace(lower(variant_key),'-','') variant_token
  FROM {{ pinned('fact_web_events') }} e
  WHERE event_ts <= (SELECT observed_at FROM {{ clock() }} clock)
)
SELECT e.*,coalesce(p.product_id,v.product_id) resolved_product_id,coalesce(p.name,v.name) resolved_product_name,
  coalesce(CAST(coalesce(p.product_id,v.product_id) AS varchar),
    CASE WHEN length(e.product_key)>0 THEN 'raw-product:'||e.product_key
      WHEN length(e.variant_key)>0 THEN 'raw-variant:'||e.variant_key END) resolved_product_key
FROM keyed e LEFT JOIN product_map p ON e.product_token=p.token LEFT JOIN variants v ON e.variant_token=v.token
