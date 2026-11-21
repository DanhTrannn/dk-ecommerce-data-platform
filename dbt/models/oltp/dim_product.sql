{{ config(materialized='incremental', unique_key='product_key') }}
SELECT
  p.product_id AS product_key,
  p.name AS product_name,
  p.slug,
  p.category_id AS category_key,
  c.code AS category_code,
  c.name AS category_name,
  p.is_active,
  p.created_at,
  {{ latest('p._ingested_at', 'c._ingested_at') }} AS _ingested_at,
  {{ gold_run() }} AS _gold_run
FROM {{ ref('products') }} p
LEFT JOIN {{ ref('categories') }} c ON c.category_id = p.category_id
{% if is_incremental() %}
WHERE p._ingested_at > {{ watermark() }} OR c._ingested_at > {{ watermark() }}
{% endif %}
