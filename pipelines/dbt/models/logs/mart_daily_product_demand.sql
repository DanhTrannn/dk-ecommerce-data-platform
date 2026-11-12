SELECT
  CAST(b.metric_date AS date) AS "metric_date",
  CAST(b.product_key AS varchar) AS "product_key",
  CAST(b.product_id AS bigint) AS "product_id",
  CAST(b.product_name AS varchar) AS "product_name",
  CAST(b.detail_view_count AS bigint) AS "detail_view_count",
  CAST(b.cart_add_count AS bigint) AS "cart_add_count",
  CAST(b.cart_remove_count AS bigint) AS "cart_remove_count",
  CAST(b.wishlist_add_count AS bigint) AS "wishlist_add_count",
  CAST(b.checkout_quote_count AS bigint) AS "checkout_quote_count",
  CAST(b.unique_visitors_count AS bigint) AS "unique_visitors_count",
  CAST(b.cart_to_view_rate_pct AS double) AS "cart_to_view_rate_pct",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH aggregate AS (
SELECT event_date metric_date,resolved_product_key product_key,resolved_product_id product_id,resolved_product_name product_name,
count_if(ecommerce_action='product_detail') detail_view_count,
count_if(ecommerce_action IN ('cart_add','cart_item_set')) cart_add_count,
count_if(ecommerce_action IN ('cart_remove','cart_item_remove')) cart_remove_count,
count_if(ecommerce_action='wishlist_add') wishlist_add_count,count_if(ecommerce_action='checkout_quote') checkout_quote_count,
count(DISTINCT actor_key) unique_visitors_count
FROM {{ ref('int_resolved_events') }} WHERE is_success
AND ecommerce_action IN ('product_detail','cart_add','cart_item_set','cart_remove','cart_item_remove','wishlist_add','checkout_quote')
AND resolved_product_key IS NOT NULL AND resolved_product_key<>''
GROUP BY event_date,resolved_product_key,resolved_product_id,resolved_product_name)
SELECT a.*,round(CASE WHEN detail_view_count>0 THEN cart_add_count*100e0/detail_view_count END,2) cart_to_view_rate_pct FROM aggregate a
) b
