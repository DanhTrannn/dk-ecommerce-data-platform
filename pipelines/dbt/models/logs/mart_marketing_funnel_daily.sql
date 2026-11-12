SELECT
  CAST(b.metric_date AS date) AS "metric_date",
  CAST(b.observed_event_count AS bigint) AS "observed_event_count",
  CAST(b.product_views AS bigint) AS "product_views",
  CAST(b.cart_additions AS bigint) AS "cart_additions",
  CAST(b.checkout_initiations AS bigint) AS "checkout_initiations",
  CAST(b.orders_completed AS bigint) AS "orders_completed",
  CAST(b.view_to_cart_rate_pct AS double) AS "view_to_cart_rate_pct",
  CAST(b.cart_to_checkout_rate_pct AS double) AS "cart_to_checkout_rate_pct",
  CAST(b.checkout_to_order_rate_pct AS double) AS "checkout_to_order_rate_pct",
  CAST(b.overall_conversion_rate_pct AS double) AS "overall_conversion_rate_pct",
  CAST(b.unique_visitors AS bigint) AS "unique_visitors",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH aggregate AS (
SELECT event_date metric_date,count(*) observed_event_count,
count_if(is_success AND ecommerce_action IN ('product_detail','catalog_search')) product_views,
count_if(is_success AND ecommerce_action IN ('cart_add','cart_item_set')) cart_additions,
count_if(is_success AND ecommerce_action IN ('checkout_quote','checkout_submit')) checkout_initiations,
count_if(is_success AND ecommerce_action='checkout_submit') orders_completed,count(DISTINCT actor_key) unique_visitors
FROM {{ pinned('fact_web_events') }} WHERE event_ts <= (SELECT observed_at FROM {{ clock() }} clock) GROUP BY event_date)
SELECT a.*,CASE WHEN product_views>0 THEN cart_additions*100e0/product_views END view_to_cart_rate_pct,
CASE WHEN cart_additions>0 THEN checkout_initiations*100e0/cart_additions END cart_to_checkout_rate_pct,
CASE WHEN checkout_initiations>0 THEN orders_completed*100e0/checkout_initiations END checkout_to_order_rate_pct,
CASE WHEN product_views>0 THEN orders_completed*100e0/product_views END overall_conversion_rate_pct FROM aggregate a
) b
