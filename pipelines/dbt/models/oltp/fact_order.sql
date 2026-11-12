SELECT
  CAST(b."order_id" AS bigint) AS "order_id",
  CAST(b."order_number" AS varchar) AS "order_number",
  CAST(b."customer_key" AS bigint) AS "customer_key",
  CAST(b."store_key" AS bigint) AS "store_key",
  CAST(b."order_date_key" AS integer) AS "order_date_key",
  CAST(b."order_date" AS date) AS "order_date",
  CAST(b."channel" AS varchar) AS "channel",
  CAST(b."status" AS varchar) AS "status",
  CAST(b."payment_method" AS varchar) AS "payment_method",
  CAST(b."payment_status" AS varchar) AS "payment_status",
  CAST(b."coupon_id" AS bigint) AS "coupon_id",
  CAST(b."subtotal_vnd" AS bigint) AS "subtotal_vnd",
  CAST(b."discount_vnd" AS bigint) AS "discount_vnd",
  CAST(b."shipping_fee_vnd" AS bigint) AS "shipping_fee_vnd",
  CAST(b."gross_revenue_vnd" AS bigint) AS "gross_revenue_vnd",
  CAST(b."total_cost_vnd" AS bigint) AS "total_cost_vnd",
  CAST(b."gross_profit_vnd" AS bigint) AS "gross_profit_vnd",
  CAST(b."net_revenue_vnd" AS bigint) AS "net_revenue_vnd",
  CAST(b."net_profit_vnd" AS bigint) AS "net_profit_vnd",
  CAST(b."is_boom" AS boolean) AS "is_boom",
  CAST(b."is_delivered" AS boolean) AS "is_delivered",
  CAST(b."order_total_vnd" AS bigint) AS "order_total_vnd",
  CAST(b."refunded_amount_vnd" AS bigint) AS "refunded_amount_vnd",
  CAST(b."created_at" AS timestamp(6) with time zone) AS "created_at",
  CAST(b."order_date_vn" AS date) AS "order_date_vn",
  CAST(b."shipping_region" AS varchar) AS "shipping_region",
  CAST(b."is_revenue_recognized" AS boolean) AS "is_revenue_recognized",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH totals AS (SELECT order_id,sum(gross_revenue_vnd) gross_revenue_vnd,sum(refunded_amount_vnd) refunded_amount_vnd,
sum(net_revenue_vnd) net_revenue_vnd,sum(item_cost_vnd) total_cost_vnd,sum(item_profit_vnd) net_profit_vnd FROM {{ ref('fact_order_item') }} GROUP BY order_id)
SELECT o.order_id,o.order_number,coalesce(o.customer_id,-1) customer_key,coalesce(o.store_id,0) store_key,o.created_at,
c.name shipping_region,o.channel,o.status,o.payment_method,p.status payment_status,o.coupon_id,o.subtotal_vnd,
o.discount_amount_vnd discount_vnd,o.shipping_fee_vnd,o.total_vnd order_total_vnd,t.gross_revenue_vnd,t.refunded_amount_vnd,
t.net_revenue_vnd,t.total_cost_vnd,t.net_profit_vnd gross_profit_vnd,t.net_profit_vnd,o.status='failed_delivery' is_boom,
o.status IN ('delivered','completed') is_delivered,o.status IN ('delivered','completed','returned') is_revenue_recognized,
CAST(at_timezone(o.created_at,'Asia/Ho_Chi_Minh') AS date) order_date,
CAST(at_timezone(o.created_at,'Asia/Ho_Chi_Minh') AS date) order_date_vn,
CAST(date_format(at_timezone(o.created_at,'Asia/Ho_Chi_Minh'),'%Y%m%d') AS integer) order_date_key
FROM {{ pinned('orders') }} o LEFT JOIN totals t ON o.order_id=t.order_id LEFT JOIN {{ pinned('payments') }} p ON o.order_id=p.order_id
LEFT JOIN {{ pinned('cities') }} c ON o.shipping_city_id=c.city_id
) b
