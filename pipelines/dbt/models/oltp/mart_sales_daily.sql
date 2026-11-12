SELECT
  CAST(b."order_date" AS date) AS "order_date",
  CAST(b."channel" AS varchar) AS "channel",
  CAST(b."store_key" AS bigint) AS "store_key",
  CAST(b."category_id" AS bigint) AS "category_id",
  CAST(b."category_name" AS varchar) AS "category_name",
  CAST(b."total_orders" AS bigint) AS "total_orders",
  CAST(b."successful_orders" AS bigint) AS "successful_orders",
  CAST(b."boom_orders" AS bigint) AS "boom_orders",
  CAST(b."items_sold" AS bigint) AS "items_sold",
  CAST(b."gross_revenue_vnd" AS bigint) AS "gross_revenue_vnd",
  CAST(b."cogs_vnd" AS bigint) AS "cogs_vnd",
  CAST(b."gross_profit_vnd" AS bigint) AS "gross_profit_vnd",
  CAST(round(CASE WHEN b.net_revenue_vnd>0 THEN b.gross_profit_vnd*100e0/b.net_revenue_vnd ELSE 0.0 END,2) AS double) AS "gross_profit_margin_pct",
  CAST(b."refunded_amount_vnd" AS bigint) AS "refunded_amount_vnd",
  CAST(b."net_revenue_vnd" AS bigint) AS "net_revenue_vnd",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH category_map AS (SELECT DISTINCT category_id,category_code FROM {{ ref('dim_product') }}), base AS (
        SELECT i.*,o.is_revenue_recognized,o.is_boom,coalesce(c.category_id,0) category_id,
        coalesce(i.category_name_snapshot,'Unknown') category_name FROM {{ ref('fact_order_item') }} i
        JOIN {{ ref('fact_order') }} o ON i.order_id=o.order_id LEFT JOIN category_map c ON i.category_code_snapshot=c.category_code)
        SELECT order_date,channel,store_key,category_id,category_name,count(DISTINCT order_id) total_orders,
        count(DISTINCT CASE WHEN is_revenue_recognized THEN order_id END) successful_orders,
        count(DISTINCT CASE WHEN is_boom THEN order_id END) boom_orders,sum(recognized_quantity) items_sold,
        sum(gross_revenue_vnd) gross_revenue_vnd,sum(refunded_amount_vnd) refunded_amount_vnd,
        sum(net_revenue_vnd) net_revenue_vnd,sum(item_cost_vnd) cogs_vnd,sum(item_profit_vnd) gross_profit_vnd
        FROM base GROUP BY order_date,channel,store_key,category_id,category_name
) b
