SELECT
  CAST(b."order_date" AS date) AS "order_date",
  CAST(b."product_id" AS bigint) AS "product_id",
  CAST(b."product_name" AS varchar) AS "product_name",
  CAST(b."units_sold" AS bigint) AS "units_sold",
  CAST(b."gross_revenue_vnd" AS bigint) AS "gross_revenue_vnd",
  CAST(b."refunded_amount_vnd" AS bigint) AS "refunded_amount_vnd",
  CAST(b."net_revenue_vnd" AS bigint) AS "net_revenue_vnd",
  CAST(b."cogs_vnd" AS bigint) AS "cogs_vnd",
  CAST(b."gross_profit_vnd" AS bigint) AS "gross_profit_vnd",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT i.order_date,p.product_id,p.product_name,sum(i.recognized_quantity) units_sold,
        sum(i.gross_revenue_vnd) gross_revenue_vnd,sum(i.refunded_amount_vnd) refunded_amount_vnd,
        sum(i.net_revenue_vnd) net_revenue_vnd,sum(i.item_cost_vnd) cogs_vnd,sum(i.item_profit_vnd) gross_profit_vnd
        FROM {{ ref('fact_order_item') }} i JOIN {{ ref('dim_product') }} p ON i.product_key=p.product_key WHERE i.order_status IN ('delivered','completed','returned')
        GROUP BY i.order_date,p.product_id,p.product_name
) b
