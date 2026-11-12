SELECT
  CAST(b."order_date" AS date) AS "order_date",
  CAST(b."channel" AS varchar) AS "channel",
  CAST(b."store_key" AS bigint) AS "store_key",
  CAST(b."store_name" AS varchar) AS "store_name",
  CAST(b."total_orders" AS bigint) AS "total_orders",
  CAST(b."recognized_orders" AS bigint) AS "recognized_orders",
  CAST(b."revenue_orders" AS bigint) AS "revenue_orders",
  CAST(b."boom_orders" AS bigint) AS "boom_orders",
  CAST(b."return_requests_count" AS bigint) AS "return_requests_count",
  CAST(b."order_total_vnd" AS bigint) AS "order_total_vnd",
  CAST(b."gross_revenue_vnd" AS bigint) AS "gross_revenue_vnd",
  CAST(b."refunded_amount_vnd" AS bigint) AS "refunded_amount_vnd",
  CAST(b."net_revenue_vnd" AS bigint) AS "net_revenue_vnd",
  CAST(b."cogs_vnd" AS bigint) AS "cogs_vnd",
  CAST(b."gross_profit_vnd" AS bigint) AS "gross_profit_vnd",
  CAST(b."cod_in_transit_vnd" AS bigint) AS "cod_in_transit_vnd",
  CAST(b."cod_shipments_count" AS bigint) AS "cod_shipments_count",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH r AS (SELECT order_id,count(DISTINCT return_id) requests FROM {{ pinned('return_requests') }}
        WHERE status<>'cancelled' GROUP BY order_id),s AS (SELECT order_id,count(shipment_id) active
        FROM {{ ref('fact_shipment') }} WHERE status IN ('assigned','picked_up','in_transit') GROUP BY order_id), base AS (
        SELECT o.*,coalesce(r.requests,0) requests,CASE WHEN o.channel='online' OR o.store_key=0 THEN 0 ELSE o.store_key END served_store,
        CASE WHEN o.payment_method='cod' THEN coalesce(s.active,0) ELSE 0 END cod_shipments FROM {{ ref('fact_order') }} o
        LEFT JOIN r ON o.order_id=r.order_id LEFT JOIN s ON o.order_id=s.order_id), labels AS (
        SELECT b.*,CASE WHEN b.served_store=0 THEN 'Kênh Online Toàn Quốc' ELSE coalesce(d.store_name,'Chưa xác định') END store_name
        FROM base b LEFT JOIN {{ ref('dim_store') }} d ON b.served_store=d.store_key)
        SELECT order_date,channel,served_store store_key,store_name,count(order_id) total_orders,
        sum((CASE WHEN is_revenue_recognized THEN 1 ELSE 0 END)) recognized_orders,sum((CASE WHEN net_revenue_vnd>0 THEN 1 ELSE 0 END)) revenue_orders,
        sum((CASE WHEN is_boom THEN 1 ELSE 0 END)) boom_orders,sum(requests) return_requests_count,sum(order_total_vnd) order_total_vnd,
        sum(gross_revenue_vnd) gross_revenue_vnd,sum(refunded_amount_vnd) refunded_amount_vnd,sum(net_revenue_vnd) net_revenue_vnd,
        sum(total_cost_vnd) cogs_vnd,sum(gross_profit_vnd) gross_profit_vnd,sum(order_total_vnd*cod_shipments) cod_in_transit_vnd,
        sum(cod_shipments) cod_shipments_count FROM labels GROUP BY order_date,channel,served_store,store_name
) b
