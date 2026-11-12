SELECT
  CAST(b."summary_key" AS varchar) AS "summary_key",
  CAST(b."order_rows" AS bigint) AS "order_rows",
  CAST(b."pending_orders" AS bigint) AS "pending_orders",
  CAST(b."over_two_hours_orders" AS bigint) AS "over_two_hours_orders",
  CAST(b."average_waiting_hours" AS double) AS "average_waiting_hours",
  CAST(b."shipment_rows" AS bigint) AS "shipment_rows",
  CAST(b."problematic_shipments" AS bigint) AS "problematic_shipments",
  CAST(b."failed_shipments" AS bigint) AS "failed_shipments",
  CAST(b."return_rows" AS bigint) AS "return_rows",
  CAST(b."return_requests_count" AS bigint) AS "return_requests_count",
  CAST(b."refund_rows" AS bigint) AS "refund_rows",
  CAST(b."successful_refunds_count" AS bigint) AS "successful_refunds_count",
  CAST(b."successful_refund_amount_vnd" AS bigint) AS "successful_refund_amount_vnd",
  CAST(b."operations_observed_at" AS timestamp(6) with time zone) AS "operations_observed_at",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH o AS (SELECT count(order_id) order_rows,
        sum((CASE WHEN status IN ('paid','confirmed') THEN 1 ELSE 0 END)) pending_orders,
        sum((CASE WHEN status IN ('paid','confirmed') AND created_at<=c.observed_at-INTERVAL '2' HOUR THEN 1 ELSE 0 END)) over_two_hours_orders,
        avg(CASE WHEN status IN ('paid','confirmed') AND created_at<=c.observed_at
        THEN (FLOOR(to_unixtime(c.observed_at))-FLOOR(to_unixtime(created_at)))/3600e0 END) average_waiting_hours
        FROM {{ ref('fact_order') }} CROSS JOIN {{ clock() }} c),s AS (SELECT count(shipment_id) shipment_rows,
        sum((CASE WHEN attempt_count>1 OR status='failed' THEN 1 ELSE 0 END)) problematic_shipments,
        sum((CASE WHEN status='failed' THEN 1 ELSE 0 END)) failed_shipments FROM {{ ref('fact_shipment') }}),r AS (
        SELECT count(return_id) return_rows,sum((CASE WHEN status<>'cancelled' THEN 1 ELSE 0 END)) return_requests_count FROM {{ pinned('return_requests') }}),
        f AS (SELECT count(refund_id) refund_rows,sum((CASE WHEN status='succeeded' THEN 1 ELSE 0 END)) successful_refunds_count,
        sum(CASE WHEN status='succeeded' THEN amount_vnd ELSE 0 END) successful_refund_amount_vnd FROM {{ pinned('refunds') }})
        SELECT 'current' summary_key,o.*,s.*,r.*,f.*,c.observed_at operations_observed_at FROM o CROSS JOIN s CROSS JOIN r CROSS JOIN f CROSS JOIN {{ clock() }} c
) b
