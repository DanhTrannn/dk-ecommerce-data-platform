SELECT
  CAST(b."return_date" AS date) AS "return_date",
  CAST(b."returned_variant_key" AS bigint) AS "returned_variant_key",
  CAST(b."product_id" AS bigint) AS "product_id",
  CAST(b."product_name" AS varchar) AS "product_name",
  CAST(b."sku" AS varchar) AS "sku",
  CAST(b."size" AS varchar) AS "size",
  CAST(b."color" AS varchar) AS "color",
  CAST(b."action_type" AS varchar) AS "action_type",
  CAST(b."reason" AS varchar) AS "reason",
  CAST(b."total_return_requests" AS bigint) AS "total_return_requests",
  CAST(b."return_item_count" AS bigint) AS "return_item_count",
  CAST(b."total_returned_quantity" AS bigint) AS "total_returned_quantity",
  CAST(b."accepted_return_quantity" AS bigint) AS "accepted_return_quantity",
  CAST(b."requested_refund_amount_vnd" AS bigint) AS "requested_refund_amount_vnd",
  CAST(b."total_refund_amount_vnd" AS bigint) AS "total_refund_amount_vnd",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT f.return_date,f.returned_variant_key,coalesce(v.product_id,0) product_id,
        coalesce(v.product_name,'Unknown') product_name,coalesce(v.sku,'Unknown') sku,coalesce(v.size,'Unknown') size,
        coalesce(v.color,'Unknown') color,f.action_type,coalesce(f.reason,'Unknown') reason,
        count(DISTINCT f.return_id) total_return_requests,count(f.return_item_id) return_item_count,
        sum(f.returned_quantity) total_returned_quantity,
        sum(CASE WHEN f.return_status='completed' AND f.inspection_status='passed' THEN f.returned_quantity ELSE 0 END) accepted_return_quantity,
        sum(f.refund_amount_vnd) requested_refund_amount_vnd,sum(f.refund_amount_vnd) total_refund_amount_vnd
        FROM {{ ref('fact_return_exchange') }} f LEFT JOIN {{ ref('dim_variant') }} v ON f.returned_variant_key=v.variant_key
        WHERE f.return_status<>'cancelled' GROUP BY f.return_date,f.returned_variant_key,coalesce(v.product_id,0),coalesce(v.product_name,'Unknown'),coalesce(v.sku,'Unknown'),coalesce(v.size,'Unknown'),coalesce(v.color,'Unknown'),f.action_type,coalesce(f.reason,'Unknown')
) b
