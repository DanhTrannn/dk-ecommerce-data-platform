SELECT
  CAST(b."return_item_id" AS bigint) AS "return_item_id",
  CAST(b."return_id" AS bigint) AS "return_id",
  CAST(b."return_number" AS varchar) AS "return_number",
  CAST(b."order_id" AS bigint) AS "order_id",
  CAST(b."order_item_id" AS bigint) AS "order_item_id",
  CAST(CAST(date_format(CAST(b.return_date AS timestamp),'%Y%m%d') AS integer) AS integer) AS "return_date_key",
  CAST(b."return_date" AS date) AS "return_date",
  CAST(b."action_type" AS varchar) AS "action_type",
  CAST(b."return_status" AS varchar) AS "return_status",
  CAST(b."inspection_status" AS varchar) AS "inspection_status",
  CAST(b."reason" AS varchar) AS "reason",
  CAST(b."returned_variant_key" AS bigint) AS "returned_variant_key",
  CAST(b."exchanged_variant_key" AS bigint) AS "exchanged_variant_key",
  CAST(b."returned_quantity" AS integer) AS "returned_quantity",
  CAST(b."refund_amount_vnd" AS bigint) AS "refund_amount_vnd",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT i.return_item_id,r.return_id,r.return_code return_number,r.order_id,i.order_item_id,
        CAST(r.created_at AS DATE) return_date,r.action_type,r.status return_status,i.inspection_status,r.customer_reason reason,
        i.variant_id returned_variant_key,i.exchange_variant_id exchanged_variant_key,i.quantity returned_quantity,
        coalesce(i.refund_amount_vnd,0) refund_amount_vnd FROM {{ pinned('return_items') }} i JOIN {{ pinned('return_requests') }} r ON i.return_id=r.return_id
) b
