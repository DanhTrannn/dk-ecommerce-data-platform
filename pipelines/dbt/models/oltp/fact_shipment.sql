SELECT
  CAST(b."shipment_id" AS bigint) AS "shipment_id",
  CAST(b."shipment_code" AS varchar) AS "shipment_code",
  CAST(b."order_id" AS bigint) AS "order_id",
  CAST(b."staff_key" AS bigint) AS "staff_key",
  CAST(CAST(date_format(CAST(b.shipment_date AS timestamp),'%Y%m%d') AS integer) AS integer) AS "shipment_date_key",
  CAST(b."shipment_date" AS date) AS "shipment_date",
  CAST(b."status" AS varchar) AS "status",
  CAST(b."attempt_count" AS integer) AS "attempt_count",
  CAST(b."dispatched_at" AS timestamp(6) with time zone) AS "dispatched_at",
  CAST(b."delivered_at" AS timestamp(6) with time zone) AS "delivered_at",
  CAST(b."failed_at" AS timestamp(6) with time zone) AS "failed_at",
  CAST(b."is_boom" AS boolean) AS "is_boom",
  CAST(b."is_delivered" AS boolean) AS "is_delivered",
  CAST(b."cod_amount_expected_vnd" AS bigint) AS "cod_amount_expected_vnd",
  CAST(b."cod_amount_collected_vnd" AS bigint) AS "cod_amount_collected_vnd",
  CAST(round(CASE WHEN coalesce(b.delivered_at,b.failed_at)>=b.dispatched_at THEN (floor(to_unixtime(coalesce(b.delivered_at,b.failed_at)))-floor(to_unixtime(b.dispatched_at)))/60e0 END,2) AS double) AS "delivery_duration_minutes",
  CAST(CAST(NULL AS boolean) AS boolean) AS "is_on_time",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT shipment_id,shipment_code,order_id,delivery_staff_id staff_key,CAST(created_at AS DATE) shipment_date,
        status,dispatched_at,delivered_at,failed_at,attempt_count,status='failed' is_boom,status='delivered' is_delivered,
        cod_amount_vnd cod_amount_expected_vnd,coalesce(cod_collected_vnd,0) cod_amount_collected_vnd
        FROM {{ pinned('shipments') }}
) b
