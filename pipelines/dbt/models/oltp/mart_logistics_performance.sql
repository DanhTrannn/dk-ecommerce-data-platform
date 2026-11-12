SELECT
  CAST(b."shipment_date" AS date) AS "shipment_date",
  CAST(b."staff_key" AS bigint) AS "staff_key",
  CAST(b."staff_code" AS varchar) AS "staff_code",
  CAST(b."assigned_city_id" AS bigint) AS "assigned_city_id",
  CAST(b."assigned_city_name" AS varchar) AS "assigned_city_name",
  CAST(b."total_shipments" AS bigint) AS "total_shipments",
  CAST(b."delivered_count" AS bigint) AS "delivered_count",
  CAST(b."boom_count" AS bigint) AS "boom_count",
  CAST(round(CASE WHEN b.total_shipments>0 THEN b.boom_count*100e0/b.total_shipments ELSE 0.0 END,2) AS double) AS "boom_rate_pct",
  CAST(b."on_time_count" AS bigint) AS "on_time_count",
  CAST(b."on_time_eligible_count" AS bigint) AS "on_time_eligible_count",
  CAST(b."problematic_shipments" AS bigint) AS "problematic_shipments",
  CAST(b."duration_sample_count" AS bigint) AS "duration_sample_count",
  CAST(b."duration_minutes_sum" AS double) AS "duration_minutes_sum",
  CAST(round(CASE WHEN b.on_time_eligible_count>0 THEN b.on_time_count*100e0/b.on_time_eligible_count END,2) AS double) AS "on_time_rate_pct",
  CAST(round(b.avg_duration_minutes,2) AS double) AS "avg_duration_minutes",
  CAST(b."total_cod_collected_vnd" AS bigint) AS "total_cod_collected_vnd",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH labeled AS (SELECT s.*,coalesce(d.staff_code,'Unknown') staff_code,
        coalesce(d.assigned_city_id,0) assigned_city_id,coalesce(d.assigned_city_name,'Unknown') assigned_city_name
        FROM {{ ref('fact_shipment') }} s LEFT JOIN {{ ref('dim_delivery_staff') }} d ON s.staff_key=d.staff_key)
        SELECT shipment_date,staff_key,staff_code,assigned_city_id,assigned_city_name,count(*) total_shipments,
        sum(CASE WHEN status='delivered' THEN 1 ELSE 0 END) delivered_count,sum(CASE WHEN is_boom THEN 1 ELSE 0 END) boom_count,
        sum(CASE WHEN status='delivered' AND is_on_time IS NOT NULL THEN 1 ELSE 0 END) on_time_eligible_count,
        sum(CASE WHEN status='delivered' AND is_on_time THEN 1 ELSE 0 END) on_time_count,
        sum(CASE WHEN attempt_count>1 OR status='failed' THEN 1 ELSE 0 END) problematic_shipments,
        count(delivery_duration_minutes) duration_sample_count,sum(delivery_duration_minutes) duration_minutes_sum,
        avg(delivery_duration_minutes) avg_duration_minutes,sum(coalesce(cod_amount_collected_vnd,0)) total_cod_collected_vnd
        FROM labeled GROUP BY shipment_date,staff_key,staff_code,assigned_city_id,assigned_city_name
) b
