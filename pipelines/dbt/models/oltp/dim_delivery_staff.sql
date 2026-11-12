SELECT
  CAST(b."staff_key" AS bigint) AS "staff_key",
  CAST(b."staff_id" AS bigint) AS "staff_id",
  CAST(b."staff_code" AS varchar) AS "staff_code",
  CAST(b."full_name" AS varchar) AS "full_name",
  CAST(b."phone" AS varchar) AS "phone",
  CAST(b."assigned_city_id" AS bigint) AS "assigned_city_id",
  CAST(b."assigned_city_name" AS varchar) AS "assigned_city_name",
  CAST(b."vehicle_type" AS varchar) AS "vehicle_type",
  CAST(b."is_active" AS boolean) AS "is_active",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT staff_id staff_key,staff_id,concat('DK-SHIP-',CAST(staff_id AS VARCHAR)) staff_code,
        full_name_pseudonymized full_name,phone_pseudonymized phone,CAST(NULL AS BIGINT) assigned_city_id,
        'Toàn quốc' assigned_city_name,vehicle_plate vehicle_type,is_active FROM {{ pinned('delivery_staff') }}
) b
