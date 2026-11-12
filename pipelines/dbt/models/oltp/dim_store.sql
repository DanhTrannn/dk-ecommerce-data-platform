SELECT
  CAST(b."store_key" AS bigint) AS "store_key",
  CAST(b."store_id" AS bigint) AS "store_id",
  CAST(b."store_code" AS varchar) AS "store_code",
  CAST(b."store_name" AS varchar) AS "store_name",
  CAST(b."city_id" AS bigint) AS "city_id",
  CAST(b."city_name" AS varchar) AS "city_name",
  CAST(b."address" AS varchar) AS "address",
  CAST(b."phone" AS varchar) AS "phone",
  CAST(b."is_active" AS boolean) AS "is_active",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT s.store_id store_key,s.store_id,s.code store_code,s.name store_name,s.city_id,
        coalesce(c.name,'Unknown') city_name,s.address,s.phone,s.is_active FROM {{ pinned('stores') }} s LEFT JOIN {{ pinned('cities') }} c ON s.city_id=c.city_id
) b
