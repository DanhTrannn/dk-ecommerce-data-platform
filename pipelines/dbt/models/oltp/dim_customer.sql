SELECT
  CAST(b."customer_key" AS bigint) AS "customer_key",
  CAST(b."customer_id" AS bigint) AS "customer_id",
  CAST(b."email" AS varchar) AS "email",
  CAST(b."phone" AS varchar) AS "phone",
  CAST(b."full_name" AS varchar) AS "full_name",
  CAST(b."role" AS varchar) AS "role",
  CAST(b."status" AS varchar) AS "status",
  CAST(b."city_id" AS bigint) AS "city_id",
  CAST(b."city_name" AS varchar) AS "city_name",
  CAST(b."store_id" AS bigint) AS "store_id",
  CAST(b."is_cod_blocked" AS boolean) AS "is_cod_blocked",
  CAST(b."boom_count" AS integer) AS "boom_count",
  CAST(b."created_at" AS timestamp(6) with time zone) AS "created_at",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT c.customer_id customer_key,c.customer_id,c.email_pseudonymized email,c.phone_pseudonymized phone,
        c.display_name_pseudonymized full_name,c.role,c.status,c.city_id,coalesce(i.name,'Unknown') city_name,c.store_id,
        coalesce(c.is_cod_blocked,false) is_cod_blocked,coalesce(c.boom_count,0) boom_count,c.created_at
        FROM {{ pinned('customers') }} c LEFT JOIN {{ pinned('cities') }} i ON c.city_id=i.city_id
) b
