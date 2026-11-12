SELECT
  CAST(b."product_key" AS bigint) AS "product_key",
  CAST(b."product_id" AS bigint) AS "product_id",
  CAST(b."product_name" AS varchar) AS "product_name",
  CAST(b."slug" AS varchar) AS "slug",
  CAST(b."category_id" AS bigint) AS "category_id",
  CAST(b."category_code" AS varchar) AS "category_code",
  CAST(b."category_name" AS varchar) AS "category_name",
  CAST(b."base_price_vnd" AS bigint) AS "base_price_vnd",
  CAST(b."is_active" AS boolean) AS "is_active",
  CAST(b."created_at" AS timestamp(6) with time zone) AS "created_at",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT p.product_id product_key,p.product_id,p.name product_name,p.slug,p.category_id,
        c.code category_code,coalesce(c.name,'Unknown') category_name,CAST(0 AS BIGINT) base_price_vnd,
        p.is_active,p.created_at FROM {{ pinned('products') }} p LEFT JOIN {{ pinned('categories') }} c ON p.category_id=c.category_id
) b
