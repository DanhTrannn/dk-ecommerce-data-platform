SELECT
  CAST(b."variant_key" AS bigint) AS "variant_key",
  CAST(b."variant_id" AS bigint) AS "variant_id",
  CAST(b."product_id" AS bigint) AS "product_id",
  CAST(b."product_name" AS varchar) AS "product_name",
  CAST(b."sku" AS varchar) AS "sku",
  CAST(b."color" AS varchar) AS "color",
  CAST(b."size" AS varchar) AS "size",
  CAST(b."price_vnd" AS bigint) AS "price_vnd",
  CAST(b."cost_price_vnd" AS bigint) AS "cost_price_vnd",
  CAST(round(b.margin_pct,2) AS double) AS "margin_pct",
  CAST(b."is_active" AS boolean) AS "is_active",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT v.variant_id variant_key,v.variant_id,v.product_id,coalesce(p.name,'Unknown') product_name,
        v.sku,v.color_code color,v.size_code size,v.price_vnd,coalesce(v.cost_price_vnd,0) cost_price_vnd,
        CASE WHEN v.price_vnd>0 THEN ((v.price_vnd-coalesce(v.cost_price_vnd,0))/CAST(v.price_vnd AS double))*100e0 ELSE 0.0 END margin_pct,
        v.is_active FROM {{ pinned('product_variants') }} v LEFT JOIN {{ pinned('products') }} p ON v.product_id=p.product_id
) b
