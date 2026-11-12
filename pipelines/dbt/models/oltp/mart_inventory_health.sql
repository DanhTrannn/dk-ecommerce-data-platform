SELECT
  CAST(b."snapshot_date" AS date) AS "snapshot_date",
  CAST(b."location_type" AS varchar) AS "location_type",
  CAST(b."location_id" AS bigint) AS "location_id",
  CAST(b."category_id" AS bigint) AS "category_id",
  CAST(b."category_name" AS varchar) AS "category_name",
  CAST(b."total_variants" AS bigint) AS "total_variants",
  CAST(b."out_of_stock_count" AS bigint) AS "out_of_stock_count",
  CAST(b."low_stock_count" AS bigint) AS "low_stock_count",
  CAST(b."total_on_hand_units" AS bigint) AS "total_on_hand_units",
  CAST(b."total_inventory_value_vnd" AS bigint) AS "total_inventory_value_vnd",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT i.snapshot_date,i.location_type,i.location_id,coalesce(p.category_id,0) category_id,
        coalesce(p.category_name,'Unknown') category_name,count(DISTINCT i.variant_key) total_variants,
        sum(CASE WHEN i.is_out_of_stock THEN 1 ELSE 0 END) out_of_stock_count,
        sum(CASE WHEN i.is_low_stock THEN 1 ELSE 0 END) low_stock_count,sum(i.on_hand_quantity) total_on_hand_units,
        sum(i.inventory_value_cost_vnd) total_inventory_value_vnd FROM {{ ref('fact_inventory_daily_snapshot') }} i
        LEFT JOIN {{ ref('dim_product') }} p ON i.product_key=p.product_key
        GROUP BY i.snapshot_date,i.location_type,i.location_id,coalesce(p.category_id,0),coalesce(p.category_name,'Unknown')
) b
