SELECT
  CAST(b."location_type" AS varchar) AS "location_type",
  CAST(b."location_id" AS bigint) AS "location_id",
  CAST(b."variant_id" AS bigint) AS "variant_id",
  CAST(b."product_key" AS bigint) AS "product_key",
  CAST(b."sku" AS varchar) AS "sku",
  CAST(b."product_name" AS varchar) AS "product_name",
  CAST(b."category_id" AS bigint) AS "category_id",
  CAST(b."category_name" AS varchar) AS "category_name",
  CAST(b."snapshot_date" AS date) AS "snapshot_date",
  CAST(b."on_hand" AS bigint) AS "on_hand",
  CAST(b."reserved_quantity" AS bigint) AS "reserved_quantity",
  CAST(b."unit_cost_vnd" AS bigint) AS "unit_cost_vnd",
  CAST(b."inventory_value_cost_vnd" AS bigint) AS "inventory_value_cost_vnd",
  CAST(b."is_low_stock" AS boolean) AS "is_low_stock",
  CAST(b."is_out_of_stock" AS boolean) AS "is_out_of_stock",
  CAST(b."observed_at" AS timestamp(6) with time zone) AS "observed_at",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT i.location_type,i.location_id,i.variant_key variant_id,i.product_key,
        v.sku,v.product_name,p.category_id,p.category_name,i.snapshot_date,i.on_hand_quantity on_hand,
        i.reserved_quantity,i.unit_cost_vnd,i.inventory_value_cost_vnd,i.is_low_stock,i.is_out_of_stock,c.observed_at
        FROM {{ ref('fact_inventory_current') }} i LEFT JOIN {{ ref('dim_variant') }} v ON i.variant_key=v.variant_key
        LEFT JOIN {{ ref('dim_product') }} p ON i.product_key=p.product_key CROSS JOIN {{ clock() }} c
) b
