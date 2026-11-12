SELECT
  CAST(b."snapshot_date_key" AS integer) AS "snapshot_date_key",
  CAST(b."snapshot_date" AS date) AS "snapshot_date",
  CAST(b."location_type" AS varchar) AS "location_type",
  CAST(b."location_id" AS bigint) AS "location_id",
  CAST(b."variant_key" AS bigint) AS "variant_key",
  CAST(b."product_key" AS bigint) AS "product_key",
  CAST(b."on_hand_quantity" AS integer) AS "on_hand_quantity",
  CAST(b."reserved_quantity" AS integer) AS "reserved_quantity",
  CAST(b."unit_cost_vnd" AS bigint) AS "unit_cost_vnd",
  CAST(b."inventory_value_cost_vnd" AS bigint) AS "inventory_value_cost_vnd",
  CAST(b."is_low_stock" AS boolean) AS "is_low_stock",
  CAST(b."is_out_of_stock" AS boolean) AS "is_out_of_stock",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT * FROM {{ ref('fact_inventory_daily_snapshot') }} WHERE snapshot_date={{ business_date() }}
) b
