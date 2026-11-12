SELECT
  CAST(CAST(date_format(CAST({{ business_date() }} AS timestamp), '%Y%m%d') AS integer) AS integer) AS "snapshot_date_key",
  CAST({{ business_date() }} AS date) AS "snapshot_date",
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
WITH balances AS (
        SELECT 'warehouse' location_type,CAST(0 AS BIGINT) location_id,variant_id,on_hand on_hand_quantity,0 reserved_quantity FROM {{ pinned('inventory') }}
        UNION ALL SELECT 'store',store_id,variant_id,on_hand,0 FROM {{ pinned('store_inventory') }})
        SELECT b.*,b.variant_id variant_key,v.product_id product_key,coalesce(v.cost_price_vnd,0) unit_cost_vnd,
        b.on_hand_quantity*coalesce(v.cost_price_vnd,0) inventory_value_cost_vnd,
        b.on_hand_quantity<=5 is_low_stock,b.on_hand_quantity=0 is_out_of_stock
        FROM balances b LEFT JOIN {{ pinned('product_variants') }} v ON b.variant_id=v.variant_id
) b
UNION ALL
SELECT "snapshot_date_key", "snapshot_date", "location_type", "location_id", "variant_key", "product_key", "on_hand_quantity", "reserved_quantity", "unit_cost_vnd", "inventory_value_cost_vnd", "is_low_stock", "is_out_of_stock", "_gold_ingested_at", "_source_run_id" FROM {{ this }} WHERE snapshot_date <> {{ business_date() }}
