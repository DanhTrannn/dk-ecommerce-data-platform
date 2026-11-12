SELECT
  CAST(b."summary_key" AS varchar) AS "summary_key",
  CAST(b."inventory_rows" AS bigint) AS "inventory_rows",
  CAST(b."total_inventory_value_vnd" AS bigint) AS "total_inventory_value_vnd",
  CAST(b."warehouse_stock_units" AS bigint) AS "warehouse_stock_units",
  CAST(b."store_stock_units" AS bigint) AS "store_stock_units",
  CAST(b."stockout_count" AS bigint) AS "stockout_count",
  CAST(b."inventory_observed_at" AS timestamp(6) with time zone) AS "inventory_observed_at",
  CAST(b."inbound_batches_count" AS bigint) AS "inbound_batches_count",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH i AS (SELECT count(variant_id) inventory_rows,
        sum(inventory_value_cost_vnd) total_inventory_value_vnd,
        sum(CASE WHEN location_type='warehouse' THEN on_hand END) warehouse_stock_units,
        sum(CASE WHEN location_type='store' THEN on_hand END) store_stock_units,
        CASE WHEN count(*)>0 THEN count(DISTINCT CASE WHEN is_out_of_stock THEN variant_id END) END stockout_count,
        max(observed_at) inventory_observed_at FROM {{ ref('mart_inventory_current') }})
        SELECT 'current' summary_key,i.*,r.inbound_batches_count FROM i CROSS JOIN
        (SELECT count(receipt_id) inbound_batches_count FROM {{ pinned('inbound_receipts') }}) r
) b
