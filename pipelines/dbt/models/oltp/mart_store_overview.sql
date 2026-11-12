SELECT
  CAST(b."store_key" AS bigint) AS "store_key",
  CAST(b."store_id" AS bigint) AS "store_id",
  CAST(b."store_code" AS varchar) AS "store_code",
  CAST(b."store_name" AS varchar) AS "store_name",
  CAST(b."is_active" AS boolean) AS "is_active",
  CAST(b."total_orders" AS bigint) AS "total_orders",
  CAST(b."completed_orders" AS bigint) AS "completed_orders",
  CAST(b."total_revenue_vnd" AS bigint) AS "total_revenue_vnd",
  CAST(b."active_staff" AS bigint) AS "active_staff",
  CAST(b."low_stock_count" AS bigint) AS "low_stock_count",
  CAST(b."stockout_count" AS bigint) AS "stockout_count",
  CAST(b."inventory_observed_at" AS timestamp(6) with time zone) AS "inventory_observed_at",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH sales AS (
        SELECT store_key,count(order_id) total_orders,sum((CASE WHEN status='completed' THEN 1 ELSE 0 END)) completed_orders,
        sum(net_revenue_vnd) total_revenue_vnd FROM {{ ref('fact_order') }} GROUP BY store_key),
        staff AS (SELECT store_id store_key,count(customer_id) active_staff FROM {{ pinned('customers') }}
        WHERE role='store_manager' AND status='active' GROUP BY store_id),
        stock AS (SELECT location_id store_key,sum((CASE WHEN is_low_stock THEN 1 ELSE 0 END)) low_stock_count,
        sum((CASE WHEN is_out_of_stock THEN 1 ELSE 0 END)) stockout_count,max(observed_at) inventory_observed_at
        FROM {{ ref('mart_inventory_current') }} WHERE location_type='store' GROUP BY location_id)
        SELECT d.store_key,d.store_id,d.store_code,d.store_name,d.is_active,coalesce(s.total_orders,0) total_orders,
        coalesce(s.completed_orders,0) completed_orders,coalesce(s.total_revenue_vnd,0) total_revenue_vnd,
        coalesce(t.active_staff,0) active_staff,i.low_stock_count,i.stockout_count,i.inventory_observed_at
        FROM {{ ref('dim_store') }} d LEFT JOIN sales s ON d.store_key=s.store_key LEFT JOIN staff t ON d.store_key=t.store_key
        LEFT JOIN stock i ON d.store_key=i.store_key
) b
