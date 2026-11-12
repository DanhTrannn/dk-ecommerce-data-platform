SELECT
  CAST(b."shipment_date" AS date) AS "shipment_date",
  CAST(b."region" AS varchar) AS "region",
  CAST(b."total_cod_shipments" AS bigint) AS "total_cod_shipments",
  CAST(b."failed_cod_shipments" AS bigint) AS "failed_cod_shipments",
  CAST(CAST(current_timestamp AS timestamp(6) with time zone) AS timestamp(6) with time zone) AS "_gold_ingested_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT s.shipment_date,coalesce(o.shipping_region,'Khu vực khác') region,
        count(s.shipment_id) total_cod_shipments,sum((CASE WHEN s.status='failed' THEN 1 ELSE 0 END)) failed_cod_shipments
        FROM {{ ref('fact_shipment') }} s JOIN {{ ref('fact_order') }} o ON s.order_id=o.order_id WHERE o.payment_method='cod'
        GROUP BY s.shipment_date,coalesce(o.shipping_region,'Khu vực khác')
) b
