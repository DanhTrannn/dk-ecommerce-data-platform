SELECT
  CAST(b."product_id" AS bigint) AS "product_id",
  CAST(b."product_name" AS varchar) AS "product_name",
  CAST(b."review_count" AS bigint) AS "review_count",
  CAST(b."recent_negative_count" AS bigint) AS "recent_negative_count",
  CAST(b."marketing_observed_at" AS timestamp(6) with time zone) AS "marketing_observed_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT r.product_id,coalesce(p.product_name,'Unknown') product_name,
        count(r.review_id) review_count,sum((CASE WHEN r.status='approved' AND r.rating<=2
        AND r.created_at>=c.observed_at-INTERVAL '45' MINUTE AND r.created_at<=c.observed_at THEN 1 ELSE 0 END)) recent_negative_count,
        c.observed_at marketing_observed_at FROM {{ pinned('product_reviews') }} r LEFT JOIN {{ ref('dim_product') }} p ON r.product_id=p.product_id
        CROSS JOIN {{ clock() }} c GROUP BY r.product_id,coalesce(p.product_name,'Unknown'),c.observed_at
) b
