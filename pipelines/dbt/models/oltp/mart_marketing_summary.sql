SELECT
  CAST(b."summary_key" AS varchar) AS "summary_key",
  CAST(b."coupon_rows" AS bigint) AS "coupon_rows",
  CAST(b."review_rows" AS bigint) AS "review_rows",
  CAST(b."marketing_observed_at" AS timestamp(6) with time zone) AS "marketing_observed_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
SELECT 'current' summary_key,q.coupon_rows,r.review_rows,c.observed_at marketing_observed_at
        FROM (SELECT count(coupon_id) coupon_rows FROM {{ pinned('coupons') }}) q CROSS JOIN
        (SELECT count(review_id) review_rows FROM {{ pinned('product_reviews') }}) r CROSS JOIN {{ clock() }} c
) b
