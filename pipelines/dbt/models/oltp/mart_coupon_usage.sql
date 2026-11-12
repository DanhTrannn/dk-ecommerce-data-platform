SELECT
  CAST(b."coupon_id" AS bigint) AS "coupon_id",
  CAST(b."coupon_code" AS varchar) AS "coupon_code",
  CAST(b."used_count" AS integer) AS "used_count",
  CAST(b."usage_limit" AS integer) AS "usage_limit",
  CAST(b."is_available" AS boolean) AS "is_available",
  CAST(b."recent_redemptions_count" AS bigint) AS "recent_redemptions_count",
  CAST(b."marketing_observed_at" AS timestamp(6) with time zone) AS "marketing_observed_at",
  CAST('{{ var("run_id", "unconfigured") }}' AS varchar) AS "_source_run_id"
FROM (
WITH recent AS (SELECT coupon_id,count(coupon_redemption_id) recent_redemptions_count
        FROM {{ pinned('coupon_redemptions') }} CROSS JOIN {{ clock() }} clock WHERE status='redeemed'
        AND redeemed_at>=observed_at-INTERVAL '15' MINUTE AND redeemed_at<=observed_at GROUP BY coupon_id)
        SELECT q.coupon_id,q.code_normalized coupon_code,q.used_count,q.total_usage_limit usage_limit,
        (q.is_active AND q.archived_at IS NULL AND q.starts_at<=c.observed_at AND q.ends_at>=c.observed_at) is_available,
        coalesce(r.recent_redemptions_count,0) recent_redemptions_count,c.observed_at marketing_observed_at
        FROM {{ pinned('coupons') }} q LEFT JOIN recent r ON q.coupon_id=r.coupon_id CROSS JOIN {{ clock() }} c
) b
