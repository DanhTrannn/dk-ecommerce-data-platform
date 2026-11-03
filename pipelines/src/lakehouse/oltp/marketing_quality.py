"""Marketing serving checks at the stored observation timestamp."""

from lakehouse.oltp.quality import QualityCheckResult


def marketing_consistency_checks(spark):
    row = spark.sql("""SELECT count(*) n,
        count_if(summary_key='current' AND marketing_observed_at IS NOT NULL
        AND coupon_rows=(SELECT count(*) FROM lakehouse.silver.silver_coupons)
        AND review_rows=(SELECT count(*) FROM lakehouse.silver.silver_product_reviews)) valid
        FROM lakehouse.gold.mart_marketing_summary""").first()
    results = [
        QualityCheckResult(
            "marketing_summary_parity",
            "mart_marketing_summary",
            str(tuple(row)),
            "(1, 1)",
            row.n == 1 and row.valid == 1,
            "Independent source coverage and observation time",
        )
    ]
    coupons = spark.sql("""WITH recent AS (
        SELECT coupon_id,count(*) n FROM lakehouse.silver.silver_coupon_redemptions
        CROSS JOIN lakehouse.gold.mart_marketing_summary m
        WHERE status='redeemed' AND redeemed_at BETWEEN marketing_observed_at-INTERVAL 15 MINUTES AND marketing_observed_at
        GROUP BY coupon_id)
        SELECT c.coupon_id,c.code_normalized coupon_code,c.used_count,c.total_usage_limit usage_limit,
        (c.is_active AND c.archived_at IS NULL AND c.starts_at<=m.marketing_observed_at
        AND c.ends_at>=m.marketing_observed_at) is_available,coalesce(r.n,0) recent_redemptions_count,m.marketing_observed_at
        FROM lakehouse.silver.silver_coupons c CROSS JOIN lakehouse.gold.mart_marketing_summary m
        LEFT JOIN recent r ON c.coupon_id=r.coupon_id""")
    reviews = spark.sql("""SELECT r.product_id,coalesce(p.product_name,'Unknown') product_name,
        count(*) review_count,sum(CASE WHEN r.status='approved' AND r.rating<=2
        AND r.created_at BETWEEN m.marketing_observed_at-INTERVAL 45 MINUTES AND m.marketing_observed_at
        THEN 1 ELSE 0 END) recent_negative_count,m.marketing_observed_at
        FROM lakehouse.silver.silver_product_reviews r CROSS JOIN lakehouse.gold.mart_marketing_summary m
        LEFT JOIN lakehouse.gold.dim_product p ON r.product_id=p.product_id
        GROUP BY r.product_id,p.product_name,m.marketing_observed_at""")
    for table, expected in [("mart_coupon_usage", coupons), ("mart_product_review_summary", reviews)]:
        actual = spark.table(f"lakehouse.gold.{table}").select(*expected.columns)
        mismatches = expected.exceptAll(actual).count() + actual.exceptAll(expected).count()
        results.append(
            QualityCheckResult(
                "marketing_grain_parity",
                table,
                str(mismatches),
                "0",
                mismatches == 0,
                "Exact grain/window parity including null limits, releases and moderation",
                failed_rows=mismatches,
            )
        )
    return results
