"""Independent aggregate parity for the admin singleton and order-mart totals."""

from lakehouse.oltp.quality.gate import QualityCheckResult


def admin_consistency_checks(spark):
    expected = spark.sql("""WITH p AS (
        SELECT count(*) product_rows,sum(CAST(is_active AS BIGINT)) active_products FROM lakehouse.silver.silver_products
    ), v AS (
        SELECT count(*) variant_rows FROM lakehouse.silver.silver_product_variants
    ), active AS (
        SELECT v.variant_id FROM lakehouse.silver.silver_product_variants v
        JOIN lakehouse.silver.silver_products p ON v.product_id=p.product_id WHERE v.is_active AND p.is_active
    ), i AS (
        SELECT count(*) inventory_rows FROM lakehouse.gold.fact_inventory_current WHERE location_type='warehouse'
    ), low AS (
        SELECT count(*) low_stock_variants FROM lakehouse.gold.fact_inventory_current i
        JOIN active a ON i.variant_key=a.variant_id WHERE i.location_type='warehouse' AND i.is_low_stock
    ), c AS (
        SELECT count(*) customer_rows,sum(CASE WHEN role='customer' THEN 1 ELSE 0 END) customers
        FROM lakehouse.silver.silver_customers
    ), r AS (SELECT count(*) review_rows FROM lakehouse.silver.silver_product_reviews),
    q AS (SELECT count(*) coupon_rows FROM lakehouse.silver.silver_coupons)
    SELECT product_rows,variant_rows,inventory_rows,customer_rows,review_rows,coupon_rows,active_products,
        CASE WHEN product_rows>0 AND variant_rows>0 THEN (SELECT count(*) FROM active) END active_variants,
        CASE WHEN product_rows>0 AND variant_rows>0 AND inventory_rows>0 THEN low_stock_variants END low_stock_variants,
        customers,CASE WHEN review_rows>0 THEN review_rows END total_reviews
    FROM p CROSS JOIN v CROSS JOIN i CROSS JOIN low CROSS JOIN c CROSS JOIN r CROSS JOIN q""")
    actual = spark.table("lakehouse.gold.mart_admin_overview").select(*expected.columns)
    mismatch = expected.exceptAll(actual).count() + actual.exceptAll(expected).count()
    result = [
        QualityCheckResult(
            "admin_domain_parity",
            "mart_admin_overview",
            str(mismatch),
            "0",
            mismatch == 0,
            "Independent domain aggregates, active catalog and warehouse coverage",
            failed_rows=mismatch,
        )
    ]
    coupons = spark.sql("""SELECT m.summary_key,m.admin_observed_at,
        sum(CASE WHEN c.coupon_id IS NULL THEN NULL WHEN c.is_active AND c.archived_at IS NULL
        AND c.starts_at<=m.admin_observed_at AND c.ends_at>m.admin_observed_at THEN 1 ELSE 0 END) active_coupons
        FROM lakehouse.gold.mart_admin_overview m LEFT JOIN lakehouse.silver.silver_coupons c ON true
        GROUP BY m.summary_key,m.admin_observed_at""")
    target = spark.table("lakehouse.gold.mart_admin_overview")
    coupon_target = target.select(*coupons.columns)
    bad = coupons.exceptAll(coupon_target).count() + coupon_target.exceptAll(coupons).count()
    bad += target.filter("summary_key<>'current' OR summary_key IS NULL OR admin_observed_at IS NULL").count()
    result.append(
        QualityCheckResult(
            "admin_observation_parity",
            "mart_admin_overview",
            str(bad),
            "0",
            bad == 0,
            "Coupon availability at a valid stored observation time",
            failed_rows=bad,
        )
    )
    states = [
        "pending_payment",
        "paid",
        "confirmed",
        "shipping",
        "delivered",
        "completed",
        "cancelled",
        "failed_delivery",
        "returned",
    ]
    sums = ",".join(f"sum(CASE WHEN status='{state}' THEN 1 ELSE 0 END) {state}_orders" for state in states)
    orders = spark.sql(f"SELECT count(*) order_rows,{sums} FROM lakehouse.gold.fact_order")
    order_target = target.select(*orders.columns)
    bad = orders.exceptAll(order_target).count() + order_target.exceptAll(orders).count()
    result.append(
        QualityCheckResult(
            "admin_order_status_parity",
            "mart_admin_overview",
            str(bad),
            "0",
            bad == 0,
            "Every order status and total match independent source aggregates",
            failed_rows=bad,
        )
    )
    return result
