"""Independent domain counts for the published admin overview."""

from pyspark.sql import functions as F


def build_mart_admin_overview(products, variants, inventory, customers, reviews, coupons, orders, run_id, *, as_of):
    observed = F.lit(as_of).cast("timestamp")
    active_products = products.filter("is_active").select("product_id")
    active_variants = variants.filter("is_active").join(active_products, "product_id").select("variant_id")
    warehouse = inventory.filter("location_type='warehouse'")
    low = warehouse.filter("is_low_stock").join(
        active_variants, warehouse.variant_key == active_variants.variant_id, "inner"
    )
    # Collapse each domain before crossing it with another; products, reviews
    # and orders must never multiply each other's counts.
    domains = [
        products.agg(
            F.count("*").alias("product_rows"), F.sum(F.col("is_active").cast("long")).alias("active_products")
        ),
        variants.agg(F.count("*").alias("variant_rows")).crossJoin(
            active_variants.agg(F.count("*").alias("active_variants"))
        ),
        warehouse.agg(F.count("*").alias("inventory_rows")).crossJoin(
            low.agg(F.count("*").alias("low_stock_variants"))
        ),
        customers.agg(
            F.count("*").alias("customer_rows"), F.sum((F.col("role") == "customer").cast("long")).alias("customers")
        ),
        reviews.agg(F.count("*").alias("review_rows")),
        coupons.agg(
            F.count("*").alias("coupon_rows"),
            F.sum(
                (
                    F.col("is_active")
                    & F.col("archived_at").isNull()
                    & (F.col("starts_at") <= observed)
                    & (F.col("ends_at") > observed)
                ).cast("long")
            ).alias("active_coupons"),
        ),
        orders.agg(
            F.count("*").alias("order_rows"),
            *[
                F.sum((F.col("status") == status).cast("long")).alias(f"{status}_orders")
                for status in [
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
            ],
        ),
    ]
    summary = domains[0]
    for frame in domains[1:]:
        summary = summary.crossJoin(frame)
    return (
        summary.withColumn(
            "active_variants",
            F.when((F.col("product_rows") > 0) & (F.col("variant_rows") > 0), F.col("active_variants")),
        )
        .withColumn(
            "low_stock_variants",
            F.when(
                (F.col("inventory_rows") > 0) & (F.col("product_rows") > 0) & (F.col("variant_rows") > 0),
                F.col("low_stock_variants"),
            ),
        )
        .withColumn("total_reviews", F.when(F.col("review_rows") > 0, F.col("review_rows")))
        .withColumn("summary_key", F.lit("current"))
        .withColumn("admin_observed_at", observed)
        .withColumn("_source_run_id", F.lit(run_id))
    )
