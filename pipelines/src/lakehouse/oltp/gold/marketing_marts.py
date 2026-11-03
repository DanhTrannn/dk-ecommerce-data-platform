"""Marketing observations with explicit windows and independent source grains."""

from pyspark.sql import DataFrame, functions as F


def build_mart_marketing_summary(coupons: DataFrame, reviews: DataFrame, run_id: str, *, as_of) -> DataFrame:
    return (
        coupons.agg(F.count("coupon_id").alias("coupon_rows"))
        .crossJoin(reviews.agg(F.count("review_id").alias("review_rows")))
        .withColumn("summary_key", F.lit("current"))
        .withColumn("marketing_observed_at", F.lit(as_of).cast("timestamp"))
        .withColumn("_source_run_id", F.lit(run_id))
    )


def build_mart_coupon_usage(coupons: DataFrame, redemptions: DataFrame, run_id: str, *, as_of) -> DataFrame:
    observed = F.lit(as_of).cast("timestamp")
    recent = (
        redemptions.filter(
            (F.col("status") == "redeemed")
            & (F.col("redeemed_at") >= observed - F.expr("INTERVAL 15 MINUTES"))
            & (F.col("redeemed_at") <= observed)
        )
        .groupBy("coupon_id")
        .agg(F.count("coupon_redemption_id").alias("recent_redemptions_count"))
    )
    return (
        coupons.join(recent, "coupon_id", "left")
        .select(
            "coupon_id",
            F.col("code_normalized").alias("coupon_code"),
            "used_count",
            F.col("total_usage_limit").alias("usage_limit"),
            (
                F.col("is_active")
                & F.col("archived_at").isNull()
                & (F.col("starts_at") <= observed)
                & (F.col("ends_at") >= observed)
            ).alias("is_available"),
            F.coalesce("recent_redemptions_count", F.lit(0)).alias("recent_redemptions_count"),
        )
        .withColumn("marketing_observed_at", observed)
        .withColumn("_source_run_id", F.lit(run_id))
    )


def build_mart_product_review_summary(reviews: DataFrame, products: DataFrame, run_id: str, *, as_of) -> DataFrame:
    observed = F.lit(as_of).cast("timestamp")
    recent_negative = (
        (F.col("status") == "approved")
        & (F.col("rating") <= 2)
        & (F.col("created_at") >= observed - F.expr("INTERVAL 45 MINUTES"))
        & (F.col("created_at") <= observed)
    )
    return (
        reviews.groupBy("product_id")
        .agg(
            F.count("review_id").alias("review_count"),
            F.sum(recent_negative.cast("long")).alias("recent_negative_count"),
        )
        .join(products.select("product_id", "product_name"), "product_id", "left")
        .withColumn("product_name", F.coalesce("product_name", F.lit("Unknown")))
        .withColumn("marketing_observed_at", observed)
        .withColumn("_source_run_id", F.lit(run_id))
    )
