"""Canonical merchandise metrics, in VND, grouped by Vietnam order date.

Shipping and cash collection are separate from merchandise revenue. Successful
returns remain recognized sales; their refunds and returned cost are deducted.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def _allocate(df: DataFrame, amount: str, weight: str, output: str) -> DataFrame:
    """Allocate integer VND exactly, with deterministic order-item rounding.

    Differences between cumulative floors conserve the order total, including
    when a discount/refund does not divide evenly between items.
    """
    partition = Window.partitionBy("order_id")
    ordered = partition.orderBy("order_item_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("_weight_total", F.sum(weight).over(partition)).withColumn(
        "_weight_running", F.sum(weight).over(ordered)
    )
    amount_col = F.col(amount).cast("decimal(20,0)")
    total = F.col("_weight_total").cast("decimal(20,0)")
    running = F.col("_weight_running").cast("decimal(20,0)")
    previous = (F.col("_weight_running") - F.col(weight)).cast("decimal(20,0)")
    allocated = F.floor(amount_col * running / total) - F.floor(amount_col * previous / total)
    return df.withColumn(output, F.when(total > 0, allocated).otherwise(0).cast("long")).drop(
        "_weight_total", "_weight_running"
    )


def build_fact_order_item(
    silver_order_items: DataFrame,
    silver_orders: DataFrame,
    run_id: str,
    silver_product_variants: DataFrame | None = None,
    *,
    silver_payments: DataFrame | None = None,
    silver_refunds: DataFrame | None = None,
    silver_return_requests: DataFrame | None = None,
    silver_return_items: DataFrame | None = None,
) -> DataFrame:
    oi, o = silver_order_items.alias("oi"), silver_orders.alias("o")
    base = oi.join(o, F.col("oi.order_id") == F.col("o.order_id"), "inner")
    if silver_product_variants is not None:
        base = base.join(silver_product_variants.alias("pv"), F.col("oi.variant_id") == F.col("pv.variant_id"), "left")
    # Historical cost must come from the order-time snapshot, never today's cost.
    unit_cost = (
        F.col("oi.cost_price_vnd") if "cost_price_vnd" in silver_order_items.columns else F.lit(None).cast("long")
    )
    product = (
        F.col("oi.product_id")
        if "product_id" in silver_order_items.columns
        else (F.col("pv.product_id") if silver_product_variants is not None else F.lit(None).cast("long"))
    )
    discount = "discount_amount_vnd" if "discount_amount_vnd" in silver_orders.columns else "discount_vnd"
    date = F.to_date(F.from_utc_timestamp(F.col("o.created_at"), "Asia/Ho_Chi_Minh"))
    df = base.select(
        "oi.order_item_id",
        "oi.order_id",
        date.alias("order_date"),
        F.date_format(date, "yyyyMMdd").cast("int").alias("order_date_key"),
        F.coalesce(F.col("o.customer_id"), F.lit(-1)).alias("customer_key"),
        product.alias("product_key"),
        F.col("oi.variant_id").alias("variant_key"),
        F.coalesce(F.col("o.store_id"), F.lit(0)).alias("store_key"),
        "o.channel",
        F.col("o.status").alias("order_status"),
        "oi.quantity",
        "oi.unit_price_vnd",
        unit_cost.alias("unit_cost_vnd"),
        (F.col("oi.quantity") * F.col("oi.unit_price_vnd")).cast("long").alias("item_total_vnd"),
        F.coalesce(F.col(f"o.{discount}"), F.lit(0)).alias("_order_discount"),
        "oi.category_code_snapshot",
        "oi.category_name_snapshot",
    )
    order_window = Window.partitionBy("order_id")
    df = df.withColumn("_order_discount", F.least(F.col("_order_discount"), F.sum("item_total_vnd").over(order_window)))
    df = _allocate(df, "_order_discount", "item_total_vnd", "allocated_discount_vnd")
    df = df.withColumn("_merchandise", F.col("item_total_vnd") - F.col("allocated_discount_vnd"))

    if silver_return_requests is not None and silver_return_items is not None:
        returned = (
            silver_return_items.alias("ri")
            .join(silver_return_requests.alias("rr"), "return_id")
            .filter((F.col("rr.status") == "completed") & (F.col("ri.inspection_status") == "passed"))
        )
        returned = returned.groupBy("order_item_id").agg(
            F.sum("ri.quantity").alias("_returned"), F.sum("ri.refund_amount_vnd").alias("_return_value")
        )
        df = df.join(returned, "order_item_id", "left")
    else:
        df = df.withColumn("_returned", F.lit(0))
        df = df.withColumn("_return_value", F.lit(0))
    df = df.withColumn(
        "returned_quantity", F.least(F.col("quantity"), F.coalesce(F.col("_returned"), F.lit(0))).cast("int")
    )

    if silver_payments is not None and silver_refunds is not None:
        refunds = (
            silver_refunds.filter(F.col("status") == "succeeded")
            .alias("r")
            .join(silver_payments.select("payment_id", "order_id"), "payment_id")
            .groupBy("order_id")
            .agg(F.sum("amount_vnd").alias("_refund_total"))
        )
        df = df.join(refunds, "order_id", "left")
    else:
        df = df.withColumn("_refund_total", F.lit(0))
    # Completed return lines guide attribution. Monetary refund amounts are
    # bounded by merchandise value; shipping refunds are not merchandise revenue.
    df = df.withColumn("_return_weight", F.least(F.col("_merchandise"), F.coalesce(F.col("_return_value"), F.lit(0))))
    df = df.withColumn(
        "_refund_weight",
        F.when(F.sum("_return_weight").over(order_window) > 0, F.col("_return_weight")).otherwise(
            F.col("_merchandise")
        ),
    )
    df = df.withColumn(
        "_refund_total",
        F.least(F.coalesce(F.col("_refund_total"), F.lit(0)), F.sum("_merchandise").over(order_window)),
    )
    # Attribute documented returns first, then allocate any other successful
    # merchandise refund over the remaining value. Refund money and physical
    # return quantities are separate facts; neither may silently erase the other.
    df = df.withColumn("_targeted_refund", F.least(F.col("_refund_total"), F.sum("_refund_weight").over(order_window)))
    df = _allocate(df, "_targeted_refund", "_refund_weight", "_targeted_amount")
    df = df.withColumn("_remaining_refund", F.col("_refund_total") - F.col("_targeted_refund"))
    df = df.withColumn("_remaining_value", F.col("_merchandise") - F.col("_targeted_amount"))
    df = _allocate(df, "_remaining_refund", "_remaining_value", "_other_refund")
    df = df.withColumn("refunded_amount_vnd", F.col("_targeted_amount") + F.col("_other_refund"))
    recognized = F.col("order_status").isin("delivered", "completed", "returned")
    df = (
        df.withColumn(
            "recognized_quantity", F.when(recognized, F.col("quantity") - F.col("returned_quantity")).otherwise(0)
        )
        .withColumn("gross_revenue_vnd", F.when(recognized, F.col("_merchandise")).otherwise(0))
        .withColumn("refunded_amount_vnd", F.when(recognized, F.col("refunded_amount_vnd")).otherwise(0))
        .withColumn("net_revenue_vnd", F.col("gross_revenue_vnd") - F.col("refunded_amount_vnd"))
        .withColumn("item_cost_vnd", (F.col("recognized_quantity") * F.col("unit_cost_vnd")).cast("long"))
        .withColumn("item_profit_vnd", F.col("net_revenue_vnd") - F.col("item_cost_vnd"))
        .withColumn("_gold_ingested_at", F.current_timestamp())
        .withColumn("_source_run_id", F.lit(run_id))
    )
    return df.drop(*[c for c in df.columns if c.startswith("_") and c not in ("_gold_ingested_at", "_source_run_id")])


def build_fact_order(
    silver_orders: DataFrame,
    silver_order_items: DataFrame,
    run_id: str,
    silver_product_variants: DataFrame | None = None,
    *,
    fact_order_items: DataFrame | None = None,
    silver_payments: DataFrame | None = None,
    **sources,
) -> DataFrame:
    items = (
        fact_order_items
        if fact_order_items is not None
        else build_fact_order_item(
            silver_order_items,
            silver_orders,
            run_id,
            silver_product_variants,
            silver_payments=silver_payments,
            **sources,
        )
    )
    totals = items.groupBy("order_id").agg(
        F.sum("gross_revenue_vnd").alias("gross_revenue_vnd"),
        F.sum("refunded_amount_vnd").alias("refunded_amount_vnd"),
        F.sum("net_revenue_vnd").alias("net_revenue_vnd"),
        F.sum("item_cost_vnd").alias("total_cost_vnd"),
        F.sum("item_profit_vnd").alias("net_profit_vnd"),
    )
    orders = silver_orders
    if "shipping_region" not in orders.columns:
        orders = orders.withColumn("shipping_region", F.lit("Khu vực khác"))
    if silver_payments is not None:
        orders = orders.join(
            silver_payments.select("order_id", F.col("status").alias("payment_status")), "order_id", "left"
        )
    else:
        orders = orders.withColumn(
            "payment_status", F.when(F.col("paid_at").isNotNull(), "succeeded").otherwise("pending")
        )
    df = orders.join(totals, "order_id", "left")
    date = F.to_date(F.from_utc_timestamp("created_at", "Asia/Ho_Chi_Minh"))
    return df.select(
        "order_id",
        "order_number",
        F.coalesce(F.col("customer_id"), F.lit(-1)).alias("customer_key"),
        F.coalesce(F.col("store_id"), F.lit(0)).alias("store_key"),
        F.date_format(date, "yyyyMMdd").cast("int").alias("order_date_key"),
        date.alias("order_date"),
        date.alias("order_date_vn"),
        "created_at",
        "shipping_region",
        "channel",
        "status",
        "payment_method",
        "payment_status",
        "coupon_id",
        "subtotal_vnd",
        F.col("discount_amount_vnd").alias("discount_vnd"),
        "shipping_fee_vnd",
        F.col("total_vnd").alias("order_total_vnd"),
        "gross_revenue_vnd",
        "refunded_amount_vnd",
        "net_revenue_vnd",
        "total_cost_vnd",
        F.col("net_profit_vnd").alias("gross_profit_vnd"),
        "net_profit_vnd",
        (F.col("status") == "failed_delivery").alias("is_boom"),
        F.col("status").isin("delivered", "completed").alias("is_delivered"),
        F.col("status").isin("delivered", "completed", "returned").alias("is_revenue_recognized"),
        F.current_timestamp().alias("_gold_ingested_at"),
        F.lit(run_id).alias("_source_run_id"),
    )
