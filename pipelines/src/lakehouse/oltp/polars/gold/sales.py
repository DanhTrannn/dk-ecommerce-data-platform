"""Exact integer merchandise allocation, recognition and refunds."""

import polars as pl

from .core import query

RECOGNIZED = ("delivered", "completed", "returned")


def allocate(frame, amount, weight, output):
    # Int128 products preserve integer VND; never route allocation through float division.
    frame = frame.sort(["order_id", "order_item_id"]).with_columns(
        pl.col(weight).cast(pl.Int128).sum().over("order_id").alias("_weight_total"),
        pl.col(weight).cast(pl.Int128).cum_sum().over("order_id").alias("_weight_running"),
    )
    total = pl.col("_weight_total")
    denominator = pl.max_horizontal(total, pl.lit(1))
    money = pl.col(amount).cast(pl.Int128)
    running = pl.col("_weight_running")
    value = (money * running // denominator) - (money * (running - pl.col(weight)) // denominator)
    return frame.with_columns(pl.when(total > 0).then(value).otherwise(0).cast(pl.Int64).alias(output)).drop(
        "_weight_total", "_weight_running"
    )


def fact_order_item(frames):
    df = query(
        frames,
        """SELECT i.order_item_id,i.order_id,o.created_at,coalesce(o.customer_id,-1) customer_key,
        v.product_id product_key,i.variant_id variant_key,coalesce(o.store_id,0) store_key,o.channel,o.status order_status,
        i.quantity,i.unit_price_vnd,i.cost_price_vnd unit_cost_vnd,i.quantity*i.unit_price_vnd item_total_vnd,
        coalesce(o.discount_amount_vnd,0) _order_discount,i.category_code_snapshot,i.category_name_snapshot
        FROM order_items i JOIN orders o ON i.order_id=o.order_id LEFT JOIN product_variants v ON i.variant_id=v.variant_id""",
    )
    date = pl.col("created_at").dt.convert_time_zone("Asia/Ho_Chi_Minh").dt.date()
    df = df.with_columns(
        date.alias("order_date"), date.dt.strftime("%Y%m%d").cast(pl.Int32).alias("order_date_key")
    ).drop("created_at")
    df = df.with_columns(
        pl.min_horizontal("_order_discount", pl.col("item_total_vnd").sum().over("order_id")).alias("_order_discount")
    )
    df = allocate(df, "_order_discount", "item_total_vnd", "allocated_discount_vnd").with_columns(
        (pl.col("item_total_vnd") - pl.col("allocated_discount_vnd")).alias("_merchandise")
    )
    returned = query(
        frames,
        """SELECT i.order_item_id,sum(i.quantity) _returned,sum(i.refund_amount_vnd) _return_value
        FROM return_items i JOIN return_requests r ON i.return_id=r.return_id
        WHERE r.status='completed' AND i.inspection_status='passed' GROUP BY i.order_item_id""",
    )
    refunds = query(
        frames,
        """SELECT p.order_id,sum(r.amount_vnd) _refund_total FROM refunds r JOIN payments p
        ON r.payment_id=p.payment_id WHERE r.status='succeeded' GROUP BY p.order_id""",
    )
    df = (
        df.join(returned, on="order_item_id", how="left")
        .join(refunds, on="order_id", how="left")
        .with_columns(
            pl.min_horizontal(pl.col("quantity"), pl.col("_returned").fill_null(0))
            .cast(pl.Int32)
            .alias("returned_quantity"),
            pl.min_horizontal(pl.col("_merchandise"), pl.col("_return_value").fill_null(0)).alias("_return_weight"),
            pl.min_horizontal(
                pl.col("_refund_total").fill_null(0), pl.col("_merchandise").sum().over("order_id")
            ).alias("_refund_total"),
        )
        .with_columns(
            pl.when(pl.col("_return_weight").sum().over("order_id") > 0)
            .then(pl.col("_return_weight"))
            .otherwise(pl.col("_merchandise"))
            .alias("_refund_weight")
        )
    )
    df = df.with_columns(
        pl.min_horizontal("_refund_total", pl.col("_refund_weight").sum().over("order_id")).alias("_targeted_refund")
    )
    df = allocate(df, "_targeted_refund", "_refund_weight", "_targeted_amount").with_columns(
        (pl.col("_refund_total") - pl.col("_targeted_refund")).alias("_remaining_refund"),
        (pl.col("_merchandise") - pl.col("_targeted_amount")).alias("_remaining_value"),
    )
    df = allocate(df, "_remaining_refund", "_remaining_value", "_other_refund")
    recognized = pl.col("order_status").is_in(RECOGNIZED)
    df = (
        df.with_columns(
            pl.when(recognized)
            .then(pl.col("quantity") - pl.col("returned_quantity"))
            .otherwise(0)
            .alias("recognized_quantity"),
            pl.when(recognized).then(pl.col("_merchandise")).otherwise(0).alias("gross_revenue_vnd"),
            pl.when(recognized)
            .then(pl.col("_targeted_amount") + pl.col("_other_refund"))
            .otherwise(0)
            .alias("refunded_amount_vnd"),
        )
        .with_columns(
            (pl.col("gross_revenue_vnd") - pl.col("refunded_amount_vnd")).alias("net_revenue_vnd"),
            (pl.col("recognized_quantity") * pl.col("unit_cost_vnd")).alias("item_cost_vnd"),
        )
        .with_columns((pl.col("net_revenue_vnd") - pl.col("item_cost_vnd")).alias("item_profit_vnd"))
    )
    return df.drop([name for name in df.columns if name.startswith("_")])


def fact_order(frames):
    df = query(
        frames,
        """WITH totals AS (
        SELECT order_id,sum(gross_revenue_vnd) gross_revenue_vnd,sum(refunded_amount_vnd) refunded_amount_vnd,
        sum(net_revenue_vnd) net_revenue_vnd,sum(item_cost_vnd) total_cost_vnd,sum(item_profit_vnd) net_profit_vnd
        FROM fact_order_item GROUP BY order_id)
        SELECT o.order_id,o.order_number,coalesce(o.customer_id,-1) customer_key,coalesce(o.store_id,0) store_key,
        o.created_at,o.shipping_region,o.channel,o.status,o.payment_method,p.status payment_status,o.coupon_id,
        o.subtotal_vnd,o.discount_amount_vnd discount_vnd,o.shipping_fee_vnd,o.total_vnd order_total_vnd,
        t.gross_revenue_vnd,t.refunded_amount_vnd,t.net_revenue_vnd,t.total_cost_vnd,
        t.net_profit_vnd gross_profit_vnd,t.net_profit_vnd,o.status='failed_delivery' is_boom,
        o.status IN ('delivered','completed') is_delivered,o.status IN ('delivered','completed','returned') is_revenue_recognized
        FROM orders o LEFT JOIN totals t ON o.order_id=t.order_id LEFT JOIN payments p ON o.order_id=p.order_id""",
    )
    day = pl.col("created_at").dt.convert_time_zone("Asia/Ho_Chi_Minh").dt.date()
    return df.with_columns(
        day.alias("order_date"),
        day.alias("order_date_vn"),
        day.dt.strftime("%Y%m%d").cast(pl.Int32).alias("order_date_key"),
    )
