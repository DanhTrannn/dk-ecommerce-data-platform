"""Polars implementation of the current Silver row-level business rules."""

import polars as pl

REQUIRED = {
    "customers": ("role", "status"),
    "categories": ("code", "name"),
    "products": ("category_id", "name"),
    "product_variants": ("product_id", "sku", "price_vnd"),
    "orders": ("status", "subtotal_vnd", "total_vnd", "created_at"),
    "order_items": ("order_id", "variant_id", "quantity", "unit_price_vnd", "line_total_vnd"),
    "payments": ("order_id", "status", "amount_vnd"),
    "refunds": ("payment_id", "status", "amount_vnd"),
    "inventory": ("variant_id", "on_hand"),
    "store_inventory": ("store_id", "variant_id", "on_hand"),
    "shipments": ("order_id", "status"),
    "return_requests": ("order_id", "customer_id", "action_type", "status"),
    "return_items": ("return_id", "order_item_id", "variant_id", "quantity"),
    "inbound_receipts": ("status", "total_items_count", "total_cost_vnd"),
    "inbound_receipt_items": ("receipt_id", "variant_id", "quantity", "unit_cost_vnd", "total_cost_vnd"),
}
STATUS_RULES = {
    "orders": (
        "status",
        "invalid_order_status",
        (
            "pending_payment",
            "paid",
            "payment_failed",
            "confirmed",
            "shipping",
            "delivered",
            "completed",
            "cancelled",
            "failed_delivery",
            "returned",
        ),
    ),
    "payments": ("status", "invalid_payment_status", ("succeeded", "failed", "pending")),
    "refunds": ("status", "invalid_refund_status", ("succeeded", "failed")),
    "shipments": (
        "status",
        "invalid_shipment_status",
        ("assigned", "picked_up", "in_transit", "delivered", "failed", "returned_to_warehouse"),
    ),
    "return_requests": ("action_type", "invalid_return_action", ("exchange", "refund")),
    "inventory_transactions": (
        "movement_type",
        "invalid_movement_type",
        (
            "inbound",
            "outbound_order",
            "outbound_pos",
            "transfer_to_store",
            "transfer_received",
            "return_boom",
            "order_cancellation",
            "return_customer",
            "exchange_out",
            "adjustment",
        ),
    ),
}


def validate_rows(frame, table):
    issues = []

    def add(condition, label):
        issues.append(pl.when(condition.fill_null(False)).then(pl.lit(label)).otherwise(pl.lit(None, dtype=pl.String)))

    add(pl.col(table.pk).is_null(), "missing_primary_key")
    for name in REQUIRED.get(table.name, ()):
        if name in frame.columns:
            add(pl.col(name).is_null(), "missing_required_" + name)
    col = pl.col
    if table.name == "product_variants":
        add(col("price_vnd") < 0, "negative_price")
    if table.name == "orders":
        field, label, values = STATUS_RULES[table.name]
        add(~col(field).is_in(values), label)
        for name in ("subtotal_vnd", "discount_amount_vnd", "shipping_fee_vnd", "total_vnd"):
            if name in frame.columns:
                add(col(name) < 0, "negative_" + name)
        if {"subtotal_vnd", "discount_amount_vnd", "shipping_fee_vnd", "total_vnd"}.issubset(frame.columns):
            add(
                col("subtotal_vnd") - col("discount_amount_vnd") + col("shipping_fee_vnd") != col("total_vnd"),
                "order_total_mismatch",
            )
    if table.name in ("cart_items", "order_items"):
        add(col("quantity") <= 0, "invalid_quantity")
    if table.name == "order_items":
        for name in ("unit_price_vnd", "cost_price_vnd", "line_total_vnd"):
            if name in frame.columns:
                add(col(name) < 0, "negative_" + name)
        if {"quantity", "unit_price_vnd", "line_total_vnd"}.issubset(frame.columns):
            add(col("quantity") * col("unit_price_vnd") != col("line_total_vnd"), "order_item_total_mismatch")
    if table.name == "payments":
        field, label, values = STATUS_RULES[table.name]
        add(~col(field).is_in(values), label)
        if "amount_vnd" in frame.columns:
            add(col("amount_vnd") < 0, "negative_payment")
    if table.name == "refunds":
        add(col("amount_vnd") < 0, "negative_refund")
        if "status" in frame.columns:
            add(~col("status").is_in(STATUS_RULES["refunds"][2]), "invalid_refund_status")
    if table.name == "coupons":
        add(
            (col("discount_type") == "percentage") & ((col("discount_value") < 1) | (col("discount_value") > 100)),
            "invalid_discount_percentage",
        )
    if table.name == "product_reviews":
        add((col("rating") < 1) | (col("rating") > 5), "invalid_rating")
    if table.name in ("inventory", "store_inventory"):
        add(col("on_hand") < 0, "negative_inventory")
    if table.name in ("shipments", "return_requests", "inventory_transactions"):
        field, label, values = STATUS_RULES[table.name]
        add(~col(field).is_in(values), label)
    if table.name == "inbound_receipts":
        add(col("total_cost_vnd") < 0, "negative_total_cost")
        add(col("total_items_count") < 0, "negative_items_count")
    if table.name == "inbound_receipt_items":
        add(col("quantity") <= 0, "invalid_quantity")
        add(col("unit_cost_vnd") < 0, "negative_unit_cost")
        add(col("total_cost_vnd") < 0, "negative_total_cost")
        if {"quantity", "unit_cost_vnd", "total_cost_vnd"}.issubset(frame.columns):
            add(col("quantity") * col("unit_cost_vnd") != col("total_cost_vnd"), "inbound_item_total_mismatch")
    tagged = frame.with_columns(pl.concat_list(issues).list.drop_nulls().alias("_violation_types")).with_columns(
        pl.col("_violation_types").list.join("|").alias("_violation_type")
    )
    return (
        tagged.filter(pl.col("_violation_types").list.len() == 0).drop("_violation_type", "_violation_types"),
        tagged.filter(pl.col("_violation_types").list.len() > 0),
    )
