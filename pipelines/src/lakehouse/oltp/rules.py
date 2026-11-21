"""Row-level business rules checked on a run's Bronze rows before they are merged into Silver."""

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
    "shipments": ("order_id", "status"),
    "return_requests": ("order_id", "customer_id", "action_type", "status"),
    "return_items": ("return_id", "order_item_id", "variant_id", "quantity"),
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


def rules(table):
    """(label, condition) pairs; a row violates a rule when its condition is true."""
    col = pl.col
    found = [("missing_primary_key", col(table.pk).is_null())]
    found += [("missing_required_" + name, col(name).is_null()) for name in REQUIRED.get(table.name, ())]
    if table.name in STATUS_RULES:
        field, label, values = STATUS_RULES[table.name]
        found.append((label, ~col(field).is_in(values)))
    if table.name == "product_variants":
        found.append(("negative_price", col("price_vnd") < 0))
    if table.name == "orders":
        found += [("negative_" + name, col(name) < 0) for name in ("subtotal_vnd", "discount_amount_vnd", "shipping_fee_vnd", "total_vnd")]
        found.append((
            "order_total_mismatch",
            col("subtotal_vnd") - col("discount_amount_vnd") + col("shipping_fee_vnd") != col("total_vnd"),
        ))
    if table.name == "order_items":
        found.append(("invalid_quantity", col("quantity") <= 0))
        found += [("negative_" + name, col(name) < 0) for name in ("unit_price_vnd", "cost_price_vnd", "line_total_vnd")]
        found.append(("order_item_total_mismatch", col("quantity") * col("unit_price_vnd") != col("line_total_vnd")))
    if table.name == "payments":
        found.append(("negative_payment", col("amount_vnd") < 0))
    if table.name == "refunds":
        found.append(("negative_refund", col("amount_vnd") < 0))
    if table.name == "coupons":
        found.append((
            "invalid_discount_percentage",
            (col("discount_type") == "percentage") & ((col("discount_value") < 1) | (col("discount_value") > 100)),
        ))
    if table.name == "product_reviews":
        found.append(("invalid_rating", (col("rating") < 1) | (col("rating") > 5)))
    if table.name == "inbound_receipt_items":
        found.append(("invalid_quantity", col("quantity") <= 0))
        found.append(("negative_unit_cost", col("unit_cost_vnd") < 0))
        found.append(("negative_total_cost", col("total_cost_vnd") < 0))
        found.append(("inbound_item_total_mismatch", col("quantity") * col("unit_cost_vnd") != col("total_cost_vnd")))
    return found


def violation_counts(frame, table):
    """Rule label -> number of violating rows, only for rules with violations."""
    counts = frame.select(
        [condition.fill_null(False).cast(pl.Int64).sum().alias(label) for label, condition in rules(table)]
    ).row(0, named=True)
    return {label: count for label, count in counts.items() if count}
