"""Row-level rules checked on a run's Bronze rows before they are merged into Silver."""

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


def rules(table):
    """(label, condition) pairs; a row violates a rule when its condition is true.

    Statuses, non-negative amounts and order/line arithmetic are PostgreSQL CHECK constraints, so they are not
    repeated here. Required fields are NOT NULL at the source too, but a null here also catches a column the
    pipeline failed to carry. The inbound line total has no source constraint.
    """
    col = pl.col
    found = [("missing_primary_key", col(table.pk).is_null())]
    found += [("missing_required_" + name, col(name).is_null()) for name in REQUIRED.get(table.name, ())]
    if table.name == "inbound_receipt_items":
        found.append(("inbound_item_total_mismatch", col("quantity") * col("unit_cost_vnd") != col("total_cost_vnd")))
    return found


def violation_counts(frame, table):
    """Rule label -> number of violating rows, only for rules with violations."""
    counts = frame.select(
        [condition.fill_null(False).cast(pl.Int64).sum().alias(label) for label, condition in rules(table)]
    ).row(0, named=True)
    return {label: count for label, count in counts.items() if count}
