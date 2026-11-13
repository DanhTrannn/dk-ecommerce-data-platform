"""Inventory ledger checks: balances equal the ledger, never go negative, and every row names its document."""

from datagen.postgres import number
from datagen.verify.report import Check, check

# Each query counts violations; the expected count is always 0.
VIOLATIONS = {
    "ledger.central_balance_mismatches": """
        SELECT count(*)
        FROM inventory i
        LEFT JOIN (
            SELECT variant_id, sum(quantity_delta) AS moved
            FROM inventory_transactions WHERE store_id IS NULL GROUP BY variant_id
        ) t USING (variant_id)
        WHERE i.on_hand <> i.opening_on_hand + coalesce(t.moved, 0)""",
    "ledger.store_balance_mismatches": """
        SELECT count(*)
        FROM store_inventory i
        LEFT JOIN (
            SELECT store_id, variant_id, sum(quantity_delta) AS moved
            FROM inventory_transactions WHERE store_id IS NOT NULL GROUP BY store_id, variant_id
        ) t USING (store_id, variant_id)
        WHERE i.on_hand <> i.opening_on_hand + coalesce(t.moved, 0)""",
    "ledger.negative_running_balances": """
        SELECT count(*)
        FROM (
            SELECT 1
            FROM (
                SELECT t.variant_id, t.store_id,
                       coalesce(c.opening_on_hand, s.opening_on_hand, 0)
                       + sum(t.quantity_delta) OVER (
                           PARTITION BY t.variant_id, t.store_id ORDER BY t.created_at, t.transaction_id
                       ) AS balance
                FROM inventory_transactions t
                LEFT JOIN inventory c ON t.store_id IS NULL AND c.variant_id = t.variant_id
                LEFT JOIN store_inventory s ON s.store_id = t.store_id AND s.variant_id = t.variant_id
            ) running
            WHERE balance < 0
            GROUP BY variant_id, store_id
        ) pairs""",
    "ledger.rows_without_required_document": """
        SELECT count(*)
        FROM inventory_transactions
        WHERE NOT CASE
            WHEN movement_type IN ('outbound_order', 'outbound_pos', 'order_cancellation', 'return_boom')
                THEN order_id IS NOT NULL AND num_nonnulls(receipt_id, return_id) = 0
            WHEN movement_type = 'inbound'
                THEN receipt_id IS NOT NULL AND num_nonnulls(order_id, return_id) = 0
            WHEN movement_type IN ('return_customer', 'exchange_out')
                THEN return_id IS NOT NULL AND num_nonnulls(order_id, receipt_id) = 0
            WHEN movement_type IN ('transfer_to_store', 'transfer_received')
                THEN reference_code IS NOT NULL AND num_nonnulls(order_id, receipt_id, return_id) = 0
            ELSE true
        END""",
}


def checks(connection) -> list[Check]:
    return [check(name, 0, number(connection, sql)) for name, sql in VIOLATIONS.items()]
