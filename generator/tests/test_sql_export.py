import tempfile
import unittest
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from generator.config import PriceBand, load_config
from generator.sql_export import export_sql


CONFIGS = Path(__file__).resolve().parents[1] / "configs"


def capture(config):
    tables = defaultdict(list)

    def collect(stream, table, columns, rows):
        tables[table].extend(dict(zip(columns, row)) for row in rows)

    with tempfile.TemporaryDirectory() as directory:
        with patch("generator.sql_export._write_insert", side_effect=collect):
            export_sql(config, Path(directory) / "history.sql")
    return tables


class SqlExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = load_config(CONFIGS / "small.yml")
        cls.config = replace(
            config,
            scale={"customers": 80, "products": 12, "variants": 48, "orders": 800},
            distributions=replace(config.distributions, order_size=(0, 0, 0, 100)),
        )
        cls.tables = capture(cls.config)
        cls.orders = {r["order_id"]: r for r in cls.tables["orders"]}
        cls.payments = {r["order_id"]: r for r in cls.tables["payments"]}

    def test_cod_is_unpaid_until_delivery(self):
        unpaid = [r for r in self.orders.values() if r["payment_method"] == "cod" and r["completed_at"] is None]
        self.assertTrue(unpaid)
        for order in unpaid:
            self.assertIsNone(order["paid_at"])
            self.assertEqual(self.payments[order["order_id"]]["status"], "pending")
            self.assertNotEqual(order["status"], "paid")

    def test_paid_timestamp_matches_successful_payment(self):
        for order in self.orders.values():
            payment = self.payments[order["order_id"]]
            self.assertEqual(order["paid_at"] is not None, payment["status"] == "succeeded")

    def test_return_references_correct_order_item_and_refund(self):
        items = {r["order_item_id"]: r for r in self.tables["order_items"]}
        requests = {r["return_id"]: r for r in self.tables["return_requests"]}
        refunds = {r["payment_id"]: r for r in self.tables["refunds"]}
        self.assertTrue(requests)
        actions = set()
        for row in self.tables["return_items"]:
            request = requests[row["return_id"]]
            item = items[row["order_item_id"]]
            order = self.orders[request["order_id"]]
            actions.add(request["action_type"])
            self.assertEqual(item["order_id"], request["order_id"])
            self.assertEqual(item["variant_id"], row["variant_id"])
            self.assertLessEqual(row["quantity"], item["quantity"])
            self.assertGreaterEqual(request["created_at"], order["completed_at"])
            self.assertLessEqual(request["resolved_at"], self.config.anchor_time)
            if request["action_type"] == "refund":
                expected = round(item["unit_price_vnd"] * (1 - order["discount_amount_vnd"] / order["subtotal_vnd"]))
                self.assertEqual(row["refund_amount_vnd"], expected * row["quantity"])
                payment = self.payments[order["order_id"]]
                self.assertEqual(refunds[payment["payment_id"]]["amount_vnd"], row["refund_amount_vnd"])
            else:
                self.assertIsNotNone(row["exchange_variant_id"])
        self.assertEqual(actions, {"refund", "exchange"})

    def test_inventory_balances_reconcile_with_movements(self):
        deltas = defaultdict(int)
        for row in self.tables["inventory_transactions"]:
            deltas[row["variant_id"]] += row["quantity_delta"]
        for balance in self.tables["inventory"]:
            self.assertEqual(balance["opening_on_hand"] + deltas[balance["variant_id"]], balance["on_hand"])

    def test_stock_history_never_goes_negative(self):
        balances = {r["variant_id"]: r["opening_on_hand"] for r in self.tables["inventory"]}
        for row in sorted(self.tables["inventory_transactions"], key=lambda r: (r["created_at"], r["quantity_delta"])):
            balances[row["variant_id"]] += row["quantity_delta"]
            self.assertGreaterEqual(balances[row["variant_id"]], 0)

    def test_returns_have_matching_stock_movements(self):
        requests = {r["return_id"]: r for r in self.tables["return_requests"]}
        movements = defaultdict(list)
        for row in self.tables["inventory_transactions"]:
            movements[row["reference_code"]].append((row["variant_id"], row["movement_type"], row["quantity_delta"]))
        for returned in self.tables["return_items"]:
            request = requests[returned["return_id"]]
            actual = movements[request["return_code"]]
            self.assertIn((returned["variant_id"], "return_customer", returned["quantity"]), actual)
            if request["action_type"] == "exchange":
                self.assertIn((returned["exchange_variant_id"], "exchange_out", -returned["quantity"]), actual)

    def test_cancelled_and_failed_delivery_restore_stock(self):
        movements = defaultdict(int)
        counts = defaultdict(int)
        for row in self.tables["inventory_transactions"]:
            movements[row["reference_code"]] += row["quantity_delta"]
            counts[row["reference_code"]] += 1
        for order in self.orders.values():
            reference = f"ORD-{order['order_id']}"
            self.assertGreater(counts[reference], 0)
            if order["status"] in {"cancelled", "failed_delivery"}:
                self.assertEqual(movements[reference], 0)

    def test_refunds_require_a_successful_payment(self):
        payments = {r["payment_id"]: r for r in self.tables["payments"]}
        for refund in self.tables["refunds"]:
            payment = payments[refund["payment_id"]]
            self.assertEqual(payment["status"], "succeeded")
            self.assertLessEqual(refund["amount_vnd"], payment["amount_vnd"])

    def test_history_is_continuous_and_matches_final_order(self):
        history = defaultdict(list)
        for row in self.tables["order_status_history"]:
            history[row["order_id"]].append(row)
        for order in self.orders.values():
            previous_status = None
            previous_time = order["created_at"]
            for event in history[order["order_id"]]:
                self.assertEqual(event["from_status"], previous_status)
                self.assertGreaterEqual(event["transitioned_at"], previous_time)
                self.assertLessEqual(event["transitioned_at"], order["updated_at"])
                previous_status, previous_time = event["to_status"], event["transitioned_at"]
            self.assertEqual(previous_status, order["status"])

    def test_single_variant_returns_fall_back_to_full_refund(self):
        config = replace(
            self.config,
            scale={"customers": 80, "products": 1, "variants": 1, "orders": 800},
            distributions=replace(self.config.distributions, quantity_per_item=(100, 0, 0)),
        )
        tables = capture(config)
        orders = {r["order_id"]: r for r in tables["orders"]}
        self.assertTrue(tables["return_requests"])
        for request in tables["return_requests"]:
            self.assertEqual(request["action_type"], "refund")
            self.assertEqual(orders[request["order_id"]]["status"], "returned")

    def test_free_items_can_be_returned(self):
        config = replace(
            self.config,
            distributions=replace(self.config.distributions, price_bands=(PriceBand(0, 0, 1),)),
        )
        tables = capture(config)
        self.assertTrue(tables["return_requests"])
        for item in tables["return_items"]:
            self.assertEqual(item["refund_amount_vnd"], 0)

    def test_all_presets_load(self):
        for path in sorted(CONFIGS.glob("*.yml")):
            with self.subTest(config=path.name):
                load_config(path)

    def test_export_is_byte_reproducible(self):
        with tempfile.TemporaryDirectory() as directory:
            first, second = (Path(directory) / name for name in ("a.sql", "b.sql"))
            export_sql(self.config, first)
            export_sql(self.config, second)
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_multiple_seeds_and_insert_batches_preserve_relations(self):
        for seed in (19, 73, 20260724):
            with self.subTest(seed=seed):
                config = replace(self.config, seed=seed, scale={**self.config.scale, "orders": 1200})
                tables = capture(config)
                self.assertEqual(len(tables["orders"]), 1200)
                items = {r["order_item_id"]: r for r in tables["order_items"]}
                requests = {r["return_id"]: r for r in tables["return_requests"]}
                for returned in tables["return_items"]:
                    item = items[returned["order_item_id"]]
                    self.assertEqual(item["order_id"], requests[returned["return_id"]]["order_id"])
                    self.assertEqual(item["variant_id"], returned["variant_id"])
                deltas = defaultdict(int)
                for transaction in tables["inventory_transactions"]:
                    deltas[transaction["variant_id"]] += transaction["quantity_delta"]
                for balance in tables["inventory"]:
                    self.assertEqual(balance["opening_on_hand"] + deltas[balance["variant_id"]], balance["on_hand"])


if __name__ == "__main__":
    unittest.main()
