"""Offline contracts for all-source schema, validation, privacy and version ordering."""

import hashlib
import importlib.util
import json
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

AVAILABLE = all(importlib.util.find_spec(name) for name in ("polars", "pyiceberg", "psycopg2"))


@unittest.skipUnless(AVAILABLE, "install batch-pipeline[polars] for native batch contracts")
class PolarsBatchTests(unittest.TestCase):
    def setUp(self):
        import polars as pl
        import pyarrow as pa
        from lakehouse.utils.config import SOURCE_TABLES

        self.pl, self.pa = pl, pa
        self.tables = {table.name: table for table in SOURCE_TABLES}
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def test_all_27_schemas_and_pii_output_contracts(self):
        from lakehouse.oltp.polars.schema import silver_schema
        from lakehouse.oltp.polars.privacy import token_name

        self.assertEqual(len(self.tables), 27)
        for table in self.tables.values():
            schema = silver_schema(table)
            self.assertIn(table.pk, schema.names)
            self.assertIn(table.cursor_field, schema.names)
            for name in table.pseudonymize:
                self.assertNotIn(name, schema.names)
                self.assertIn(token_name(name), schema.names)

    def test_unclassified_empty_bronze_is_not_relabelled(self):
        from lakehouse.oltp.polars.writer import guard_bronze

        table = SimpleNamespace(schema=lambda: SimpleNamespace(column_names=["id"]), current_snapshot=lambda: None)
        with self.assertRaisesRegex(ValueError, "legacy or unclassified"):
            guard_bronze(table)
        table.schema = lambda: SimpleNamespace(column_names=["id", "_source_system"])
        guard_bronze(table)

    def test_table_and_namespace_guards(self):
        from lakehouse.oltp.polars.pipeline import selected_tables, validate_run

        self.assertEqual(len(selected_tables()), 27)
        for names in ([], ["customers", "customers"], ["customer_credentials"], ["orders; DROP TABLE orders"]):
            with self.assertRaises(ValueError):
                selected_tables(names)
        for prefix in ("bronze", "silver", "gold", "polars_batch_../silver", "polars_batch_"):
            with self.assertRaises(ValueError):
                validate_run(prefix, "run", "lakehouse", 10000)

    def test_empty_source_schema_uses_postgres_types_not_inferred_nulls(self):
        from lakehouse.oltp.polars.source import arrow_source_schema

        fields = [
            SimpleNamespace(name="id", type_code=20),
            SimpleNamespace(name="value", type_code=2950),
            SimpleNamespace(name="changed", type_code=1114),
        ]
        schema = arrow_source_schema(fields)
        self.assertEqual(schema.types, [self.pa.int64(), self.pa.string(), self.pa.timestamp("us", tz="UTC")])
        with self.assertRaises(ValueError):
            arrow_source_schema([SimpleNamespace(name="unsupported", type_code=999999)])

    def test_pii_tokens_preserve_existing_space_trim_and_null_semantics(self):
        from lakehouse.oltp.polars.privacy import token

        expected = hashlib.sha256("secret:đặng trần".encode()).hexdigest()
        self.assertEqual(token("  ĐẶNG TRẦN  ", "secret"), expected)
        self.assertNotEqual(token("\tĐẶNG TRẦN\t", "secret"), expected)
        self.assertIsNone(token(None, "secret"))

    def test_multiple_order_violations_and_privacy_safe_quarantine(self):
        from lakehouse.oltp.polars.schema import silver_schema
        from lakehouse.oltp.polars.transforms import prepare_silver
        from lakehouse.oltp.polars.privacy import token

        source = self.pa.Table.from_pylist(
            [
                dict(
                    order_id=None,
                    status="bad",
                    subtotal_vnd=-10,
                    discount_amount_vnd=0,
                    shipping_fee_vnd=0,
                    total_vnd=9,
                    created_at=self.now,
                    updated_at=self.now,
                    receiver_name="  ĐẶNG TRẦN  ",
                    receiver_phone="0900000000",
                    shipping_address_text="Quận 7, Hồ Chí Minh",
                )
            ]
        )
        valid, rejected = prepare_silver(
            source, self.tables["orders"], silver_schema(self.tables["orders"]), "run", self.now, "secret"
        )
        self.assertEqual(valid.num_rows, 0)
        row = rejected.to_pylist()[0]
        self.assertEqual(
            row["violation_types"],
            ["missing_primary_key", "invalid_order_status", "negative_subtotal_vnd", "order_total_mismatch"],
        )
        record = json.loads(row["record_data"])
        self.assertNotIn("order_id", record)
        self.assertEqual(record["receiver_name"], token("  ĐẶNG TRẦN  ", "secret"))
        self.assertEqual(record["shipping_region"], "Quận 7")
        self.assertNotIn("0900000000", row["record_data"])
        self.assertEqual(record["created_at"], "2026-01-01T00:00:00.000Z")

    def test_valid_order_keeps_region_and_drops_raw_pii(self):
        from lakehouse.oltp.polars.schema import silver_schema
        from lakehouse.oltp.polars.transforms import prepare_silver

        page = self.pa.Table.from_pylist(
            [
                dict(
                    order_id=1,
                    status="completed",
                    subtotal_vnd=10,
                    total_vnd=10,
                    discount_amount_vnd=0,
                    shipping_fee_vnd=0,
                    created_at=self.now,
                    updated_at=self.now,
                    shipping_address_text="Cầu Giấy",
                    receiver_name="Lan",
                    receiver_phone=None,
                )
            ]
        )
        data, rejected = prepare_silver(
            page, self.tables["orders"], silver_schema(self.tables["orders"]), "run", self.now, "secret"
        )
        self.assertEqual(rejected.num_rows, 0)
        self.assertNotIn("receiver_name", data.column_names)
        self.assertEqual(data.to_pylist()[0]["shipping_region"], "Cầu Giấy")
        self.assertIsNone(data.to_pylist()[0]["receiver_phone_pseudonymized"])

    def test_business_rule_families(self):
        from lakehouse.oltp.polars.validation import validate_rows

        cases = [
            ("product_variants", dict(product_id=1, sku="x", price_vnd=-1), "negative_price"),
            ("payments", dict(order_id=1, status="bad", amount_vnd=-1), "invalid_payment_status"),
            ("refunds", dict(payment_id=1, status="bad", amount_vnd=-1), "negative_refund"),
            ("coupons", dict(discount_type="percentage", discount_value=101), "invalid_discount_percentage"),
            ("product_reviews", dict(rating=9), "invalid_rating"),
            ("inventory", dict(on_hand=-1), "negative_inventory"),
            ("store_inventory", dict(variant_id=1, on_hand=-1), "negative_inventory"),
            ("shipments", dict(order_id=1, status="bad"), "invalid_shipment_status"),
            (
                "return_requests",
                dict(order_id=1, customer_id=1, action_type="bad", status="new"),
                "invalid_return_action",
            ),
            ("inventory_transactions", dict(movement_type="bad"), "invalid_movement_type"),
            ("inbound_receipts", dict(status="new", total_cost_vnd=-1, total_items_count=-1), "negative_total_cost"),
            (
                "inbound_receipt_items",
                dict(receipt_id=1, variant_id=1, quantity=2, unit_cost_vnd=3, total_cost_vnd=5),
                "inbound_item_total_mismatch",
            ),
            (
                "order_items",
                dict(order_id=1, variant_id=1, quantity=0, unit_price_vnd=2, line_total_vnd=3),
                "invalid_quantity",
            ),
            ("cart_items", dict(quantity=0), "invalid_quantity"),
        ]
        for name, row, expected in cases:
            table = self.tables[name]
            row.setdefault(table.pk, 1)
            valid, rejected = validate_rows(self.pl.DataFrame([row]), table)
            self.assertEqual(valid.height, 0, name)
            self.assertIn(expected, rejected["_violation_types"].to_list()[0], name)

    def test_older_cursor_is_rejected_and_null_target_can_be_filled(self):
        from lakehouse.oltp.polars.transforms import filter_current

        table = self.tables["orders"]
        delta = self.pa.Table.from_pylist([dict(order_id=1, updated_at=self.now)])
        current = self.pa.Table.from_pylist([dict(order_id=1, updated_at=self.now + timedelta(days=1))])
        self.assertEqual(filter_current(delta, current, table).num_rows, 0)
        current = self.pa.Table.from_arrays(
            [self.pa.array([1]), self.pa.array([None], type=self.pa.timestamp("us", tz="UTC"))],
            names=["order_id", "updated_at"],
        )
        self.assertEqual(filter_current(delta, current, table).num_rows, 1)
