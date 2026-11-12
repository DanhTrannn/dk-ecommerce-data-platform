"""Pure PostgreSQL source SQL/config checks; no network required."""

import os
import unittest
from unittest.mock import patch

from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.postgres import identifier, reader_settings, page_query, source_query
from lakehouse.utils.sql import render_sql


class PostgresSourceTests(unittest.TestCase):
    def test_credentials_are_separate_reader_settings(self):
        with patch.dict(
            os.environ,
            {
                "POSTGRES_ECOMMERCE_DB": "retail data",
                "POSTGRES_SOURCE_HOST": "postgres",
                "POSTGRES_SOURCE_PORT": "5432",
                "POSTGRES_ECOMMERCE_READER_USER": "reader",
                "POSTGRES_ECOMMERCE_READER_PASSWORD": "secret@:/?#",
            },
            clear=True,
        ):
            options = reader_settings()
        self.assertEqual(options["dbname"], "retail data")
        self.assertEqual(options["host"], "postgres")
        self.assertEqual(options["password"], "secret@:/?#")
        self.assertEqual(options["user"], "reader")

    def test_missing_credentials_fail_without_values_in_error(self):
        with patch.dict(os.environ, {"POSTGRES_ECOMMERCE_READER_PASSWORD": "private"}, clear=True):
            with self.assertRaises(ValueError) as caught:
                reader_settings()
        self.assertNotIn("private", str(caught.exception))

    def test_half_open_interval_normalizes_offsets(self):
        orders = next(table for table in SOURCE_TABLES if table.name == "orders")
        query = source_query(orders, "2026-11-07T07:00:00+07:00", "2026-11-07T08:00:00+07:00")
        self.assertIn('"public"."orders"', query)
        self.assertIn(">= TIMESTAMPTZ '2026-11-07T00:00:00+00:00'", query)
        self.assertIn("< TIMESTAMPTZ '2026-11-07T01:00:00+00:00'", query)
        self.assertNotIn("BETWEEN", query)

    def test_invalid_interval_is_rejected(self):
        for lower, upper in (
            ("2026-01-01", "2026-01-02"),
            ("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z"),
        ):
            with self.assertRaises(ValueError):
                source_query(SOURCE_TABLES[0], lower, upper)

    def test_initial_load_has_no_time_predicate(self):
        self.assertEqual(source_query(SOURCE_TABLES[0]), 'SELECT * FROM "public"."customers"')

    def test_page_uses_strict_last_primary_key(self):
        query = page_query('SELECT * FROM "public"."orders"', "order_id", 10, 10000)
        self.assertIn('WHERE "order_id" > 10 ORDER BY "order_id" LIMIT 10000', query)
        self.assertNotIn(" WHERE ", page_query("SELECT 1", "order_id", None, 2))
        with self.assertRaises(ValueError):
            page_query("SELECT 1", "order_id", "1; DROP TABLE orders", 2)

    def test_source_identifiers_use_postgresql_quotes(self):
        self.assertEqual(
            render_sql("postgres/table_count.sql", table_spec_name=identifier("orders")).strip(),
            'SELECT COUNT(*) AS cnt FROM "public"."orders"',
        )
        with self.assertRaises(ValueError):
            identifier("orders; DROP TABLE customers")

    def test_credential_table_is_excluded(self):
        self.assertEqual(len(SOURCE_TABLES), 27)
        self.assertNotIn("customer_credentials", {table.name for table in SOURCE_TABLES})
