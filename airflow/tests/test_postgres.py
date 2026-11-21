import unittest

from lakehouse.oltp.sources import SOURCE_TABLES
from lakehouse.utils.postgres import identifier, source_query


class PostgresSourceTests(unittest.TestCase):
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

    def test_initial_load_selects_configured_columns_without_time_predicate(self):
        query = source_query(SOURCE_TABLES[0])
        self.assertTrue(query.startswith('SELECT "customer_id", "public_id", "role"'))
        self.assertTrue(query.endswith('"updated_at" FROM "public"."customers"'))

    def test_full_load_reads_rows_created_before_the_run_start(self):
        orders = next(table for table in SOURCE_TABLES if table.name == "orders")
        query = source_query(orders, created_before="2026-11-21T03:00:00+07:00")
        self.assertIn("WHERE \"created_at\" < TIMESTAMPTZ '2026-11-20T20:00:00+00:00'", query)

    def test_window_and_cutoff_are_exclusive(self):
        with self.assertRaises(ValueError):
            source_query(
                SOURCE_TABLES[0], "2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z", created_before="2026-11-21T00:00:00Z"
            )

    def test_cutoff_needs_a_timezone(self):
        with self.assertRaises(ValueError):
            source_query(SOURCE_TABLES[0], created_before="2026-11-21T00:00:00")

    def test_source_identifiers_use_postgresql_quotes(self):
        self.assertEqual(identifier("orders"), '"orders"')
        with self.assertRaises(ValueError):
            identifier("orders; DROP TABLE customers")

    def test_credential_table_is_excluded(self):
        self.assertEqual(len(SOURCE_TABLES), 19)
        self.assertNotIn("customer_credentials", {table.name for table in SOURCE_TABLES})
