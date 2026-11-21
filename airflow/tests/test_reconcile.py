import unittest

from iceberg_fixture import AT, TABLES, create_tables, category, movement, order, page, sql_catalog
from lakehouse.oltp.staging import write_staging
from lakehouse.oltp.reconcile import ReconciliationFailed, reconcile_table, source_reconciliation

ORDERS, CATEGORIES = TABLES["orders"], TABLES["categories"]


class FakeCursor:
    def __init__(self, answers):
        self.answers, self.sql = list(answers), []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, parameters=None):
        self.sql.append((sql, parameters))

    def fetchone(self):
        return self.answers.pop(0)

    def fetchall(self):
        return self.answers.pop(0)


class FakeConnection:
    def __init__(self, answers):
        self.cursor_ = FakeCursor(answers)

    def cursor(self):
        return self.cursor_


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)
        create_tables(self.catalog)

    def seed(self, **rows):
        for name, values in rows.items():
            write_staging(self.catalog, TABLES[name], [page(name, values)], AT)

    def test_row_count_and_money_total_are_reconciled_with_the_source(self):
        self.seed(orders=[order(1)])
        with self.assertRaises(ReconciliationFailed) as failure:
            reconcile_table(self.catalog, ORDERS, {"rows": 2, "total": 200, "ledger": None})
        self.assertIn("orders: source_rows 2 != 1", str(failure.exception))
        self.assertIn("orders: source_total_vnd 200 != 100", str(failure.exception))

    def test_matching_full_load_passes(self):
        self.seed(orders=[order(1)], categories=[category(1)])
        reconcile_table(self.catalog, ORDERS, {"rows": 1, "total": 100, "ledger": None})
        reconcile_table(self.catalog, CATEGORIES, {"rows": 1, "total": None, "ledger": None})

    def test_ledger_balance_is_reconciled_with_source_stock(self):
        ledger = TABLES["inventory_transactions"]
        self.seed(inventory_transactions=[movement(1, variant_id=7, quantity_delta=5), movement(2, variant_id=7, store_id=3, quantity_delta=2)])
        reconcile_table(self.catalog, ledger, {"rows": 2, "total": None, "ledger": {(None, 7): 5, (3, 7): 2}})
        with self.assertRaisesRegex(ReconciliationFailed, r"inventory_transactions: ledger_balance_mismatch = 2"):
            reconcile_table(self.catalog, ledger, {"rows": 2, "total": None, "ledger": {(None, 7): 5, (3, 7): 1, (3, 8): 4}})


class SourceReconciliationTests(unittest.TestCase):
    CUTOFF = "2026-11-21T03:00:00+07:00"

    def test_rows_created_after_cutoff_do_not_fail_checks(self):
        connection = FakeConnection([(5, 500)])
        self.assertEqual(source_reconciliation(connection, ORDERS, self.CUTOFF), {"rows": 5, "total": 500, "ledger": None})
        sql, parameters = connection.cursor_.sql[0]
        self.assertIn('"created_at" < %(cutoff)s', sql)
        self.assertIn('sum("total_vnd")', sql)
        self.assertEqual(parameters["cutoff"].isoformat(), "2026-11-20T20:00:00+00:00")

    def test_table_without_money_total_counts_rows_only(self):
        connection = FakeConnection([(3, None)])
        self.assertEqual(source_reconciliation(connection, CATEGORIES, self.CUTOFF)["total"], None)

    def test_stock_subtracts_movements_after_cutoff_in_one_query(self):
        ledger = TABLES["inventory_transactions"]
        connection = FakeConnection([(4, None), [(None, 7, 5), (3, 7, 2)]])
        result = source_reconciliation(connection, ledger, self.CUTOFF)
        self.assertEqual(result["ledger"], {(None, 7): 5, (3, 7): 2})
        stock_sql = connection.cursor_.sql[1][0]
        self.assertIn('"created_at" >= %(cutoff)s', stock_sql)
        self.assertIn("opening_on_hand", stock_sql)


if __name__ == "__main__":
    unittest.main()
