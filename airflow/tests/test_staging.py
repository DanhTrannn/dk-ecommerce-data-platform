import unittest
from datetime import timedelta
from unittest.mock import patch

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from iceberg_fixture import AT, TABLES, create_tables, category, order, page, sql_catalog
from lakehouse.oltp import staging
from lakehouse.oltp.staging import staging_rows, write_staging

CATEGORIES = TABLES["categories"]


class StagingTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)
        create_tables(self.catalog)

    def pages(self, *ranges):
        return [page("categories", [category(i) for i in range(start, end + 1)]) for start, end in ranges]

    def rows(self, table=CATEGORIES):
        return staging_rows(self.catalog, table).read_all()

    def test_chunks_are_written_with_the_ingestion_time(self):
        self.assertEqual(write_staging(self.catalog, CATEGORIES, self.pages((1, 3), (4, 6)), AT), 6)
        rows = self.rows()
        self.assertEqual(rows.schema.names[-1], "_ingested_at")
        self.assertEqual(set(rows["_ingested_at"].to_pylist()), {AT})

    def test_a_batch_is_appended_once_it_reaches_the_row_limit(self):
        with patch.object(staging, "BATCH_ROWS", 3):
            write_staging(self.catalog, CATEGORIES, self.pages((1, 2), (3, 4), (5, 5)), AT)
        self.assertEqual(self.rows().num_rows, 5)

    def test_a_rerun_replaces_the_previous_extract(self):
        write_staging(self.catalog, CATEGORIES, self.pages((1, 3)), AT)
        write_staging(self.catalog, CATEGORIES, self.pages((1, 3)), AT + timedelta(minutes=5))
        rows = self.rows()
        self.assertEqual(rows.num_rows, 3)
        self.assertEqual(set(rows["_ingested_at"].to_pylist()), {AT + timedelta(minutes=5)})

    def test_an_empty_extract_empties_the_table(self):
        write_staging(self.catalog, CATEGORIES, self.pages((1, 3)), AT)
        self.assertEqual(write_staging(self.catalog, CATEGORIES, self.pages((1, 0)), AT), 0)
        self.assertEqual(self.rows().num_rows, 0)

    def test_reloading_one_table_leaves_the_others(self):
        orders = TABLES["orders"]
        write_staging(self.catalog, CATEGORIES, self.pages((1, 3)), AT)
        write_staging(self.catalog, orders, [page("orders", [order(1), order(2)])], AT)
        write_staging(self.catalog, CATEGORIES, self.pages((1, 2)), AT)
        self.assertEqual(self.rows().num_rows, 2)
        self.assertEqual(self.rows(orders).num_rows, 2)

    def test_ingest_table_reads_one_table_in_its_own_transaction(self):
        reads = []

        def chunks(connection, table, start=None, end=None, *, created_before=None):
            reads.append((table.name, start, end, created_before))
            return self.pages((1, 2))

        with patch.object(staging, "source_chunks", chunks), patch.object(staging, "source_read_transaction") as transaction:
            written = staging.extract_table(self.catalog, object(), CATEGORIES, AT, created_before="2026-11-21T00:00:00Z")
        self.assertEqual(written, 2)
        self.assertEqual(reads, [("categories", None, None, "2026-11-21T00:00:00Z")])
        transaction.assert_called_once()

    def test_missing_table_asks_for_the_init_dag(self):
        empty = sql_catalog(self)
        empty.create_namespace("staging")
        with self.assertRaisesRegex(NoSuchTableError, "lakehouse_oltp_init"):
            write_staging(empty, CATEGORIES, self.pages((1, 2)), AT)
        with self.assertRaisesRegex(NoSuchTableError, "lakehouse_oltp_init"):
            staging_rows(empty, CATEGORIES)

    def test_a_column_the_table_lacks_is_refused(self):
        extra = self.pages((1, 2))[0].append_column("loyalty_tier", pa.array(["gold", "silver"]))
        with self.assertRaisesRegex(ValueError, r"\['loyalty_tier'\].*lakehouse_oltp_init"):
            write_staging(self.catalog, CATEGORIES, [extra], AT)


if __name__ == "__main__":
    unittest.main()
