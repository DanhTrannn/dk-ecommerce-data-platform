import re
import unittest
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from iceberg_fixture import AT, TABLES, create_tables, category, order, page, sql_catalog
from lakehouse.oltp import staging
from lakehouse.oltp.drift import SchemaChanged
from lakehouse.oltp.schema import create_staging_and_bronze
from lakehouse.oltp.staging import LINEAGE_FIELDS, extract_table, staging_rows, write_staging

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

        def chunks(connection, table, start=None, end=None, *, created_before=None, columns=None):
            reads.append((table.name, start, end, created_before, columns))
            return self.pages((1, 2))

        with patch.object(staging, "source_chunks", chunks), patch.object(staging, "source_read_transaction") as transaction:
            written = staging.extract_table(self.catalog, object(), CATEGORIES, AT, created_before="2026-11-21T00:00:00Z")
        self.assertEqual(written, 2)
        self.assertEqual(reads, [("categories", None, None, "2026-11-21T00:00:00Z", None)])
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


CODES = {"category_id": 20, "parent_category_id": 20, "is_active": 16, "created_at": 1184, "updated_at": 1184}


class Source:
    """Fake source: answers the `LIMIT 0` column read and a SELECT of named columns over some categories."""

    def __init__(self, rows, codes=None, missing=()):
        self.rows, self.codes, self.queries = rows, CODES | (codes or {}), []
        self.names = [name for name in (*CATEGORIES.columns, *self.codes) if name not in missing]
        self.names = list(dict.fromkeys(self.names))

    def rollback(self):
        pass

    def cursor(self, name=None):
        source = self

        class Cursor:
            description, pending = None, []

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, query, params=None):
                source.queries.append(query)
                names = source.names if "LIMIT 0" in query else re.findall(r'"(\w+)"', query.split(" FROM ")[0])
                self.columns = [SimpleNamespace(name=n, type_code=source.codes.get(n, 25), precision=None, scale=None)
                                for n in names]
                self.pending = [] if "LIMIT 0" in query else [tuple(row.get(n) for n in names) for row in source.rows]
                self.description = self.columns

            def fetchmany(self, size):
                rows, self.pending = self.pending[:size], self.pending[size:]
                return rows

        return Cursor()


class IncrementalExtractTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)
        create_tables(self.catalog)

    def extract(self, table, source):
        return extract_table(self.catalog, source, table, AT, start="2026-11-20T00:00:00Z", end="2026-11-21T00:00:00Z")

    def test_incremental_skips_an_approved_column_bronze_lacks(self):
        approved = replace(CATEGORIES, columns=(*CATEGORIES.columns, "loyalty_tier"))
        source = Source([category(1) | {"loyalty_tier": "gold"}], codes={"loyalty_tier": 25})
        self.assertEqual(self.extract(approved, source), 1)
        self.assertNotIn("loyalty_tier", source.queries[-1])

    def test_incremental_refuses_a_dropped_column(self):
        write_staging(self.catalog, CATEGORIES, [page("categories", [category(9)])], AT)
        with self.assertRaises(SchemaChanged) as raised:
            self.extract(CATEGORIES, Source([category(1)], missing=("code",)))
        self.assertIn("dropped", str(raised.exception))
        self.assertEqual(staging_rows(self.catalog, CATEGORIES).read_all()["category_id"].to_pylist(), [9])

    def test_overflow_of_a_widened_value_names_the_sync(self):
        for layer in ("staging", "bronze"):
            self.catalog.drop_table((layer, "categories"))
        narrow = page("categories", []).schema
        narrow = narrow.set(narrow.get_field_index("parent_category_id"), pa.field("parent_category_id", pa.int32()))
        create_staging_and_bronze(self.catalog, CATEGORIES, pa.schema([*narrow, *LINEAGE_FIELDS]))
        source = Source([category(1) | {"parent_category_id": 2**40}])
        with self.assertRaises(SchemaChanged) as raised:
            self.extract(CATEGORIES, source)
        self.assertIn("lakehouse_oltp_sync", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
