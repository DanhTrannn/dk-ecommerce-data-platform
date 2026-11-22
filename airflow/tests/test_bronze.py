import unittest
from datetime import timedelta

import pyarrow as pa

from iceberg_fixture import AT, TABLES, category, create_tables, page, sql_catalog
from lakehouse.oltp.bronze import live_keys, mark_deleted, merge_bronze
from lakehouse.oltp.staging import write_staging

CATEGORIES = TABLES["categories"]


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)
        create_tables(self.catalog)

    def load(self, rows, *, full, at=AT):
        write_staging(self.catalog, CATEGORIES, [page("categories", rows)], at)
        return merge_bronze(self.catalog, CATEGORIES, full=full)

    def flags(self):
        return {row["category_id"]: (row["_is_deleted"], row["_deleted_at"]) for row in self.rows()}

    def rows(self):
        return sorted(self.catalog.load_table(("bronze", "categories")).scan().to_arrow().to_pylist(), key=lambda r: r["category_id"])

    def test_full_load_replaces_the_live_rows(self):
        self.load([category(9)], full=True)
        self.assertEqual(self.load([category(1), category(2)], full=True), 2)
        live = [row["category_id"] for row in self.rows() if not row["_is_deleted"]]
        self.assertEqual(live, [1, 2])

    def test_incremental_load_keeps_other_rows_and_updates_newer_versions(self):
        self.load([category(1, name="a"), category(2)], full=True)
        self.load([category(1, name="b", later=True), category(3)], full=False)
        self.assertEqual([(row["category_id"], row["name"]) for row in self.rows()], [(1, "b"), (2, "cat-2"), (3, "cat-3")])

    def test_rerun_of_an_incremental_load_writes_nothing_new(self):
        self.load([category(1)], full=True)
        self.load([category(2)], full=False)
        self.assertEqual(self.load([category(2)], full=False), 0)
        self.assertEqual(len(self.rows()), 2)

    def test_source_values_are_kept_as_the_source_holds_them(self):
        self.load([category(1)], full=True)
        self.assertIsInstance(self.rows()[0]["public_id"], str)


    def test_loaded_rows_are_live(self):
        self.load([category(1), category(2)], full=True)
        self.assertEqual(self.flags(), {1: (False, None), 2: (False, None)})

    def test_mark_deleted_flags_rows_and_keeps_the_first_detection_time(self):
        self.load([category(1), category(2)], full=True)
        self.assertEqual(mark_deleted(self.catalog, CATEGORIES, pa.array([1]), AT, AT), 1)
        self.assertEqual(self.flags()[1], (True, AT))
        self.assertEqual(live_keys(self.catalog, CATEGORIES).to_pylist(), [2])
        later = AT + timedelta(days=1)
        mark_deleted(self.catalog, CATEGORIES, pa.array([1]), later, later)
        self.assertEqual(live_keys(self.catalog, CATEGORIES).to_pylist(), [2])
        self.assertEqual(self.flags()[1], (True, AT))

    def test_restored_row_with_the_same_cursor_comes_back_live(self):
        self.load([category(1)], full=True)
        mark_deleted(self.catalog, CATEGORIES, pa.array([1]), AT, AT)
        self.assertEqual(self.load([category(1)], full=False), 1)
        self.assertEqual(self.flags(), {1: (False, None)})

    def test_full_load_keeps_tombstones_and_flags_rows_missing_from_the_source(self):
        self.load([category(1), category(2), category(3)], full=True)
        mark_deleted(self.catalog, CATEGORIES, pa.array([1]), AT, AT)
        later = AT + timedelta(days=1)
        self.load([category(3)], full=True, at=later)
        self.assertEqual(self.flags(), {1: (True, AT), 2: (True, later), 3: (False, None)})


if __name__ == "__main__":
    unittest.main()
