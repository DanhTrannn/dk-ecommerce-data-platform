import unittest

from iceberg_fixture import AT, TABLES, category, create_tables, page, sql_catalog
from lakehouse.oltp.bronze import merge_bronze
from lakehouse.oltp.staging import write_staging

CATEGORIES = TABLES["categories"]


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)
        create_tables(self.catalog)

    def load(self, rows, *, full):
        write_staging(self.catalog, CATEGORIES, [page("categories", rows)], AT)
        return merge_bronze(self.catalog, CATEGORIES, full=full)

    def rows(self):
        return sorted(self.catalog.load_table(("bronze", "categories")).scan().to_arrow().to_pylist(), key=lambda r: r["category_id"])

    def test_full_load_replaces_the_table(self):
        self.load([category(9)], full=True)
        self.assertEqual(self.load([category(1), category(2)], full=True), 2)
        self.assertEqual([row["category_id"] for row in self.rows()], [1, 2])

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


if __name__ == "__main__":
    unittest.main()
