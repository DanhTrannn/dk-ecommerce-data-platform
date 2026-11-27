import unittest
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from iceberg_fixture import AT, TABLES, category, create_tables, page, sql_catalog
from lakehouse.oltp.bronze import (
    RELOAD_PENDING,
    filter_current,
    live_keys,
    mark_deleted,
    merge_bronze,
    overwrite_bronze,
    purge_bronze,
    reload_pending,
)
from lakehouse.oltp.sources import SOURCE_TABLES
from lakehouse.oltp.staging import write_staging

CATEGORIES = TABLES["categories"]
NOW = datetime(2026, 1, 1, tzinfo=UTC)


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


    def test_purge_bronze_removes_only_flagged_rows(self):
        self.load([category(1), category(2), category(3)], full=True)
        mark_deleted(self.catalog, CATEGORIES, pa.array([2]), AT, AT)
        self.assertEqual(purge_bronze(self.catalog, CATEGORIES), 1)
        self.assertEqual(self.flags(), {1: (False, None), 3: (False, None)})

    def test_purge_bronze_twice_deletes_nothing_the_second_time(self):
        self.load([category(1), category(2)], full=True)
        mark_deleted(self.catalog, CATEGORIES, pa.array([2]), AT, AT)
        purge_bronze(self.catalog, CATEGORIES)
        self.assertEqual(purge_bronze(self.catalog, CATEGORIES), 0)


class OverwriteBronzeTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)
        create_tables(self.catalog)

    def overwrite(self, rows, at=AT):
        write_staging(self.catalog, CATEGORIES, [page("categories", rows)], at)
        return overwrite_bronze(self.catalog, CATEGORIES)

    def table(self):
        return self.catalog.load_table(("bronze", "categories"))

    def rows(self):
        return {row["category_id"]: row for row in self.table().scan().to_arrow().to_pylist()}

    def test_reference_tables_are_full_load(self):
        self.assertEqual(
            {table.name for table in SOURCE_TABLES if table.full_load},
            {"categories", "cities", "stores", "coupons", "delivery_staff", "products", "product_variants"},
        )

    def test_overwrite_into_an_empty_table(self):
        self.assertEqual(self.overwrite([category(1)]), 1)
        self.assertEqual(self.rows()[1]["_ingested_at"], AT)

    def test_overwrite_keeps_the_time_of_unchanged_rows(self):
        self.overwrite([category(1), category(2)])
        self.overwrite([category(1), category(2)], at=AT + timedelta(days=1))
        self.assertEqual({key: row["_ingested_at"] for key, row in self.rows().items()}, {1: AT, 2: AT})

    def test_overwrite_gives_changed_rows_the_new_time(self):
        later = AT + timedelta(days=1)
        self.overwrite([category(1), category(2)])
        self.overwrite([category(1, name="b", later=True), category(2)], at=later)
        rows = self.rows()
        self.assertEqual((rows[1]["name"], rows[1]["_ingested_at"]), ("b", later))
        self.assertEqual(rows[2]["_ingested_at"], AT)

    def test_overwrite_gives_a_filled_null_the_new_time(self):
        later = AT + timedelta(days=1)
        self.overwrite([dict(category(1), parent_category_id=None)])
        self.overwrite([dict(category(1), parent_category_id=7)], at=later)
        self.assertEqual(self.rows()[1]["_ingested_at"], later)

    def test_overwrite_drops_rows_missing_from_the_source(self):
        write_staging(self.catalog, CATEGORIES, [page("categories", [category(1), category(2)])], AT)
        merge_bronze(self.catalog, CATEGORIES, full=True)
        mark_deleted(self.catalog, CATEGORIES, pa.array([2]), AT, AT)
        self.overwrite([category(1)])
        rows = self.rows()
        self.assertEqual(list(rows), [1])
        self.assertEqual((rows[1]["_is_deleted"], rows[1]["_deleted_at"]), (False, None))

    def test_overwrite_with_an_empty_source_empties_the_table(self):
        self.overwrite([category(1)])
        self.assertEqual(self.overwrite([]), 0)
        self.assertEqual(self.rows(), {})

    def test_overwrite_rerun_gives_the_same_table(self):
        self.overwrite([category(1), category(2, name="x")])
        first = self.rows()
        self.overwrite([category(1), category(2, name="x")], at=AT + timedelta(days=1))
        self.assertEqual(self.rows(), first)

    def test_overwrite_clears_reload_pending(self):
        self.overwrite([category(1)])
        with self.table().transaction() as transaction:
            transaction.set_properties({RELOAD_PENDING: "name"})
        self.overwrite([category(1)])
        self.assertEqual(reload_pending(self.table()), [])


class FilterCurrentTests(unittest.TestCase):
    def test_older_cursor_is_rejected_and_null_target_can_be_filled(self):
        table = TABLES["orders"]
        delta = pa.Table.from_pylist([dict(order_id=1, updated_at=NOW)])
        current = pa.Table.from_pylist([dict(order_id=1, updated_at=NOW + timedelta(days=1))])
        self.assertEqual(filter_current(delta, current, table).num_rows, 0)
        current = pa.Table.from_arrays(
            [pa.array([1]), pa.array([None], type=pa.timestamp("us", tz="UTC"))],
            names=["order_id", "updated_at"],
        )
        self.assertEqual(filter_current(delta, current, table).num_rows, 1)

    def test_same_cursor_with_changed_values_is_kept(self):
        table = TABLES["orders"]
        delta = pa.Table.from_pylist([dict(order_id=1, updated_at=NOW, status="completed")])
        current = pa.Table.from_pylist([dict(order_id=1, updated_at=NOW, status="confirmed")])
        self.assertEqual(filter_current(delta, current, table).num_rows, 1)
        self.assertEqual(filter_current(delta, delta, table).num_rows, 0)


if __name__ == "__main__":
    unittest.main()
