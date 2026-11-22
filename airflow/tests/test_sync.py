import re
import unittest
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pyarrow as pa
from unittest.mock import patch

from iceberg_fixture import AT, TABLES, category, create_tables, page, sql_catalog
from lakehouse.oltp.bronze import mark_deleted, merge_bronze
from lakehouse.oltp import sync
from lakehouse.oltp.drift import SchemaChanged
from lakehouse.oltp.schema import evolve_schema
from lakehouse.oltp.staging import staging_rows, write_staging
from lakehouse.oltp.sync import detect_deletes, load_bronze, sync_schema

CATEGORIES = TABLES["categories"]
TYPES = {"category_id": 20, "parent_category_id": 20, "is_active": 16, "created_at": 1184, "updated_at": 1184}
APPROVED = replace(CATEGORIES, columns=(*CATEGORIES.columns, "loyalty_tier"))


class Source:
    """Fake source holding some categories, with the given extra columns; reads columns, keys and rows."""

    def __init__(self, rows, extra=()):
        self.rows = {row["category_id"]: row for row in rows}
        self.names = [*CATEGORIES.columns, *extra]

    def rollback(self):
        pass

    def cursor(self, name=None):
        return Cursor(self)


class Cursor:
    def __init__(self, source):
        self.source, self.pending, self.columns, self.description = source, [], [], None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        rows = [self.source.rows[key] for key in sorted(self.source.rows)]
        if "LIMIT 0" in query:
            names, rows = self.source.names, []
        elif "IS NOT NULL LIMIT 1" in query:
            column = re.search(r'WHERE "(\w+)" IS NOT NULL', query).group(1)
            names, rows = ["one"], [{"one": 1} for row in rows if row.get(column) is not None][:1]
        else:
            names = re.findall(r'"(\w+)"', query.split(" FROM ")[0])
            if params is not None:
                rows = [row for row in rows if row["category_id"] in params["keys"]]
        self.columns = [SimpleNamespace(name=n, type_code=TYPES.get(n, 25), precision=None, scale=None) for n in names]
        self.description = self.columns
        self.pending = [tuple(row.get(name) for name in names) for row in rows]

    def fetchone(self):
        return self.pending[0] if self.pending else None

    def fetchmany(self, size):
        rows, self.pending = self.pending[:size], self.pending[size:]
        return rows


class DetectTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)
        create_tables(self.catalog)
        write_staging(self.catalog, CATEGORIES, [page("categories", [category(1), category(2), category(3)])], AT)
        merge_bronze(self.catalog, CATEGORIES, full=True)
        mark_deleted(self.catalog, CATEGORIES, page("categories", [category(3)])["category_id"], AT, AT)
        self.source = Source([category(2), category(3), category(4)])
        self.detected_at = AT + timedelta(days=1)

    def bronze(self):
        rows = self.catalog.load_table(("bronze", "categories")).scan().to_arrow().to_pylist()
        return {row["category_id"]: (row["_is_deleted"], row["_deleted_at"]) for row in rows}

    def test_detect_flags_vanished_rows_and_stages_returned_ones(self):
        result = detect_deletes(self.catalog, self.source, CATEGORIES, self.detected_at)
        self.assertEqual(result, {"deleted": 1, "restored": 2, "reload": []})
        self.assertEqual(self.bronze()[1], (True, self.detected_at))
        staged = staging_rows(self.catalog, CATEGORIES, ("category_id",)).read_all()["category_id"].to_pylist()
        self.assertEqual(sorted(staged), [3, 4])

    def test_detect_rerun_changes_nothing(self):
        detect_deletes(self.catalog, self.source, CATEGORIES, self.detected_at)
        merge_bronze(self.catalog, CATEGORIES, full=False)
        self.assertEqual(
            detect_deletes(self.catalog, self.source, CATEGORIES, self.detected_at), {"deleted": 0, "restored": 0, "reload": []}
        )
        self.assertEqual(self.bronze(), {1: (True, self.detected_at), 2: (False, None), 3: (False, None), 4: (False, None)})


    def staged(self):
        return sorted(staging_rows(self.catalog, CATEGORIES, ("category_id",)).read_all()["category_id"].to_pylist())

    def approve(self, tier):
        """Add the approved column to staging and Bronze, as the schema task does, and return a source holding it."""
        columns = {field.name: field.type for field in page("categories", []).schema} | {"loyalty_tier": pa.string()}
        evolve_schema(self.catalog, APPROVED, columns)
        return Source([category(2) | {"loyalty_tier": tier}, category(3), category(4)], extra=("loyalty_tier",))

    def test_sync_schema_applies_safe_changes_then_fails_on_unlisted(self):
        source = Source([category(2)], extra=("loyalty_tier", "metadata"))
        with self.assertRaises(SchemaChanged) as raised:
            sync_schema(self.catalog, source, APPROVED)
        self.assertIn("unlisted ['metadata']", str(raised.exception))
        self.assertIn("loyalty_tier", self.catalog.load_table(("bronze", "categories")).schema().column_names)

    def test_detect_reloads_a_table_whose_new_column_is_all_null_in_bronze(self):
        source = self.approve("gold")
        result = detect_deletes(self.catalog, source, APPROVED, self.detected_at)
        self.assertEqual(result["reload"], ["loyalty_tier"])
        self.assertEqual(self.staged(), [2, 3, 4])

    def bronze_tiers(self):
        rows = self.catalog.load_table(("bronze", "categories")).scan().to_arrow().to_pylist()
        return {row["category_id"]: (row["loyalty_tier"], row["_is_deleted"]) for row in rows}

    def test_reload_replaces_bronze_in_one_commit_then_stops(self):
        source = self.approve("gold")
        detect_deletes(self.catalog, source, APPROVED, self.detected_at)
        load_bronze(self.catalog, APPROVED)
        self.assertEqual(self.bronze_tiers(), {1: (None, True), 2: ("gold", False), 3: (None, False), 4: (None, False)})
        self.assertEqual(detect_deletes(self.catalog, source, APPROVED, self.detected_at)["reload"], [])

    def test_reload_is_retried_until_the_bronze_load_succeeds(self):
        source = self.approve("gold")
        detect_deletes(self.catalog, source, APPROVED, self.detected_at)
        with patch.object(sync, "merge_bronze", side_effect=RuntimeError("load failed")):
            with self.assertRaises(RuntimeError):
                load_bronze(self.catalog, APPROVED)
        self.assertEqual(detect_deletes(self.catalog, source, APPROVED, self.detected_at)["reload"], ["loyalty_tier"])
        self.assertEqual(self.staged(), [2, 3, 4])

    def test_reload_needs_no_source_query(self):
        source = self.approve(None)
        detect_deletes(self.catalog, source, APPROVED, self.detected_at)
        load_bronze(self.catalog, APPROVED)
        self.assertEqual(detect_deletes(self.catalog, source, APPROVED, self.detected_at)["reload"], [])


if __name__ == "__main__":
    unittest.main()
