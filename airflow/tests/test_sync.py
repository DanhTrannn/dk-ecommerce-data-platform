import unittest
from datetime import timedelta
from types import SimpleNamespace

from iceberg_fixture import AT, TABLES, category, create_tables, page, sql_catalog
from lakehouse.oltp.bronze import mark_deleted, merge_bronze
from lakehouse.oltp.staging import staging_rows, write_staging
from lakehouse.oltp.sync import detect_deletes

CATEGORIES = TABLES["categories"]
TYPES = {"category_id": 20, "parent_category_id": 20, "is_active": 16, "created_at": 1184, "updated_at": 1184}


class Source:
    """Fake source holding some categories; answers the key read and the read by keys."""

    def __init__(self, rows):
        self.rows = {row["category_id"]: row for row in rows}

    def rollback(self):
        pass

    def cursor(self, name=None):
        return Cursor(self.rows)


class Cursor:
    def __init__(self, rows):
        self.source, self.pending, self.columns, self.description = rows, [], [], None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        names = [CATEGORIES.pk] if params is None else list(CATEGORIES.columns)
        keys = sorted(self.source) if params is None else [key for key in params["keys"] if key in self.source]
        self.columns = [SimpleNamespace(name=n, type_code=TYPES.get(n, 25), precision=None, scale=None) for n in names]
        self.pending = [tuple(self.source[key].get(name) for name in names) for key in keys]

    def fetchmany(self, size):
        self.description = self.columns
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
        self.assertEqual(result, {"deleted": 1, "restored": 2})
        self.assertEqual(self.bronze()[1], (True, self.detected_at))
        staged = staging_rows(self.catalog, CATEGORIES, ("category_id",)).read_all()["category_id"].to_pylist()
        self.assertEqual(sorted(staged), [3, 4])

    def test_detect_rerun_changes_nothing(self):
        detect_deletes(self.catalog, self.source, CATEGORIES, self.detected_at)
        merge_bronze(self.catalog, CATEGORIES, full=False)
        self.assertEqual(detect_deletes(self.catalog, self.source, CATEGORIES, self.detected_at), {"deleted": 0, "restored": 0})
        self.assertEqual(self.bronze(), {1: (True, self.detected_at), 2: (False, None), 3: (False, None), 4: (False, None)})


if __name__ == "__main__":
    unittest.main()
