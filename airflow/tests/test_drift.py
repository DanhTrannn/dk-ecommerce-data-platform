import unittest
from types import SimpleNamespace

import pyarrow as pa

from iceberg_fixture import TABLES, page
from lakehouse.oltp.drift import Drift, classify, source_columns
from lakehouse.oltp.sources import SOURCE_TABLES

CATEGORIES = TABLES["categories"]


def bronze():
    return pa.schema([*page("categories", []).schema, pa.field("_ingested_at", pa.timestamp("us", tz="UTC"))])


def source(**changes):
    columns = {field.name: field.type for field in page("categories", []).schema}
    columns.update(changes)
    return {name: kind for name, kind in columns.items() if kind is not False}


class ClassifyTests(unittest.TestCase):
    def test_identical_schemas_have_no_drift(self):
        drift = classify(CATEGORIES, source(), bronze())
        self.assertEqual(drift, Drift())
        self.assertEqual((drift.breaking, drift.evolve), ((), ()))

    def test_new_source_column_is_unlisted(self):
        self.assertEqual(classify(CATEGORIES, source(loyalty_tier=None), bronze()), Drift(unlisted=("loyalty_tier",)))

    def test_excluded_column_is_not_reported(self):
        staff = TABLES["delivery_staff"]
        columns = {name: pa.string() for name in staff.columns} | {"phone": None}
        schema = pa.schema([pa.field(name, pa.string()) for name in staff.columns])
        self.assertEqual(classify(staff, columns, schema), Drift())

    def test_allowlisted_column_missing_from_bronze_is_approved(self):
        schema = bronze().remove(bronze().get_field_index("code"))
        self.assertEqual(classify(CATEGORIES, source(), schema), Drift(approved=("code",)))

    def test_long_over_int_is_widened(self):
        schema = bronze().set(bronze().get_field_index("parent_category_id"), pa.field("parent_category_id", pa.int32()))
        drift = classify(CATEGORIES, source(), schema)
        self.assertEqual(drift, Drift(widened=("parent_category_id",)))
        self.assertEqual(drift.evolve, ("parent_category_id",))

    def test_int_under_long_is_narrowed(self):
        self.assertEqual(classify(CATEGORIES, source(parent_category_id=pa.int32()), bronze()), Drift(narrowed=("parent_category_id",)))

    def test_missing_allowlisted_column_is_dropped(self):
        drift = classify(CATEGORIES, source(code=False), bronze())
        self.assertEqual(drift, Drift(dropped=("code",)))
        self.assertEqual(drift.breaking, ("code",))

    def test_string_over_long_is_incompatible(self):
        self.assertEqual(classify(CATEGORIES, source(parent_category_id=pa.string()), bronze()), Drift(incompatible=("parent_category_id",)))

    def test_unmappable_type_of_an_unlisted_column_does_not_fail(self):
        description = [SimpleNamespace(name=name, type_code=20, precision=None, scale=None) for name in ("category_id",)]
        description.append(SimpleNamespace(name="metadata", type_code=3802, precision=None, scale=None))

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, query):
                self.description = description

        columns = source_columns(SimpleNamespace(cursor=lambda: Cursor()), CATEGORIES)
        self.assertEqual(columns, {"category_id": pa.int64(), "metadata": None})

    def test_excluded_lists_the_seventeen_columns(self):
        self.assertEqual(sum(len(table.excluded) for table in SOURCE_TABLES), 17)
        for table in SOURCE_TABLES:
            self.assertFalse(set(table.excluded) & set(table.columns), table.name)


if __name__ == "__main__":
    unittest.main()
