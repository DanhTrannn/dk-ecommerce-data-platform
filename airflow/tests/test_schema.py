import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa

from iceberg_fixture import AT, TABLES, category, page, sql_catalog
from lakehouse.oltp.staging import write_staging
from lakehouse.oltp import schema
from lakehouse.oltp.bronze import mark_deleted, merge_bronze
from lakehouse.oltp.drift import SchemaChanged
from lakehouse.oltp.schema import create_oltp_schema, evolve_schema
from lakehouse.oltp.sources import SOURCE_TABLES

BOOLEAN, BIGINT, TEXT, TIMESTAMPTZ = 16, 20, 25, 1184


class Source:
    """Fake source connection that answers only the init step's `LIMIT 0` reads."""

    def __init__(self, extra=(), missing=(), types=None):
        self.extra, self.missing, self.types = extra, missing, types or {}

    def set_session(self, **settings):
        pass

    def rollback(self):
        pass

    def cursor(self):
        source = self

        class Cursor:
            description = None

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, query):
                table = next(t for t in SOURCE_TABLES if f'FROM "public"."{t.name}"' in query)
                names = [name for name in (*table.columns, *source.extra) if name not in source.missing]
                self.description = [source.column(name) for name in names]

        return Cursor()

    def column(self, name):
        if name in self.types:
            code = self.types[name]
        elif name.endswith("_at"):
            code = TIMESTAMPTZ
        elif name.startswith("is_"):
            code = BOOLEAN
        elif (name.endswith("_id") and "public_id" not in name) or name.endswith("_vnd"):
            code = BIGINT
        else:
            code = TEXT
        return SimpleNamespace(name=name, type_code=code, precision=None, scale=None)


class CreateSchemaTests(unittest.TestCase):
    def setUp(self):
        self.catalog = sql_catalog(self)

    def bronze(self, name):
        return self.catalog.load_table(("staging", name))

    def test_creates_every_namespace_and_table(self):
        create_oltp_schema(self.catalog, Source())
        self.assertEqual(
            sorted(name for (name,) in self.catalog.list_namespaces()),
            ["bronze", "gold", "silver", "staging"],
        )
        for table in SOURCE_TABLES:
            staging = self.catalog.load_table(("staging", table.name))
            self.assertEqual(staging.schema().column_names, [*table.columns, "_ingested_at"])
            bronze = self.catalog.load_table(("bronze", table.name))
            self.assertEqual(bronze.schema().column_names, [*table.columns, "_ingested_at", "_is_deleted", "_deleted_at"])
        self.assertFalse(self.catalog.table_exists(("gold", "_mart_state")))

    def test_rerun_keeps_the_data_and_adds_approved_columns_only(self):
        create_oltp_schema(self.catalog, Source())
        write_staging(self.catalog, TABLES["categories"], [page("categories", [category(1)])], AT)
        approved = replace(TABLES["categories"], columns=(*TABLES["categories"].columns, "loyalty_tier"))
        tables = [approved if table.name == "categories" else table for table in SOURCE_TABLES]
        with patch.object(schema, "SOURCE_TABLES", tables):
            create_oltp_schema(self.catalog, Source(extra=("loyalty_tier", "unreviewed")))
        columns = self.bronze("categories").schema().column_names
        self.assertIn("loyalty_tier", columns)
        self.assertNotIn("unreviewed", columns)
        self.assertEqual(self.bronze("categories").scan().to_arrow().num_rows, 1)

    def test_evolve_adds_an_approved_column_to_staging_and_bronze(self):
        create_oltp_schema(self.catalog, Source())
        approved = replace(TABLES["categories"], columns=(*TABLES["categories"].columns, "loyalty_tier"))
        source = {name: pa.string() for name in approved.columns} | {"category_id": pa.int64(), "parent_category_id": pa.int64(),
                                                                     "is_active": pa.bool_(), "created_at": pa.timestamp("us", tz="UTC"),
                                                                     "updated_at": pa.timestamp("us", tz="UTC")}
        drift = evolve_schema(self.catalog, approved, source)
        self.assertEqual(drift.approved, ("loyalty_tier",))
        for layer in ("staging", "bronze"):
            self.assertIn("loyalty_tier", self.catalog.load_table((layer, "categories")).schema().column_names)
        self.assertEqual(evolve_schema(self.catalog, approved, source).evolve, ())

    def test_bronze_accepts_upserts_after_a_column_is_added(self):
        create_oltp_schema(self.catalog, Source())
        write_staging(self.catalog, TABLES["categories"], [page("categories", [category(1)])], AT)
        merge_bronze(self.catalog, TABLES["categories"], full=True)
        approved = replace(TABLES["categories"], columns=(*TABLES["categories"].columns, "loyalty_tier"))
        evolve_schema(self.catalog, approved, {f.name: f.type for f in page("categories", []).schema} | {"loyalty_tier": pa.string()})
        self.assertEqual(mark_deleted(self.catalog, approved, pa.array([1]), AT, AT), 1)

    def test_a_failed_rewrite_after_adding_a_column_is_finished_by_a_retry(self):
        create_oltp_schema(self.catalog, Source())
        write_staging(self.catalog, TABLES["categories"], [page("categories", [category(1)])], AT)
        merge_bronze(self.catalog, TABLES["categories"], full=True)
        approved = replace(TABLES["categories"], columns=(*TABLES["categories"].columns, "loyalty_tier"))
        columns = {f.name: f.type for f in page("categories", []).schema} | {"loyalty_tier": pa.string()}
        with patch("pyiceberg.table.Transaction.overwrite", side_effect=OSError("storage down")):
            with self.assertRaises(OSError):
                evolve_schema(self.catalog, approved, columns)
        evolve_schema(self.catalog, approved, columns)
        self.assertEqual(mark_deleted(self.catalog, approved, pa.array([1]), AT, AT), 1)

    def test_evolve_widens_a_column(self):
        create_oltp_schema(self.catalog, Source(types={"boom_count": 23}))
        self.assertEqual(str(self.bronze("customers").schema().find_field("boom_count").field_type), "int")
        create_oltp_schema(self.catalog, Source(types={"boom_count": 20}))
        for layer in ("staging", "bronze"):
            field = self.catalog.load_table((layer, "customers")).schema().find_field("boom_count")
            self.assertEqual(str(field.field_type), "long")

    def test_init_refuses_a_dropped_column_without_altering(self):
        create_oltp_schema(self.catalog, Source())
        before = self.bronze("categories").schema()
        with self.assertRaises(SchemaChanged) as raised:
            create_oltp_schema(self.catalog, Source(missing=("code",)))
        self.assertIn("dropped ['code']", str(raised.exception))
        self.assertEqual(self.bronze("categories").schema(), before)


if __name__ == "__main__":
    unittest.main()
