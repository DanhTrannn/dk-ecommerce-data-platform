import unittest
from types import SimpleNamespace

import pyarrow as pa

from iceberg_fixture import AT, TABLES, category, page, sql_catalog
from lakehouse.oltp.staging import write_staging
from lakehouse.oltp.schema import MART_STATE, create_oltp_schema
from lakehouse.oltp.sources import SOURCE_TABLES

BOOLEAN, BIGINT, TEXT, TIMESTAMPTZ = 16, 20, 25, 1184


class Source:
    """Fake source connection that answers only the init step's `LIMIT 0` reads."""

    def __init__(self, extra=()):
        self.extra = extra

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
                self.description = [source.column(name) for name in (*table.columns, *source.extra)]

        return Cursor()

    @staticmethod
    def column(name):
        if name.endswith("_at"):
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
        self.assertEqual(self.catalog.load_table(MART_STATE).schema().column_names, ["mart", "watermark", "last_day"])

    def test_rerun_keeps_the_mart_state(self):
        create_oltp_schema(self.catalog, Source())
        state = self.catalog.load_table(MART_STATE)
        state.append(pa.table({"mart": ["mart_sales_daily"], "watermark": pa.array([AT], pa.timestamp("us", tz="UTC")),
                               "last_day": pa.array([AT.date()], pa.date32())}))
        create_oltp_schema(self.catalog, Source())
        self.assertEqual(self.catalog.load_table(MART_STATE).scan().to_arrow()["mart"].to_pylist(), ["mart_sales_daily"])

    def test_rerun_keeps_the_data_and_adds_new_source_columns(self):
        create_oltp_schema(self.catalog, Source())
        write_staging(self.catalog, TABLES["categories"], [page("categories", [category(1)])], AT)
        create_oltp_schema(self.catalog, Source(extra=("loyalty_tier",)))
        bronze = self.bronze("categories")
        self.assertIn("loyalty_tier", bronze.schema().column_names)
        self.assertEqual(bronze.scan().to_arrow().num_rows, 1)
        rows = page("categories", [category(2)]).append_column("loyalty_tier", pa.array(["gold"]))
        self.assertEqual(write_staging(self.catalog, TABLES["categories"], [rows], AT), 1)


if __name__ == "__main__":
    unittest.main()
