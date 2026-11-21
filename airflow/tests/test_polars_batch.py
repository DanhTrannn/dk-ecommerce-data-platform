import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pyarrow as pa

from iceberg_fixture import guessed_schema

from lakehouse.oltp.bronze import filter_current
from lakehouse.oltp.source import arrow_source_schema
from lakehouse.oltp.sources import SOURCE_TABLES

TABLES = {table.name: table for table in SOURCE_TABLES}
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def typed(table, row):
    return pl.from_arrow(pa.Table.from_pylist([row], schema=guessed_schema(table)))


class PolarsBatchTests(unittest.TestCase):
    def test_silver_models_select_every_source_column(self):
        self.assertEqual(len(SOURCE_TABLES), 19)
        for table in SOURCE_TABLES:
            sql = (Path(__file__).parents[2] / f"dbt/models/silver/{table.name}.sql").read_text()
            self.assertIn(f"unique_key='{table.pk}'", sql)
            self.assertIn(f"source('bronze', '{table.name}')", sql)
            for column in [*table.columns, "_ingested_at"]:
                self.assertIn(f" AS {column}", sql, f"{table.name}.{column}")

    def test_every_silver_model_is_tested_against_bronze(self):
        tests = (Path(__file__).parents[2] / "dbt/models/silver/silver.yml").read_text()
        for table in SOURCE_TABLES:
            self.assertIn(f"key: {table.pk}\n            bronze: source('bronze', '{table.name}')", tests, table.name)

    def test_dbt_sources_use_plain_table_names(self):
        text = (Path(__file__).parents[2] / "dbt/models/sources.yml").read_text()
        self.assertNotIn("identifier: silver_", text)
        for table in SOURCE_TABLES:
            self.assertIn(f"- name: {table.name}\n", text)

    def test_empty_source_schema_uses_postgres_types_not_inferred_nulls(self):
        fields = [
            SimpleNamespace(name="id", type_code=20),
            SimpleNamespace(name="value", type_code=2950),
            SimpleNamespace(name="changed", type_code=1114),
        ]
        schema = arrow_source_schema(fields)
        self.assertEqual(schema.types, [pa.int64(), pa.string(), pa.timestamp("us", tz="UTC")])
        with self.assertRaises(ValueError):
            arrow_source_schema([SimpleNamespace(name="unsupported", type_code=999999)])

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
