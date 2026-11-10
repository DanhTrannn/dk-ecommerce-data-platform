"""Offline contracts for the optional Python migration pilot."""

import importlib.util
import os
import tempfile
import unittest
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

AVAILABLE = all(importlib.util.find_spec(name) for name in ("polars", "pyiceberg", "psycopg2"))


@unittest.skipUnless(AVAILABLE, "install batch-pipeline[polars] for migration contracts")
class PolarsPilotTests(unittest.TestCase):
    def setUp(self):
        import polars as pl
        import pyarrow as pa
        from lakehouse.oltp.polars_pilot import SOURCE_SCHEMA

        self.pl, self.pa = pl, pa
        self.now = datetime(2026, 1, 2, tzinfo=UTC)
        self.public_id = uuid.uuid4()
        self.row = dict(
            category_id=1,
            public_id=str(self.public_id),
            parent_category_id=None,
            code="sample",
            name="Tiếng Việt",
            is_active=True,
            created_at=self.now,
            updated_at=self.now,
        )
        self.source = pa.Table.from_pylist([self.row], schema=SOURCE_SCHEMA)

    def test_namespace_guard_excludes_serving_and_path_injection(self):
        from lakehouse.oltp.polars_pilot import validate_namespace

        validate_namespace("polars_pilot_acceptance")
        for value in ("bronze", "silver", "gold", "polars_pilot_../gold", "polars_pilot_"):
            with self.assertRaises(ValueError):
                validate_namespace(value)

    def test_uuid_nullable_and_timestamp_contract(self):
        from lakehouse.oltp.polars_pilot import SILVER_SCHEMA, silver_candidates

        result = silver_candidates(self.source, self.pl.DataFrame(), "test", self.now)
        self.assertEqual(result.schema, SILVER_SCHEMA)
        row = result.to_pylist()[0]
        self.assertEqual(row["public_id"], self.public_id.bytes)
        self.assertIsNone(row["parent_category_id"])
        self.assertIsNone(row["slug"])
        self.assertEqual(row["updated_at"], self.now)
        self.assertEqual(row["name"], "Tiếng Việt")

    def test_older_backfill_cannot_replace_newer_state(self):
        from lakehouse.oltp.polars_pilot import silver_candidates

        current = self.pl.DataFrame({"category_id": [1], "updated_at": [self.now + timedelta(days=1)]})
        self.assertEqual(silver_candidates(self.source, current, "old", self.now).num_rows, 0)
        current = current.with_columns(self.pl.lit(self.now).alias("updated_at"))
        self.assertEqual(silver_candidates(self.source, current, "same", self.now).num_rows, 1)

    def test_duplicate_delta_uses_latest_cursor(self):
        from lakehouse.oltp.polars_pilot import SOURCE_SCHEMA, silver_candidates

        rows = [dict(self.row, name="new", updated_at=self.now + timedelta(hours=1)), self.row]
        result = silver_candidates(
            self.pa.Table.from_pylist(rows, schema=SOURCE_SCHEMA), self.pl.DataFrame(), "run", self.now
        )
        self.assertEqual(result.num_rows, 1)
        self.assertEqual(result.to_pylist()[0]["name"], "new")

    def test_invalid_category_fails_before_silver_write(self):
        from lakehouse.oltp.polars_pilot import SOURCE_SCHEMA, silver_candidates

        for name in ("category_id", "code", "name"):
            row = dict(self.row, **{name: None})
            with self.assertRaises(ValueError):
                silver_candidates(
                    self.pa.Table.from_pylist([row], schema=SOURCE_SCHEMA), self.pl.DataFrame(), "run", self.now
                )

    def test_empty_page_keeps_typed_lineage_schema(self):
        from lakehouse.oltp.polars_pilot import BRONZE_SCHEMA, SOURCE_SCHEMA, bronze_page

        page = bronze_page(self.pa.Table.from_pylist([], schema=SOURCE_SCHEMA), self.now)
        self.assertEqual(page.schema, BRONZE_SCHEMA)
        self.assertEqual(page.num_rows, 0)

    def test_catalog_credentials_are_read_as_data_not_executed(self):
        from lakehouse.utils.iceberg import catalog_properties

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clients.env"
            path.write_text("POLARIS_SPARK_CLIENT_ID=batch\nPOLARIS_SPARK_CLIENT_SECRET=$(do-not-execute)\n")
            with patch.dict(
                os.environ,
                dict(
                    POLARIS_CREDENTIAL_FILE=str(path),
                    POLARIS_CATALOG_URI="http://polaris:8181/api/catalog",
                    POLARIS_CATALOG_NAME="lakehouse",
                    MINIO_ENDPOINT="http://minio:9000",
                    MINIO_SPARK_ACCESS_KEY="writer",
                    MINIO_SPARK_SECRET_KEY="private",
                    POLARIS_REALM="custom",
                ),
                clear=True,
            ):
                properties = catalog_properties()
        self.assertEqual(properties["credential"], "batch:$(do-not-execute)")
        self.assertEqual(properties["header.Polaris-Realm"], "custom")
        self.assertEqual(properties["s3.access-key-id"], "writer")
