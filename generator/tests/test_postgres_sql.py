import tempfile
import unittest
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from generator.config import load_config
from generator.sql_export import _sql_literal, export_sql


class PostgresSQLTests(unittest.TestCase):
    def test_native_uuid_boolean_and_safe_text_literals(self):
        value = uuid.uuid4()
        self.assertEqual(_sql_literal(value), f"'{value}'::uuid")
        self.assertEqual(_sql_literal(True), "TRUE")
        self.assertEqual(_sql_literal(False), "FALSE")
        self.assertEqual(_sql_literal("O'Brien\\100%\nảnh\r\t;"), "E'O''Brien\\\\100%\\nảnh\\r\\t;'")
        with self.assertRaises(ValueError):
            _sql_literal("bad\x00text")

    def test_timestamp_is_utc_and_independent_of_host_timezone(self):
        value = datetime(2026, 7, 1, 7, tzinfo=timezone(timedelta(hours=7)))
        self.assertEqual(_sql_literal(value), _sql_literal(value.astimezone(UTC)))
        self.assertEqual(_sql_literal(value), _sql_literal(datetime(2026, 7, 1)))

    def test_artifact_has_transaction_verification_and_identity_restart(self):
        config = replace(
            load_config(Path(__file__).resolve().parents[1] / "configs/small.yml"),
            scale=dict(customers=10, products=2, variants=8, orders=30),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.sql"
            export_sql(config, path)
            sql = path.read_text()
        self.assertIn('"sql_format": "postgresql-v1"', sql)
        self.assertIn('INSERT INTO public."customers"', sql)
        self.assertIn("DO $tlcn_verify$", sql)
        self.assertIn("ALTER SEQUENCE %s RESTART WITH %s", sql)
        self.assertTrue(sql.endswith("COMMIT;\n"))
        for forbidden in ("UNHEX(", "START TRANSACTION", "SET NAMES", "`", "setval("):
            self.assertNotIn(forbidden, sql)
