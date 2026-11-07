import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from platform_app.api.db.base import Base
from platform_app.api.db.schema import render_schema
from platform_app.api.db.types import GUID
from platform_app.api.core.config import Settings


class SchemaContractTests(unittest.TestCase):
    def test_bootstrap_is_reproducible_and_matches_models(self):
        path = Path(__file__).resolve().parents[5] / "database" / "schema.sql"
        self.assertEqual(path.read_text(), render_schema())
        self.assertEqual(render_schema(), render_schema())
        self.assertEqual(len(Base.metadata.tables), 28)
        self.assertEqual(sum(len(t.columns) for t in Base.metadata.tables.values()), 316)

    def test_postgres_ddl_preserves_all_foreign_keys(self):
        sql = render_schema()
        self.assertEqual(
            sql.count(" FOREIGN KEY("), sum(len(t.foreign_key_constraints) for t in Base.metadata.tables.values())
        )
        self.assertIn("staff_id) REFERENCES customers", sql)
        for forbidden in ("AUTO_INCREMENT", "UNHEX(", "ENGINE=", " ON UPDATE CURRENT_TIMESTAMP", "BINARY(16)"):
            self.assertNotIn(forbidden, sql)
        for table in Base.metadata.tables.values():
            self.assertIn("CREATE TABLE", str(CreateTable(table).compile(dialect=postgresql.dialect())))

    def test_uuid_accepts_string_and_native_values(self):
        value = uuid.uuid4()
        column = GUID()
        for data in (str(value), value):
            self.assertEqual(column.process_bind_param(data, None), value)
            self.assertEqual(column.process_result_value(data, None), value)
        self.assertIsNone(column.process_bind_param(None, None))
        self.assertIsNone(column.process_result_value(None, None))
        with self.assertRaises(ValueError):
            column.process_bind_param("not-a-uuid", None)

    def test_api_database_url_is_required_without_secret_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValidationError) as error:
                Settings(_env_file=None, secret_key=uuid.uuid4().hex)
            self.assertIn("database_url", {entry["loc"][0] for entry in error.exception.errors()})


if __name__ == "__main__":
    unittest.main()
