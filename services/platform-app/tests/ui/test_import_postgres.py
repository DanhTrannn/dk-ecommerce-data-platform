"""Real import worker and importer, only against disposable PostgreSQL."""

import os
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import text

from api.db.postgres_fixture import PostgresFixture
from platform_app.api.db.base import Base
from platform_app.api.db.import_dataset import import_dataset
from platform_app.ui.artifacts import collect_artifacts
from platform_app.ui.import_store import ImportStore, target_for
from platform_app.ui.import_worker import run_import

from .import_fixture import generated_job


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_SCHEMA_TESTS") == "1", "Enable isolated PostgreSQL tests")
class ConsoleImportPostgresTests(PostgresFixture):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="tlcn-import-tests-")
        self.addCleanup(temporary.cleanup)
        self.jobs, self.job, self.sql = generated_job(Path(temporary.name))
        self.target = target_for(self.engine.url)
        self.imports = ImportStore(self.jobs, launcher=lambda *_: os.getpid(), alive=lambda *_: True)
        with self.engine.begin() as connection:
            tables = ",".join(f'public."{name}"' for name in sorted(Base.metadata.tables))
            connection.exec_driver_sql(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")

    def scalar(self, sql):
        with self.engine.connect() as connection:
            return connection.scalar(text(sql))

    def test_background_process_imports_once_and_persists_receipt(self):
        store = ImportStore(self.jobs)
        processes = []
        create_process = subprocess.Popen

        def capture(*args, **kwargs):
            process = create_process(*args, **kwargs)
            processes.append(process)
            return process

        with (
            patch.dict(
                os.environ,
                {
                    "API_DATABASE_URL": self.engine.url.render_as_string(hide_password=False),
                    "API_SECRET_KEY": uuid.uuid4().hex,
                },
            ),
            patch("platform_app.ui.import_store.subprocess.Popen", side_effect=capture),
        ):
            store.start(self.job.job_id, self.target)
        try:
            self.assertEqual(processes[0].wait(timeout=20), 0, store.history(self.job.job_id))
        except subprocess.TimeoutExpired:
            processes[0].kill()
            processes[0].wait()
            self.fail("Import worker did not exit within 20 seconds")
        record = ImportStore(self.jobs).history(self.job.job_id)[0]
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(record["receipt"]["customers"], 10)
        self.assertEqual(record["receipt"]["orders"], 30)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders"), 30)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM cities"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM stores"), 6)
        store.reconcile()
        self.assertEqual(store.history(self.job.job_id)[0]["status"], "succeeded")

    def test_sql_failure_rolls_back_data_and_sequence_and_can_be_retried(self):
        self.sql.write_text(self.sql.read_text().replace("COMMIT;\n", "SELECT 1/0;\nCOMMIT;\n"))
        self.jobs.set_artifacts(self.job.job_id, collect_artifacts(self.job.directory))
        attempt = self.imports.start(self.job.job_id, self.target)
        self.assertEqual(run_import(self.job.directory, attempt["attempt_id"], self.engine), 1)
        self.assertEqual(self.imports.history(self.job.job_id)[0]["status"], "failed")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM customers"), 0)
        self.assertEqual(self.scalar("SELECT last_value FROM customers_customer_id_seq"), 1)
        self.assertEqual(self.scalar("SELECT last_value FROM stores_store_id_seq"), 1)

    def test_dataset_previously_loaded_by_cli_is_not_loaded_again(self):
        receipt = import_dataset(self.engine, self.sql)
        attempt = self.imports.start(self.job.job_id, self.target)
        self.assertEqual(run_import(self.job.directory, attempt["attempt_id"], self.engine), 1)
        record = self.imports.history(self.job.job_id)[0]
        self.assertEqual(record["status"], "already_present")
        self.assertIsNone(record["receipt"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders"), receipt["orders"])

    def test_partial_generation_with_successful_sql_remains_importable(self):
        self.jobs.finish_job(self.job.job_id, "partial", "logs fixture failure")
        attempt = self.imports.start(self.job.job_id, self.target)
        self.assertEqual(run_import(self.job.directory, attempt["attempt_id"], self.engine), 0)
        self.assertEqual(self.imports.history(self.job.job_id)[0]["status"], "succeeded")
        self.assertEqual(self.jobs.get_job(self.job.job_id).status, "partial")
