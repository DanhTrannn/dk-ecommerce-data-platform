import json
import os
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from platform_app.ui.artifacts import collect_artifacts, sha256_file
from platform_app.ui.import_store import ImportStore, target_for
from platform_app.ui.import_worker import run_import
from platform_app.ui.job_store import JobStoreError

from .import_fixture import generated_job


class ConsoleImportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.jobs, self.job, self.sql = generated_job(Path(temporary.name))
        self.engine = create_engine("postgresql+psycopg2://user:private-fixture@localhost/source_test")
        self.addCleanup(self.engine.dispose)
        self.target = target_for(self.engine.url)
        self.launch = MagicMock(return_value=os.getpid())
        self.imports = ImportStore(self.jobs, launcher=self.launch, alive=lambda *_: True)
        self.receipt = dict(
            sql_sha256=sha256_file(self.sql),
            customers=10,
            orders=30,
            logical_identity=self.job.state["dataset_identity"],
            placed_order_value_vnd=100000,
        )

    def start(self):
        return self.imports.start(self.job.job_id, self.target)

    def test_success_preserves_generation_and_private_snapshot_is_removed(self):
        before = (self.job.directory / "checksums.sha256").read_bytes()
        attempt = self.start()
        snapshots = []

        def load(engine, path):
            snapshots.append(path)
            self.assertNotEqual(path, self.sql)
            self.assertEqual(path.read_bytes(), self.sql.read_bytes())
            return self.receipt

        with patch("platform_app.ui.import_worker.import_dataset", side_effect=load):
            self.assertEqual(run_import(self.job.directory, attempt["attempt_id"], self.engine), 0)
        record = ImportStore(self.jobs).history(self.job.job_id)[0]
        self.assertEqual(record["status"], "succeeded")
        self.assertEqual(record["receipt"], self.receipt)
        self.assertFalse(snapshots[0].exists())
        self.assertEqual(before, (self.job.directory / "checksums.sha256").read_bytes())
        self.assertFalse(any(row["path"].startswith("imports/") for row in collect_artifacts(self.job.directory)))
        self.assertEqual(self.jobs.get_job(self.job.job_id).status, "succeeded")
        self.assertNotIn("private-fixture", json.dumps(record))
        with self.assertRaises(JobStoreError):
            self.start()

    def test_double_click_starts_only_one_worker(self):
        def click(_):
            try:
                self.start()
                return True
            except JobStoreError:
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(click, range(2))), 1)
        self.launch.assert_called_once()
        self.assertEqual(len(self.imports.history(self.job.job_id)), 1)

    def test_modified_sql_is_rejected_before_database_use(self):
        attempt = self.start()
        self.sql.write_text(self.sql.read_text() + "-- changed\n")
        with patch("platform_app.ui.import_worker.import_dataset") as load:
            self.assertEqual(run_import(self.job.directory, attempt["attempt_id"], self.engine), 1)
        load.assert_not_called()
        record = self.imports.history(self.job.job_id)[0]
        self.assertEqual(record["status"], "failed")
        self.assertIn("Checksum", record["message"])

    def test_target_change_cannot_redirect_an_existing_request(self):
        attempt = self.start()
        self.engine.url = make_url("postgresql+psycopg2://user@localhost/another_database")
        with patch("platform_app.ui.import_worker.import_dataset") as load:
            run_import(self.job.directory, attempt["attempt_id"], self.engine)
        load.assert_not_called()
        self.assertEqual(self.imports.history(self.job.job_id)[0]["status"], "failed")

    def test_lost_worker_is_unknown_and_cannot_be_automatically_reimported(self):
        self.start()
        store = ImportStore(self.jobs, alive=lambda *_: False)
        store.reconcile()
        self.assertEqual(store.history(self.job.job_id)[0]["status"], "unknown")
        with self.assertRaises(JobStoreError):
            store.start(self.job.job_id, self.target)

    def test_launch_failure_is_persisted_and_retry_creates_another_attempt(self):
        self.launch.side_effect = OSError("private-fixture")
        with self.assertRaises(JobStoreError):
            self.start()
        failed = self.imports.history(self.job.job_id)[0]
        self.assertEqual(failed["status"], "failed")
        self.assertNotIn("private-fixture", json.dumps(failed))
        self.launch.side_effect = None
        self.start()
        self.assertEqual(len(self.imports.history(self.job.job_id)), 2)

    def test_duplicate_database_run_is_not_reported_as_a_new_success(self):
        attempt = self.start()
        with patch(
            "platform_app.ui.import_worker.import_dataset",
            side_effect=ValueError("Dataset already imported; no rows were changed"),
        ):
            run_import(self.job.directory, attempt["attempt_id"], self.engine)
        record = self.imports.history(self.job.job_id)[0]
        self.assertEqual(record["status"], "already_present")
        self.assertIsNone(record["receipt"])
        with self.assertRaises(JobStoreError):
            self.start()

    def test_database_messages_are_redacted(self):
        attempt = self.start()
        original = Exception("private-fixture SQL with personal data")
        original.pgcode = "23514"
        error = OperationalError("private SQL", {}, original)
        with patch("platform_app.ui.import_worker.import_dataset", side_effect=error):
            run_import(self.job.directory, attempt["attempt_id"], self.engine)
        record = self.imports.history(self.job.job_id)[0]
        self.assertEqual(record["status"], "failed")
        self.assertNotIn("private-fixture", json.dumps(record))

    def test_connection_failure_is_unknown(self):
        attempt = self.start()

        def fail_at_commit(engine, path):
            engine.dispatch.commit(None)
            raise OperationalError(None, None, Exception("private-fixture"))

        with patch(
            "platform_app.ui.import_worker.import_dataset",
            side_effect=fail_at_commit,
        ):
            run_import(self.job.directory, attempt["attempt_id"], self.engine)
        record = self.imports.history(self.job.job_id)[0]
        self.assertEqual(record["status"], "unknown")
        self.assertNotIn("private-fixture", json.dumps(record))

    def test_connection_failure_before_commit_allows_explicit_retry(self):
        attempt = self.start()
        with patch(
            "platform_app.ui.import_worker.import_dataset",
            side_effect=OperationalError(None, None, Exception("private-fixture")),
        ):
            run_import(self.job.directory, attempt["attempt_id"], self.engine)
        self.assertEqual(self.imports.history(self.job.job_id)[0]["status"], "failed")
        self.start()
        self.assertEqual(len(self.imports.history(self.job.job_id)), 2)

    def test_receipt_write_failure_after_commit_is_unknown(self):
        attempt = self.start()
        original = ImportStore.finish
        calls = []

        def finish(store, record, status, **values):
            calls.append(status)
            if status == "succeeded":
                raise OSError("private-fixture")
            original(store, record, status, **values)

        with (
            patch("platform_app.ui.import_worker.import_dataset", return_value=self.receipt) as load,
            patch.object(ImportStore, "finish", finish),
        ):
            run_import(self.job.directory, attempt["attempt_id"], self.engine)
        load.assert_called_once()
        self.assertEqual(calls, ["succeeded", "unknown"])
        self.assertEqual(self.imports.history(self.job.job_id)[0]["status"], "unknown")

    def test_unsafe_paths_incomplete_jobs_and_identity_mismatch_are_rejected(self):
        with self.assertRaises(JobStoreError):
            self.imports.job("../outside")
        original = self.jobs.get_job(self.job.job_id).state["artifacts"]
        self.jobs.set_artifacts(
            self.job.job_id, [{"kind": "sql", "path": "outputs/sql/../../other.sql", "sha256": "a" * 64}]
        )
        with self.assertRaises(JobStoreError):
            self.start()
        self.jobs.set_artifacts(self.job.job_id, original)
        self.jobs.finish_job(self.job.job_id, "running")
        with self.assertRaises(JobStoreError):
            self.start()
        self.jobs.finish_job(self.job.job_id, "succeeded")
        sql = self.sql.read_text()
        metadata = json.loads(sql.splitlines()[1].removeprefix("-- import_metadata: "))
        metadata["logical_identity"] = str(uuid.uuid4())
        self.sql.write_text(sql.replace(sql.splitlines()[1], "-- import_metadata: " + json.dumps(metadata)))
        self.jobs.set_artifacts(self.job.job_id, collect_artifacts(self.job.directory))
        attempt = self.start()
        with patch("platform_app.ui.import_worker.import_dataset") as load:
            run_import(self.job.directory, attempt["attempt_id"], self.engine)
        load.assert_not_called()
        self.assertEqual(self.imports.history(self.job.job_id)[0]["status"], "failed")

    def test_result_ui_queues_once_and_disables_a_second_click(self):
        from streamlit.testing.v1 import AppTest
        from .import_fixture import ROOT

        with (
            patch.dict(os.environ, {"GENERATOR_CONSOLE_DATA_DIR": str(self.jobs.root)}),
            patch("platform_app.ui.views.imports.configured_target", return_value=self.target),
            patch("platform_app.ui.import_store._launch", return_value=os.getpid()) as launch,
            patch("platform_app.ui.import_store._alive", return_value=True),
        ):
            app = AppTest.from_file(str(ROOT / "services/platform-app/src/platform_app/ui/app.py")).run()
            app.radio(key="section").set_value("Kết quả & lịch sử").run()
            self.assertFalse(app.exception)
            key = f"import-{self.job.job_id}"
            self.assertFalse(app.button(key=key).disabled)
            app.button(key=key).click().run()
            self.assertFalse(app.exception)
            launch.assert_called_once()
            self.assertTrue(app.button(key=key).disabled)
            self.assertEqual(len(self.imports.history(self.job.job_id)), 1)
