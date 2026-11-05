import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from generator_console.job_store import (
    STAGE_FAILED,
    STAGE_SKIPPED,
    STAGE_SUCCEEDED,
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    STATUS_PARTIAL,
    STATUS_SUCCEEDED,
    JobStore,
)
from generator_console.worker import run_job

DOCUMENT = {
    "scenario_id": "test-scenario",
    "seed": 1,
    "anchor_time": "2026-07-01T00:00:00+00:00",
    "history_months": 1,
    "modes": ["seed_master"],
    "scale": {"customers": 1, "products": 1, "variants": 1, "orders": 0},
}
CONFIG_YAML = "scenario_id: test-scenario\n"


class FakeRunner:
    def __init__(self, job_dir: Path, *, fail_logs: bool = False, fail_validation: bool = False):
        self.job_dir = job_dir
        self.fail_logs = fail_logs
        self.fail_validation = fail_validation

    def __call__(self, command, log_path):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("fake stage log\n", encoding="utf-8")
        if "run" in command:
            if self.fail_validation:
                return subprocess.CompletedProcess(command, 1, "", "validation failed")
            manifest = self.job_dir / "test-scenario-identity.manifest.json"
            manifest.write_text("{}\n", encoding="utf-8")
            payload = {
                "manifest_path": str(manifest),
                "logical_identity": "identity",
                "generator_version": "0.8.0",
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")
        if "export-sql" in command:
            output = Path(command[command.index("--output") + 1])
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text("SELECT 1;\n", encoding="utf-8")
            payload = {
                "sql_path": str(output),
                "generation_run_id": "run-id",
                "logical_identity": "identity",
                "customers": 1,
                "products": 1,
                "variants": 1,
                "orders": 0,
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")
        if "export-logs" in command:
            if self.fail_logs:
                return subprocess.CompletedProcess(command, 1, "", "log export failed")
            output = Path(command[command.index("--output-directory") + 1])
            log_dir = output / "landing" / "logs" / "ingest_date=2026-01-01"
            log_dir.mkdir(parents=True, exist_ok=True)
            (log_dir / "part.jsonl.gz").write_bytes(b"log")
            payload = {
                "output_root": str(output),
                "logical_identity": "log-identity",
                "expected_requests": 1000,
                "emitted_requests": 10,
                "files": 1,
                "manifests": 0,
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")
        raise AssertionError(f"unexpected command: {command}")


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "jobs"
        self.store = JobStore(self.root, launcher=lambda job_dir: None)

    def tearDown(self):
        self.temporary.cleanup()

    def create(self, output="both"):
        return self.store.create_job(
            DOCUMENT,
            {"preset": "small", "output": output, "expected_requests": 1000},
            preset_name="small",
            config_yaml=CONFIG_YAML,
            generator_version="0.8.0",
            dataset_identity="identity",
        )

    def test_success_writes_stages_and_artifacts(self):
        job = self.create()
        exit_code = run_job(job.directory, FakeRunner(job.directory))
        self.assertEqual(exit_code, 0)
        state = self.store.get_job(job.job_id).state
        self.assertEqual(state["status"], STATUS_SUCCEEDED)
        self.assertEqual(state["stages"]["validate"]["status"], STAGE_SUCCEEDED)
        self.assertEqual(state["stages"]["sql"]["status"], STAGE_SUCCEEDED)
        self.assertEqual(state["stages"]["logs"]["status"], STAGE_SUCCEEDED)
        self.assertEqual(state["log_identity"], "log-identity")
        kinds = {item["kind"] for item in state["artifacts"]}
        self.assertTrue({"config", "manifest", "sql", "logs_directory", "checksums"}.issubset(kinds))
        self.assertTrue((job.directory / "outputs" / "sql" / "test-scenario-identity.sql").is_file())

    def test_partial_when_sql_succeeds_and_logs_fail(self):
        job = self.create()
        exit_code = run_job(job.directory, FakeRunner(job.directory, fail_logs=True))
        self.assertEqual(exit_code, 1)
        state = self.store.get_job(job.job_id).state
        self.assertEqual(state["status"], STATUS_PARTIAL)
        self.assertEqual(state["stages"]["sql"]["status"], STAGE_SUCCEEDED)
        self.assertEqual(state["stages"]["logs"]["status"], STAGE_FAILED)
        self.assertTrue((job.directory / "outputs" / "sql" / "test-scenario-identity.sql").is_file())
        kinds = {item["kind"] for item in state["artifacts"]}
        self.assertIn("sql", kinds)
        self.assertIn("checksums", kinds)
        self.assertIn("logs: log export failed", state["error"])

    def test_validation_failure_skips_exports(self):
        job = self.create()
        exit_code = run_job(job.directory, FakeRunner(job.directory, fail_validation=True))
        self.assertEqual(exit_code, 1)
        state = self.store.get_job(job.job_id).state
        self.assertEqual(state["status"], STATUS_FAILED)
        self.assertEqual(state["stages"]["validate"]["status"], STAGE_FAILED)
        self.assertEqual(state["stages"]["sql"]["status"], STAGE_SKIPPED)
        self.assertEqual(state["stages"]["logs"]["status"], STAGE_SKIPPED)

    def test_sql_only_skips_logs(self):
        job = self.create(output="sql")
        exit_code = run_job(job.directory, FakeRunner(job.directory))
        self.assertEqual(exit_code, 0)
        state = self.store.get_job(job.job_id).state
        self.assertEqual(state["stages"]["logs"]["status"], STAGE_SKIPPED)
        self.assertEqual(state["status"], STATUS_SUCCEEDED)

    def test_logs_only_skips_sql(self):
        job = self.create(output="logs")
        exit_code = run_job(job.directory, FakeRunner(job.directory))
        self.assertEqual(exit_code, 0)
        state = self.store.get_job(job.job_id).state
        self.assertEqual(state["stages"]["sql"]["status"], STAGE_SKIPPED)
        self.assertEqual(state["status"], STATUS_SUCCEEDED)

    def test_interrupted_job_keeps_existing_sql(self):
        job = self.create(output="sql")
        sql_path = job.directory / "outputs" / "sql" / "kept.sql"
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        sql_path.write_text("SELECT 1;\n", encoding="utf-8")
        self.store.mark_started(job.job_id, 999999)
        self.store.mark_stage_running(job.job_id, "sql", "generator export-sql")
        self.store.reconcile()
        state = self.store.get_job(job.job_id).state
        self.assertEqual(state["status"], STATUS_INTERRUPTED)
        self.assertEqual(state["stages"]["sql"]["status"], "interrupted")
        self.assertTrue(sql_path.is_file())


if __name__ == "__main__":
    unittest.main()
