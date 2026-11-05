import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from generator_console.job_store import (
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    JobAlreadyRunning,
    JobNotFound,
    JobStore,
)

DOCUMENT = {
    "scenario_id": "test-scenario",
    "seed": 1,
    "anchor_time": "2026-07-01T00:00:00+00:00",
    "history_months": 1,
    "modes": ["seed_master"],
    "scale": {"customers": 1, "products": 1, "variants": 1, "orders": 0},
}
CONFIG_YAML = "scenario_id: test-scenario\n"


def request(output="both", expected_requests=1000):
    return {"preset": "small", "output": output, "expected_requests": expected_requests}


class JobStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "jobs"
        self.store = JobStore(self.root, launcher=lambda job_dir: None)

    def tearDown(self):
        self.temporary.cleanup()

    def create(self, output="both", launcher=None):
        store = JobStore(self.root, launcher=launcher or (lambda job_dir: None))
        return store.create_job(
            DOCUMENT,
            request(output),
            preset_name="small",
            config_yaml=CONFIG_YAML,
            generator_version="0.8.0",
            dataset_identity="identity",
        )

    def test_creates_isolated_job_layout(self):
        job = self.create()
        self.assertTrue((job.directory / "config.yml").is_file())
        self.assertTrue((job.directory / "request.json").is_file())
        self.assertTrue((job.directory / "state.json").is_file())
        self.assertTrue((job.directory / "logs").is_dir())
        self.assertTrue((job.directory / "outputs").is_dir())
        self.assertEqual(job.state["status"], STATUS_QUEUED)
        self.assertEqual(job.state["dataset_identity"], "identity")
        self.assertEqual(job.state["stages"]["logs"]["status"], "pending")

    def test_skips_disabled_stage(self):
        job = self.create(output="sql")
        self.assertEqual(job.state["stages"]["sql"]["status"], "pending")
        self.assertEqual(job.state["stages"]["logs"]["status"], "skipped")

    def test_second_job_rejected_while_worker_is_alive(self):
        store = JobStore(self.root, launcher=lambda job_dir: os.getpid())
        with patch.object(JobStore, "_pid_matches", return_value=True):
            store.create_job(
                DOCUMENT,
                request(),
                preset_name="small",
                config_yaml=CONFIG_YAML,
                generator_version="0.8.0",
                dataset_identity="identity",
            )
            with self.assertRaises(JobAlreadyRunning):
                store.create_job(
                    DOCUMENT,
                    request(),
                    preset_name="small",
                    config_yaml=CONFIG_YAML,
                    generator_version="0.8.0",
                    dataset_identity="identity",
                )

    def test_reconcile_marks_dead_worker_interrupted(self):
        job = self.create(launcher=lambda job_dir: 999999)
        self.store.reconcile()
        restored = self.store.get_job(job.job_id)
        self.assertEqual(restored.status, STATUS_INTERRUPTED)
        self.assertIsNone(self.store.active_job())

    def test_reconcile_keeps_live_worker(self):
        store = JobStore(self.root, launcher=lambda job_dir: os.getpid())
        with patch.object(JobStore, "_pid_matches", return_value=True):
            job = store.create_job(
                DOCUMENT,
                request(),
                preset_name="small",
                config_yaml=CONFIG_YAML,
                generator_version="0.8.0",
                dataset_identity="identity",
            )
            store.reconcile()
            self.assertEqual(store.get_job(job.job_id).status, STATUS_QUEUED)

    def test_launcher_failure_marks_job_failed(self):
        def fail(job_dir):
            raise OSError("cannot start")

        job = self.create(launcher=fail)
        self.assertEqual(job.state["status"], STATUS_FAILED)
        self.assertIn("cannot start", job.state["error"])
        self.assertIsNone(self.store.active_job())

    def test_state_updates_and_finish(self):
        job = self.create()
        self.store.mark_started(job.job_id, os.getpid())
        self.store.mark_stage_running(job.job_id, "validate", "generator run")
        self.store.complete_stage(
            job.job_id,
            "validate",
            "succeeded",
            exit_code=0,
            summary={"logical_identity": "identity"},
        )
        self.store.finish_job(job.job_id, STATUS_SUCCEEDED)
        restored = self.store.get_job(job.job_id)
        self.assertEqual(restored.status, STATUS_SUCCEEDED)
        self.assertEqual(restored.state["started_at"] is not None, True)
        self.assertEqual(restored.state["stages"]["validate"]["status"], "succeeded")
        self.assertEqual(restored.state["stages"]["validate"]["exit_code"], 0)

    def test_active_fallback_and_release(self):
        job = self.create(launcher=lambda job_dir: os.getpid())
        with patch.object(JobStore, "_pid_matches", return_value=True):
            self.assertEqual(self.store.active_job().job_id, job.job_id)
            (self.root / ".active.json").unlink()
            self.assertEqual(self.store.active_job().job_id, job.job_id)
            self.store.release_active(job.job_id)

    def test_missing_job_raises(self):
        with self.assertRaises(JobNotFound):
            self.store.get_job("missing")

    def test_list_jobs_reads_state(self):
        job = self.create()
        jobs = self.store.list_jobs()
        self.assertEqual([item.job_id for item in jobs], [job.job_id])
        state = json.loads((job.directory / "state.json").read_text())
        self.assertEqual(state["job_id"], job.job_id)

    def test_reconcile_marks_running_stage_interrupted(self):
        job = self.create(launcher=lambda job_dir: 999999)
        self.store.mark_started(job.job_id, 999999)
        self.store.mark_stage_running(job.job_id, "validate", "generator run")
        self.store.reconcile()
        restored = self.store.get_job(job.job_id)
        self.assertEqual(restored.status, STATUS_INTERRUPTED)
        self.assertEqual(restored.state["stages"]["validate"]["status"], STATUS_INTERRUPTED)
        self.assertEqual(restored.state["stages"]["sql"]["status"], "skipped")

    def test_default_launcher_returns_pid(self):
        # The production launcher is exercised separately with a tiny real job;
        # this only pins that a callable returning an int is accepted.
        job = self.create(launcher=lambda job_dir: 12345)
        self.assertEqual(job.state["pid"], 12345)
        self.assertEqual(job.state["status"], STATUS_QUEUED)


if __name__ == "__main__":
    unittest.main()
