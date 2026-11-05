import tempfile
import unittest
import zipfile
from pathlib import Path

from generator_console.artifacts import (
    archive_logs,
    collect_artifacts,
    sha256_file,
    write_checksums,
)


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.job = Path(self.temporary.name) / "job"
        self.job.mkdir()
        (self.job / "config.yml").write_text("scenario_id: test\n", encoding="utf-8")
        (self.job / "request.json").write_text("{}\n", encoding="utf-8")
        (self.job / "test.manifest.json").write_text("{}\n", encoding="utf-8")
        (self.job / "outputs" / "sql").mkdir(parents=True)
        (self.job / "outputs" / "sql" / "test.sql").write_text("SELECT 1;\n", encoding="utf-8")
        log_root = self.job / "outputs" / "access-logs" / "landing" / "logs"
        log_root.mkdir(parents=True)
        (log_root / "a.jsonl.gz").write_bytes(b"one")
        (log_root / "b.jsonl.gz").write_bytes(b"two")
        (self.job / "logs").mkdir()
        (self.job / "logs" / "validate.log").write_text("ok\n", encoding="utf-8")
        (self.job / "state.json").write_text("{}\n", encoding="utf-8")

    def tearDown(self):
        self.temporary.cleanup()

    def test_sha256_file(self):
        expected = "e1a9d8a6d0c6a3d4f5b2c1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0"
        self.assertEqual(len(sha256_file(self.job / "config.yml")), 64)
        self.assertNotEqual(sha256_file(self.job / "config.yml"), expected)

    def test_write_checksums_excludes_state_and_self(self):
        path = write_checksums(self.job)
        text = path.read_text()
        self.assertIn("config.yml", text)
        self.assertIn("outputs/access-logs/landing/logs/a.jsonl.gz", text)
        self.assertNotIn("state.json", text)
        self.assertNotIn("checksums.sha256", text)

    def test_collect_artifacts_summarizes_log_tree(self):
        write_checksums(self.job)
        artifacts = collect_artifacts(self.job)
        by_path = {item["path"]: item for item in artifacts}
        self.assertEqual(by_path["config.yml"]["kind"], "config")
        self.assertEqual(by_path["test.manifest.json"]["kind"], "manifest")
        self.assertEqual(by_path["outputs/sql/test.sql"]["kind"], "sql")
        self.assertEqual(by_path["checksums.sha256"]["kind"], "checksums")
        logs = by_path["outputs/access-logs"]
        self.assertEqual(logs["kind"], "logs_directory")
        self.assertEqual(logs["file_count"], 2)
        self.assertEqual(logs["size"], 6)
        self.assertNotIn("outputs/access-logs/landing/logs/a.jsonl.gz", by_path)

    def test_archive_logs_within_limit(self):
        archive = archive_logs(self.job, limit=100)
        self.assertIsNotNone(archive)
        with zipfile.ZipFile(archive) as zipped:
            names = zipped.namelist()
        self.assertIn("access-logs/landing/logs/a.jsonl.gz", names)
        self.assertIn("access-logs/landing/logs/b.jsonl.gz", names)

    def test_archive_logs_skipped_over_limit(self):
        self.assertIsNone(archive_logs(self.job, limit=1))

    def test_collect_artifacts_includes_archive(self):
        archive_logs(self.job, limit=100)
        write_checksums(self.job)
        artifacts = collect_artifacts(self.job)
        by_path = {item["path"]: item for item in artifacts}
        self.assertEqual(by_path["outputs/access-logs.zip"]["kind"], "logs_archive")
        self.assertEqual(by_path["outputs/access-logs"]["archive"], "outputs/access-logs.zip")


if __name__ == "__main__":
    unittest.main()
