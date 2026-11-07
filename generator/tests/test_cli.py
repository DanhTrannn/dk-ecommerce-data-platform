import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from generator.cli import main
from generator.log_export import LogExportSummary
from generator.sql_export import DatasetSummary

CONFIGS = Path(__file__).resolve().parents[1] / "configs"


class CliJsonTests(unittest.TestCase):
    def test_run_json_prints_manifest_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "out"
            stdout = io.StringIO()
            with (
                redirect_stdout(stdout),
                patch(
                    "sys.argv",
                    ["generator", "run", "--config", str(CONFIGS / "small.yml"), "--output", str(output), "--json"],
                ),
            ):
                main()
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["generator_version"], "0.9.0")
            self.assertTrue(Path(payload["manifest_path"]).is_file())
            self.assertIn("logical_identity", payload)
            self.assertEqual(payload["sql_format"], "postgresql-v1")

    def test_export_sql_json_prints_summary(self):
        summary = DatasetSummary(
            sql_path=Path("/tmp/test.sql"),
            generation_run_id="run-id",
            customers=1,
            products=2,
            variants=3,
            orders=4,
        )
        stdout = io.StringIO()
        with (
            redirect_stdout(stdout),
            patch("generator.cli.export_sql", return_value=summary),
            patch(
                "sys.argv",
                ["generator", "export-sql", "--config", str(CONFIGS / "small.yml"), "--json"],
            ),
        ):
            main()
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["sql_path"], "/tmp/test.sql")
        self.assertEqual(payload["orders"], 4)
        self.assertEqual(payload["sql_format"], "postgresql-v1")
        self.assertEqual(payload["generator_version"], "0.9.0")
        self.assertIn("logical_identity", payload)

    def test_export_logs_json_prints_summary(self):
        summary = LogExportSummary(
            output_root=Path("/tmp/logs"),
            logical_identity="log-id",
            expected_requests=100,
            emitted_requests=90,
            files=5,
            manifests=0,
        )
        stdout = io.StringIO()
        with (
            redirect_stdout(stdout),
            patch("generator.cli.export_logs", return_value=summary),
            patch(
                "sys.argv",
                [
                    "generator",
                    "export-logs",
                    "--config",
                    str(CONFIGS / "small.yml"),
                    "--expected-requests",
                    "100",
                    "--json",
                ],
            ),
        ):
            main()
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["output_root"], "/tmp/logs")
        self.assertEqual(payload["emitted_requests"], 90)


if __name__ == "__main__":
    unittest.main()
