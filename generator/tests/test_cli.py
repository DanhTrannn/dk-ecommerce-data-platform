import io
import os
import tempfile
import unittest
from pathlib import Path
from contextlib import redirect_stderr
from unittest import mock

from datagen.cli import main
from datagen.presets import load_log_preset, log_preset_path
from datagen.oltp.run import LoadFailed


class CliTests(unittest.TestCase):
    def test_generate_requires_database_url(self):
        with mock.patch.dict(os.environ, {}, clear=True), redirect_stderr(io.StringIO()) as err:
            self.assertEqual(main(["generate", "--preset", "1m"]), 2)
        self.assertIn("GENERATOR_DATABASE_URL", err.getvalue())

    def test_unknown_preset_is_reported(self):
        with (
            mock.patch.dict(os.environ, {"GENERATOR_DATABASE_URL": "postgresql://x@/y"}),
            redirect_stderr(io.StringIO()) as err,
        ):
            self.assertEqual(main(["generate", "--preset", "7m"]), 2)
        self.assertIn("7m", err.getvalue())

    def test_failed_load_tells_how_to_reclaim_the_disk_space(self):
        with (
            mock.patch.dict(os.environ, {"GENERATOR_DATABASE_URL": "postgresql://x@/y"}),
            mock.patch("datagen.cli.connect"),
            mock.patch("datagen.cli.generate", side_effect=LoadFailed("Lô online 3 lỗi: boom")),
            redirect_stderr(io.StringIO()) as err,
        ):
            self.assertEqual(main(["generate", "--preset", "1m"]), 1)
        self.assertIn("Lô online 3 lỗi: boom", err.getvalue())
        self.assertIn("datagen reset --yes", err.getvalue())
        self.assertIn("make seed", err.getvalue())

    def test_reset_requires_yes(self):
        with redirect_stderr(io.StringIO()) as err:
            self.assertEqual(main(["reset"]), 2)
        self.assertIn("--yes", err.getvalue())

    def test_reset_requires_database_url(self):
        with mock.patch.dict(os.environ, {}, clear=True), redirect_stderr(io.StringIO()) as err:
            self.assertEqual(main(["reset", "--yes"]), 2)
        self.assertIn("GENERATOR_DATABASE_URL", err.getvalue())


if __name__ == "__main__":
    unittest.main()


class LogsCliTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.tmp = Path(temporary.name)
        patcher = mock.patch.dict(os.environ, {"GENERATOR_DATABASE_URL": "postgresql://x@/y"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_logs(self, out):
        with mock.patch("datagen.cli.connect") as connect, redirect_stderr(io.StringIO()) as err:
            code = main(["logs", "--preset", "1m", "--out", str(out)])
        return code, err.getvalue(), connect

    def test_refuses_non_empty_out(self):
        out = self.tmp / "x"
        out.mkdir()
        (out / "old.parquet").write_bytes(b"x")
        code, err, connect = self.run_logs(out)
        self.assertEqual(code, 1)
        self.assertIn(str(out), err)
        connect.assert_not_called()
        self.assertEqual([p.name for p in out.iterdir()], ["old.parquet"])

    def test_refuses_a_log_preset_for_another_oltp_preset(self):
        other = load_log_preset(log_preset_path("1m")).model_copy(update={"oltp_scenario_id": "10m-fashion-v2"})
        with mock.patch("datagen.cli.load_log_preset", return_value=other):
            code, err, connect = self.run_logs(self.tmp / "y")
        self.assertEqual(code, 1)
        self.assertIn("10m-fashion-v2", err)
        connect.assert_not_called()

    def test_refuses_a_window_outside_the_oltp_history(self):
        preset = load_log_preset(log_preset_path("1m"))
        window = preset.window.model_copy(update={"start": preset.window.start.replace(year=2020)})
        with mock.patch("datagen.cli.load_log_preset", return_value=preset.model_copy(update={"window": window})):
            code, err, connect = self.run_logs(self.tmp / "z")
        self.assertEqual(code, 1)
        self.assertIn("2020", err)
        connect.assert_not_called()

    def test_refuses_a_window_ending_after_the_oltp_history(self):
        preset = load_log_preset(log_preset_path("1m"))
        window = preset.window.model_copy(update={"end": preset.window.end.replace(year=2027)})
        with mock.patch("datagen.cli.load_log_preset", return_value=preset.model_copy(update={"window": window})):
            code, err, connect = self.run_logs(self.tmp / "w")
        self.assertEqual(code, 1)
        self.assertIn("2027", err)
        connect.assert_not_called()

    def test_missing_log_preset_is_reported(self):
        missing = self.tmp / "7m.yml"
        with mock.patch("datagen.cli.log_preset_path", return_value=missing):
            code, err, connect = self.run_logs(self.tmp / "v")
        self.assertEqual(code, 2)
        self.assertIn(str(missing), err)
        connect.assert_not_called()
