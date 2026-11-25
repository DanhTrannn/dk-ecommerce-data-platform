import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from platform_app.provisioning import admin


class PrepareStorageTests(unittest.TestCase):
    def test_prepare_storage_creates_scenarios_dir(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dirs = (root / "secrets", root / "access-admin", root / "scenarios")
            with (
                patch.object(admin, "STORAGE_DIRS", dirs),
                patch.object(admin, "EXPORTER_KEY", dirs[0] / "exporter.key"),
                patch.object(admin, "atomic_private"),
                patch("os.chown") as chown,
                patch("os.setgroups"),
                patch("os.setgid"),
                patch("os.setuid"),
            ):
                admin.prepare_storage()
            for path in dirs:
                self.assertTrue(path.is_dir())
                self.assertEqual(path.stat().st_mode & 0o777, 0o700)
                chown.assert_any_call(path, 1000, 1000)


if __name__ == "__main__":
    unittest.main()
