import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from platform_app.api.models.customer import Customer, CustomerCredential
from platform_app.provisioning import admin
from platform_app.ui.scenarios.config import ScenarioSettings


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


class ScenarioAdminTests(unittest.TestCase):
    def session(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        return session

    def settings(self, email="sim@web.local"):
        return ScenarioSettings(admin_email=email, admin_password="Sim@12345", _env_file=None)

    def test_scenario_admin_seeded_when_configured(self):
        session = self.session()
        admin.seed_scenario_admin(session, self.settings(), "admin@web.local")
        added = [call.args[0] for call in session.add.call_args_list]
        customer = next(row for row in added if isinstance(row, Customer))
        credential = next(row for row in added if isinstance(row, CustomerCredential))
        self.assertEqual(customer.role, "admin")
        self.assertEqual(credential.email_normalized, "sim@web.local")

    def test_scenario_admin_skipped_without_env(self):
        session = self.session()
        admin.seed_scenario_admin(session, ScenarioSettings(_env_file=None), "admin@web.local")
        session.add.assert_not_called()

    def test_blank_admin_env_means_not_configured(self):
        with patch.dict("os.environ", {"SCENARIO_ADMIN_EMAIL": "", "SCENARIO_ADMIN_PASSWORD": ""}):
            settings = ScenarioSettings(_env_file=None)
        self.assertIsNone(settings.admin_email)
        self.assertIsNone(settings.admin_password)

    def test_admin_email_must_differ_from_bootstrap(self):
        with self.assertRaises(RuntimeError):
            admin.seed_scenario_admin(self.session(), self.settings("Admin@Web.local"), "admin@web.local")


if __name__ == "__main__":
    unittest.main()
