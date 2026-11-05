import os
import tempfile
import unittest
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parents[1] / "src" / "generator_console" / "app.py"


class AppSmokeTests(unittest.TestCase):
    def test_app_renders_without_jobs(self):
        previous = os.environ.get("GENERATOR_CONSOLE_DATA_DIR")
        with tempfile.TemporaryDirectory() as directory:
            os.environ["GENERATOR_CONSOLE_DATA_DIR"] = directory
            try:
                app = AppTest.from_file(str(APP), default_timeout=30)
                app.run()
            finally:
                if previous is None:
                    os.environ.pop("GENERATOR_CONSOLE_DATA_DIR", None)
                else:
                    os.environ["GENERATOR_CONSOLE_DATA_DIR"] = previous
        self.assertEqual(len(app.exception), 0)
        self.assertEqual([title.value for title in app.title][-1], "Generator Console")
        sections = next(radio for radio in app.radio if radio.label == "Màn hình")
        self.assertIn("Cấu hình", sections.options)
        self.assertEqual(app.selectbox(key="preset_name").value, "small.yml")


if __name__ == "__main__":
    unittest.main()
