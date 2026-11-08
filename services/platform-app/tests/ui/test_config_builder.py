import unittest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from generator.config import config_from_dict

from platform_app.ui.config_builder import FormValues, build_config_document, freeze_config, load_preset
from platform_app.ui.job_store import JobStore, JobStoreError


class LocationFormTests(unittest.TestCase):
    def test_form_submission_passes_selected_locations_to_job(self):
        from streamlit.testing.v1 import AppTest

        root = Path(__file__).resolve().parents[4]
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(os.environ, {"GENERATOR_CONSOLE_DATA_DIR": directory}),
            patch.object(JobStore, "create_job", side_effect=JobStoreError("Submission captured for test")) as create,
        ):
            app = AppTest.from_file(str(root / "services/platform-app/src/platform_app/ui/app.py")).run()
            self.assertFalse(app.exception)
            self.assertEqual(len(app.multiselect(key="store_codes").value), 6)
            app.multiselect(key="store_codes").set_value(["HN-CENTRE"])
            app.button[0].click().run()
            self.assertFalse(app.exception)
            create.assert_called_once()
            self.assertEqual(create.call_args.args[0]["locations"], {"store_codes": ["HN-CENTRE"]})

    def test_selection_and_empty_selection_are_frozen_in_job_config(self):
        preset = load_preset(Path(__file__).resolve().parents[4] / "generator/configs/small.yml")
        for stores in (("HN-CENTRE",), ()):
            values = FormValues(
                scenario_id="fixture",
                seed=1,
                anchor_time="2026-07-01T00:00:00+00:00",
                history_months=1,
                customers=10,
                products=2,
                variants=8,
                orders=20,
                distributions_yaml="",
                store_codes=stores,
            )
            document = build_config_document(preset, values)
            config = config_from_dict(document)
            self.assertEqual(config.locations.store_codes, stores)
            self.assertIn("store_codes:", freeze_config(document))
            self.assertNotIn("locations", preset)
