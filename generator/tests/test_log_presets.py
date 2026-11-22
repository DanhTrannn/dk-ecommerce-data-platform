import unittest
from datetime import date

import yaml
from pydantic import ValidationError

from datagen.presets import LogPreset, load_log_preset, log_preset_path

from fixtures import set_path


def log_data():
    return yaml.safe_load(log_preset_path("1m").read_text(encoding="utf-8"))


class LogPresetTests(unittest.TestCase):
    def assert_rejected(self, data, key):
        with self.assertRaises(ValidationError) as caught:
            LogPreset.model_validate(data)
        self.assertIn(key, str(caught.exception))

    def test_three_log_presets_load_and_differ_only_in_identity(self):
        presets = {name: load_log_preset(log_preset_path(name)) for name in ("1m", "10m", "50m")}
        dumps = {name: p.model_dump(exclude={"scenario_id", "oltp_scenario_id"}) for name, p in presets.items()}
        self.assertEqual(dumps["1m"], dumps["10m"])
        self.assertEqual(dumps["1m"], dumps["50m"])
        self.assertEqual(presets["1m"].window.start, date(2025, 11, 1))
        self.assertEqual(presets["1m"].window.end, date(2025, 12, 16))
        self.assertGreater(presets["1m"].sessions.sale_day_ratio_multiplier, 0)
        self.assertEqual(presets["1m"].oltp_scenario_id, "1m-fashion-v2")

    def test_reversed_window_is_rejected(self):
        self.assert_rejected(set_path(log_data(), "window.start", "2025-12-20"), "window")

    def test_rate_outside_unit_interval_is_rejected(self):
        self.assert_rejected(set_path(log_data(), "funnel.non_buying_cart_rate", 1.5), "non_buying_cart_rate")

    def test_probability_parameters_must_lie_strictly_inside_unit_interval(self):
        self.assert_rejected(set_path(log_data(), "sessions.views_per_session.buying.p", 1.0), "p")
        self.assert_rejected(set_path(log_data(), "sessions.views_per_session.non_buying.theta", 0), "theta")

    def test_unknown_key_is_rejected(self):
        self.assert_rejected(set_path(log_data(), "sessions.typo", 1), "typo")


if __name__ == "__main__":
    unittest.main()
