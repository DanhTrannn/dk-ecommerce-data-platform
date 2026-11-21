import copy
import unittest
from datetime import UTC, datetime

from pydantic import ValidationError

from datagen.presets import OltpPreset, load_oltp_preset, preset_path

from fixtures import preset_data, set_path


class PresetTests(unittest.TestCase):
    def test_three_presets_load_and_differ_only_in_scenario_and_scale(self):
        presets = {name: load_oltp_preset(preset_path(name)) for name in ("1m", "10m", "50m")}
        dumps = {name: p.model_dump(exclude={"scenario_id", "scale"}) for name, p in presets.items()}
        self.assertEqual(dumps["1m"], dumps["10m"])
        self.assertEqual(dumps["1m"], dumps["50m"])
        self.assertEqual([presets[n].scale.buying_customers for n in ("1m", "10m", "50m")], [260000, 2600000, 13000000])
        self.assertEqual([presets[n].scale.stores for n in ("1m", "10m", "50m")], [5, 20, 50])

    def test_derived_fields(self):
        preset = load_oltp_preset(preset_path("1m"))
        self.assertEqual(preset.history_start, datetime(2025, 7, 1, tzinfo=UTC))
        self.assertEqual(preset.history_years, 1.0)
        self.assertEqual(preset.run_id, "1m-fashion-v2:20260724")

    def assert_rejected(self, data, key):
        with self.assertRaises(ValidationError) as caught:
            OltpPreset.model_validate(data)
        self.assertIn(key, str(caught.exception))

    def test_unknown_key_is_rejected(self):
        self.assert_rejected(set_path(preset_data(), "outcomes.typo_rate", 0.1), "typo_rate")

    def test_missing_key_is_rejected(self):
        data = preset_data()
        del data["pos"]["walk_in_rate"]
        self.assert_rejected(data, "walk_in_rate")

    def test_city_shares_must_sum_to_one(self):
        data = preset_data()
        data["locations"]["cities"][0]["share"] = 0.5
        self.assert_rejected(data, "cities")

    def test_city_zone_without_delivery_days_is_rejected(self):
        data = preset_data()
        del data["fulfilment"]["delivery_days"]["special"]
        self.assert_rejected(data, "special")

    def test_reversed_range_is_rejected(self):
        self.assert_rejected(set_path(preset_data(), "policies.inventory.buffer_units", [200, 80]), "buffer_units")

    def test_rate_outside_unit_interval_is_rejected(self):
        self.assert_rejected(set_path(preset_data(), "outcomes.return_rate", 1.5), "return_rate")

    def test_hour_profile_must_have_24_entries(self):
        data = copy.deepcopy(preset_data())
        data["calendar"]["hour_of_day"]["normal"] = data["calendar"]["hour_of_day"]["normal"][:23]
        self.assert_rejected(data, "normal")

    def test_variants_per_product_must_leave_room_for_exchanges_and_fit_the_sizes(self):
        for value in (1, 5):
            with self.subTest(value=value):
                self.assert_rejected(
                    set_path(preset_data(), "products.variants_per_product", value), "variants_per_product"
                )
        OltpPreset.model_validate(set_path(preset_data(), "products.variants_per_product", 2))

    def test_unknown_category_code_is_rejected(self):
        data = preset_data()
        data["products"]["categories"]["mu"] = data["products"]["categories"].pop("tui-xach")
        self.assert_rejected(data, "mu")

    def test_pos_payment_methods_need_every_method_and_some_weight(self):
        data = preset_data()
        del data["pos"]["payment_methods"]["cash"]
        self.assert_rejected(data, "cash")
        self.assert_rejected(
            set_path(preset_data(), "pos.payment_methods", {"cash": 0, "card": 0, "vietqr": 0}), "payment_methods"
        )

    def test_tet_date_must_fall_inside_history(self):
        self.assert_rejected(set_path(preset_data(), "calendar.tet.dates", ["2024-02-10"]), "tet")


if __name__ == "__main__":
    unittest.main()
