import tempfile
import unittest
from pathlib import Path

import yaml

from generator.config import config_from_dict, load_config
from generator.validation import validate_scale

CONFIGS = Path(__file__).resolve().parents[1] / "configs"


def small_raw() -> dict:
    return yaml.safe_load((CONFIGS / "small.yml").read_text(encoding="utf-8"))


class ValidationTests(unittest.TestCase):
    def test_config_from_dict_returns_validated_config(self):
        config = config_from_dict(small_raw())
        self.assertEqual(config.scenario_id, "small-repurchase-v2")
        self.assertEqual(config.scale["customers"], 500)

    def test_rejects_variants_below_products(self):
        raw = small_raw()
        raw["scale"] = {"customers": 10, "products": 5, "variants": 4, "orders": 10}
        with self.assertRaisesRegex(ValueError, "variants must be greater"):
            config_from_dict(raw)

    def test_rejects_variants_above_combinations(self):
        raw = small_raw()
        raw["scale"] = {"customers": 10, "products": 2, "variants": 121, "orders": 10}
        with self.assertRaisesRegex(ValueError, "unique size/color"):
            config_from_dict(raw)

    def test_rejects_missing_scale_key(self):
        raw = small_raw()
        del raw["scale"]["orders"]
        with self.assertRaisesRegex(ValueError, "missing scale keys"):
            config_from_dict(raw)

    def test_rejects_negative_orders(self):
        raw = small_raw()
        raw["scale"]["orders"] = -1
        with self.assertRaisesRegex(ValueError, "orders must be non-negative"):
            config_from_dict(raw)

    def test_load_config_rejects_bad_scale(self):
        raw = small_raw()
        raw["scale"] = {"customers": 10, "products": 5, "variants": 4, "orders": 10}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.yml"
            path.write_text(yaml.safe_dump(raw), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_config(path)

    def test_validate_scale_returns_counts(self):
        self.assertEqual(
            validate_scale({"customers": 1, "products": 2, "variants": 3, "orders": 0}),
            (1, 2, 3, 0),
        )


if __name__ == "__main__":
    unittest.main()
