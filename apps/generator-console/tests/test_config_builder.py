import unittest
from pathlib import Path

import yaml

from generator.config import config_from_dict
from generator_console.config_builder import (
    FormValues,
    build_config_document,
    build_request,
    freeze_config,
    list_presets,
    load_preset,
    resolve_expected_requests,
    validate_config_document,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIGS = REPO_ROOT / "generator" / "configs"
SMALL = CONFIGS / "small.yml"


def values(**overrides):
    preset = load_preset(SMALL)
    base = FormValues(
        scenario_id=preset["scenario_id"],
        seed=preset["seed"],
        anchor_time=preset["anchor_time"],
        history_months=preset["history_months"],
        customers=preset["scale"]["customers"],
        products=preset["scale"]["products"],
        variants=preset["scale"]["variants"],
        orders=preset["scale"]["orders"],
        output_mode="both",
        expected_requests=0,
        distributions_yaml=yaml.safe_dump(preset["distributions"], sort_keys=False),
    )
    return FormValues(**{**base.__dict__, **overrides})


class ConfigBuilderTests(unittest.TestCase):
    def test_list_presets(self):
        names = [path.name for path in list_presets(CONFIGS)]
        self.assertIn("small.yml", names)
        self.assertEqual(names, sorted(names))

    def test_valid_document_round_trips(self):
        preset = load_preset(SMALL)
        document = build_config_document(preset, values())
        config = validate_config_document(document, values())
        frozen = freeze_config(document)
        restored = config_from_dict(yaml.safe_load(frozen))
        self.assertEqual(restored.logical_identity, config.logical_identity)
        self.assertEqual(document["dataset_size"], preset["dataset_size"])
        self.assertEqual(document["modes"], preset["modes"])

    def test_rejects_variants_below_products(self):
        document = build_config_document(load_preset(SMALL), values(products=5, variants=4))
        with self.assertRaisesRegex(ValueError, "variants must be greater"):
            validate_config_document(document, values(products=5, variants=4))

    def test_rejects_naive_anchor_time(self):
        document = build_config_document(load_preset(SMALL), values(anchor_time="2026-07-01T00:00:00"))
        with self.assertRaisesRegex(ValueError, "timezone"):
            validate_config_document(document, values(anchor_time="2026-07-01T00:00:00"))

    def test_rejects_scenario_id_with_path_separator(self):
        document = build_config_document(load_preset(SMALL), values(scenario_id="../escape"))
        with self.assertRaisesRegex(ValueError, "scenario_id"):
            validate_config_document(document, values(scenario_id="../escape"))

    def test_rejects_distributions_yaml_list(self):
        with self.assertRaisesRegex(ValueError, "mapping"):
            build_config_document(load_preset(SMALL), values(distributions_yaml="- one\n- two\n"))

    def test_rejects_zero_history_months(self):
        document = build_config_document(load_preset(SMALL), values(history_months=0))
        with self.assertRaisesRegex(ValueError, "history_months"):
            validate_config_document(document, values(history_months=0))

    def test_auto_expected_requests_uses_order_floor(self):
        preset = load_preset(SMALL)
        document = build_config_document(preset, values(orders=10))
        self.assertEqual(resolve_expected_requests(document, values(orders=10)), 1000)

    def test_explicit_expected_requests_is_kept(self):
        document = build_config_document(load_preset(SMALL), values(expected_requests=42))
        self.assertEqual(resolve_expected_requests(document, values(expected_requests=42)), 42)

    def test_build_request_records_mode_and_preset(self):
        request = build_request("small", values(output_mode="sql"), 1000)
        self.assertEqual(request, {"preset": "small", "output": "sql", "expected_requests": 1000})


if __name__ == "__main__":
    unittest.main()
