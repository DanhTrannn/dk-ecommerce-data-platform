import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import yaml

from generator.config import config_from_dict, load_config
from generator.locations import LocationConfig
from generator.runner import run
from generator.sql_export import export_sql

CONFIG = Path(__file__).resolve().parents[1] / "configs/small.yml"


class LocationTests(unittest.TestCase):
    def test_default_and_subset_select_only_required_cities(self):
        config = load_config(CONFIG)
        self.assertEqual(config.locations.counts, {"cities": 5, "stores": 6})
        raw = yaml.safe_load(CONFIG.read_text())
        raw["locations"] = {"store_codes": ["hcm-centre", " HN-CENTRE "]}
        subset = config_from_dict(raw)
        self.assertEqual(subset.locations.store_codes, ("HCM-CENTRE", "HN-CENTRE"))
        self.assertEqual(subset.locations.counts, {"cities": 2, "stores": 2})
        self.assertNotEqual(subset.logical_identity, config.logical_identity)
        self.assertEqual(
            subset.logical_identity,
            replace(subset, locations=LocationConfig(("HN-CENTRE", "HCM-CENTRE"))).logical_identity,
        )

    def test_invalid_location_settings_fail_before_export(self):
        for value in (
            [],
            None,
            "all",
            {"count": 2},
            {"store_codes": "HCM-CENTRE"},
            {"store_codes": ["UNKNOWN"]},
            {"store_codes": ["HN-CENTRE", "hn-centre"]},
            {"store_codes": [1]},
        ):
            raw = yaml.safe_load(CONFIG.read_text())
            raw["locations"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                config_from_dict(raw)

    def test_selection_flows_into_sql_metadata_and_manifest(self):
        config = replace(
            load_config(CONFIG),
            locations=LocationConfig(("HCM-NORTHEAST",)),
            scale=dict(customers=10, products=2, variants=8, orders=20),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dataset.sql"
            summary = export_sql(config, path)
            sql = path.read_text()
            metadata = json.loads(sql.splitlines()[1].removeprefix("-- import_metadata: "))
            manifest = json.loads(run(config, Path(directory)).read_text())
        self.assertEqual((summary.cities, summary.stores), (1, 1))
        self.assertEqual(metadata["locations"], {"store_codes": ["HCM-NORTHEAST"]})
        self.assertEqual(metadata["counts"]["stores"], 1)
        self.assertEqual(manifest["location_counts"], {"cities": 1, "stores": 1})
        self.assertIn("INSERT INTO public.cities", sql)
        self.assertLess(sql.index("INSERT INTO public.cities"), sql.index("INSERT INTO public.stores"))
        self.assertNotIn("HN-CENTRE", sql)

    def test_empty_selection_generates_no_location_inserts(self):
        config = replace(
            load_config(CONFIG), locations=LocationConfig(()), scale=dict(customers=1, products=1, variants=1, orders=0)
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dataset.sql"
            summary = export_sql(config, path)
            sql = path.read_text()
        self.assertEqual((summary.cities, summary.stores), (0, 0))
        self.assertNotIn("INSERT INTO public.cities", sql)
        self.assertNotIn("INSERT INTO public.stores", sql)
