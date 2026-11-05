import unittest
from pathlib import Path

from generator_console.reference_profile import load_profile

REPO_ROOT = Path(__file__).resolve().parents[3]
PROFILE = REPO_ROOT / "generator" / "profiles" / "uci-online-retail-uk-v1.json"


class ReferenceProfileTests(unittest.TestCase):
    def test_loads_measured_profile(self):
        profile = load_profile(PROFILE)
        self.assertEqual(profile["profile_version"], "uci-online-retail-uk-v1")
        self.assertEqual(profile["status"], "measured_reference_not_applied_to_generator")
        self.assertEqual(profile["source"]["name"], "UCI Online Retail")
        self.assertEqual(profile["source"]["license"], "CC BY 4.0")
        self.assertEqual(profile["audit"]["raw_rows"], 541909)
        self.assertTrue(profile["profile_id"])


if __name__ == "__main__":
    unittest.main()
