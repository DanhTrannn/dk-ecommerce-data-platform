import csv
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import yaml

from generator.reference_input import COLUMNS
from generator.reference_profile import build_reference_profile, write_reference_profile
from generator.cli import main


class ReferenceProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.csv"
        self.spec = self.root / "spec.yml"
        rows = [
            ["1", "12345", "Gift", "2", "2020-01-02T10:00:00", "10", "1", "United Kingdom"],
            ["1", "12345", "Gift", "2", "2020-01-02T10:00:00", "10", "1", "United Kingdom"],
            ["1", "12345", "Gift", "3", "2020-01-02T10:00:00", "12", "1", "United Kingdom"],
            ["1", "12346", "", "1", "2020-01-02T10:00:00", "10", "1", "United Kingdom"],
            ["1", "POST", "Postage", "1", "2020-01-02T10:00:00", "2", "1", "United Kingdom"],
            ["2", "12345", "Gift", "1", "2020-01-05T10:00:00", "10", "1", "United Kingdom"],
            ["3", "12346", "Gift", "1", "2020-01-06T10:00:00", "10", "", "United Kingdom"],
            ["4", "12345", "Gift", "5", "2020-02-01T00:00:00", "10", "1", "United Kingdom"],
            ["5", "12347", "Gift", "2", "2020-02-05T00:00:00", "10", "1", "United Kingdom"],
            ["6", "12345", "Gift", "1", "2020-01-02T10:00:00", "10", "1", "Germany"],
            ["7", "12345", "Gift", "1", "2020-03-01T00:00:00", "10", "1", "United Kingdom"],
            ["8", "12345", "Gift", "1", "2020-01-02T10:00:00", "10", "2", "United Kingdom"],
            ["8", "12346", "Gift", "1", "2020-01-02T10:00:00", "10", "3", "United Kingdom"],
            ["C9", "12345", "Credit", "-1", "2020-01-02T10:00:00", "10", "1", "United Kingdom"],
            ["10", "12345", "Adjustment", "-1", "2020-01-02T10:00:00", "10", "1", "United Kingdom"],
            ["11", "12345", "Gift", "1", "2020-01-02T10:00:00", "0", "1", "United Kingdom"],
        ]
        with self.source.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(COLUMNS)
            writer.writerows(rows)
        self.settings = {
            "profile_version": "fixture-v1",
            "source": {"sha256": hashlib.sha256(self.source.read_bytes()).hexdigest(), "name": "local fixture"},
            "cohort": {
                "country": "United Kingdom",
                "stock_code_pattern": r"[0-9]{5}[A-Za-z]{0,2}",
                "duplicate_policy": "retain",
            },
            "windows": {
                "fit": {"start": "2020-01-01T00:00:00", "end": "2020-02-01T00:00:00"},
                "evaluation": {"start": "2020-02-01T00:00:00", "end": "2020-03-01T00:00:00"},
            },
        }
        self.save_spec()

    def save_spec(self):
        self.spec.write_text(yaml.safe_dump(self.settings))

    def test_basket_and_quantity_use_invoice_sku_grain_without_silent_dedup(self):
        profile = build_reference_profile(self.source, self.spec)
        fit = profile["periods"]["fit"]
        self.assertEqual(fit["invoices"], 3)
        self.assertEqual(fit["basket_distinct_skus"]["histogram"], {"1": 2, "2": 1})
        self.assertEqual(fit["quantity_per_invoice_sku"]["histogram"], {"1": 3, "7": 1})
        self.assertEqual(profile["audit"]["suspected_duplicate_rows_raw"], 1)
        self.assertEqual(profile["audit"]["suspected_duplicate_rows_kept"], 1)

    def test_missing_customer_keeps_basket_but_cannot_become_repeat_customer(self):
        fit = build_reference_profile(self.source, self.spec)["periods"]["fit"]
        self.assertEqual(fit["invoices_without_customer"], 1)
        repeat = fit["repeat_purchase"]
        self.assertEqual(repeat["identified_customers"], 1)
        self.assertEqual(repeat["orders_per_identified_customer"]["histogram"], {"2": 1})
        self.assertEqual(repeat["repeat_customer_rate"], 1)
        self.assertEqual(repeat["observed_interpurchase_gap_days"]["mean"], 3)

    def test_evaluation_boundary_and_customer_history_are_separate(self):
        profile = build_reference_profile(self.source, self.spec)
        evaluation = profile["periods"]["evaluation"]
        self.assertEqual(evaluation["invoices"], 2)
        self.assertEqual(evaluation["quantity_per_invoice_sku"]["histogram"], {"2": 1, "5": 1})
        self.assertEqual(evaluation["repeat_purchase"]["orders_per_identified_customer"]["histogram"], {"2": 1})

    def test_audit_conserves_rows_and_quarantines_conflicting_invoices(self):
        audit = build_reference_profile(self.source, self.spec)["audit"]
        excluded = audit["excluded_rows_by_primary_reason"]
        self.assertEqual(audit["raw_rows"], audit["kept_rows"] + sum(excluded.values()))
        self.assertEqual(excluded["invoice_metadata_conflict"], 2)
        self.assertEqual(excluded["credit_or_cancellation"], 1)
        self.assertEqual(excluded["nonmerchandise_or_nonstandard_code"], 1)
        self.assertEqual(excluded["other_country"], 1)
        self.assertEqual(excluded["outside_windows"], 1)

    def test_concentration_uses_units_not_revenue(self):
        concentration = build_reference_profile(self.source, self.spec)["periods"]["fit"]["product_concentration"]
        self.assertEqual(concentration["sku_units_ranked"], [8, 2])
        self.assertEqual(concentration["top_sku_fractions"]["0.1"], {"sku_count": 1, "unit_share": 0.8})

    def test_profile_is_reproducible_and_does_not_publish_identifiers(self):
        output = self.root / "profile.json"
        first = write_reference_profile(self.source, self.spec, output)
        content = output.read_bytes()
        second = write_reference_profile(self.source, self.spec, output)
        self.assertEqual(first, second)
        self.assertEqual(content, output.read_bytes())
        self.assertNotIn('"12345"', json.dumps(first))
        self.assertNotIn('"customer_id"', json.dumps(first))

    def test_changed_policy_changes_identity(self):
        first = build_reference_profile(self.source, self.spec)["profile_id"]
        self.settings["cohort"]["country"] = "Germany"
        self.save_spec()
        self.assertNotEqual(first, build_reference_profile(self.source, self.spec)["profile_id"])

    def test_checksum_mismatch_fails_without_overwriting_report(self):
        output = self.root / "profile.json"
        output.write_text("previous report")
        self.source.write_text("changed source")
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            write_reference_profile(self.source, self.spec, output)
        self.assertEqual(output.read_text(), "previous report")

    def test_empty_cohort_returns_null_measures(self):
        self.settings["cohort"]["country"] = "absent country"
        self.save_spec()
        fit = build_reference_profile(self.source, self.spec)["periods"]["fit"]
        self.assertEqual(fit["invoices"], 0)
        self.assertIsNone(fit["basket_distinct_skus"]["mean"])
        self.assertIsNone(fit["repeat_purchase"]["repeat_customer_rate"])

    def test_overlapping_windows_are_rejected(self):
        self.settings["windows"]["evaluation"]["start"] = "2020-01-20T00:00:00"
        self.save_spec()
        with self.assertRaisesRegex(ValueError, "overlap"):
            build_reference_profile(self.source, self.spec)

    def test_output_cannot_replace_source_or_spec(self):
        for path in (self.source, self.spec):
            with self.subTest(path=path), self.assertRaises(ValueError):
                write_reference_profile(self.source, self.spec, path)

    def test_malformed_values_are_audited_instead_of_becoming_measurements(self):
        cases = (
            ("Quantity", "NaN", "invalid_quantity"),
            ("Quantity", "1.5", "invalid_quantity"),
            ("UnitPrice", "Infinity", "invalid_price"),
            ("InvoiceDate", "2020-01-02T10:00:00+00:00", "invalid_invoice_date"),
            ("CustomerID", "unknown", "invalid_customer_id"),
            ("Country", "", "missing_country"),
        )
        with self.source.open("a", newline="") as stream:
            writer = csv.writer(stream)
            for field, value, _ in cases:
                row = dict(
                    zip(COLUMNS, ["99", "12345", "Gift", "1", "2020-01-02T10:00:00", "10", "1", "United Kingdom"])
                )
                row[field] = value
                writer.writerow(row.values())
        self.settings["source"]["sha256"] = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.save_spec()
        profile = build_reference_profile(self.source, self.spec)
        excluded = profile["audit"]["excluded_rows_by_primary_reason"]
        self.assertEqual(excluded["invalid_quantity"], 2)
        for _, _, reason in cases[2:]:
            self.assertEqual(excluded[reason], 1)
        self.assertEqual(profile["periods"]["fit"]["invoices"], 3)

    def test_cli_builds_profile_without_loading_a_generation_config(self):
        output = self.root / "profile.json"
        arguments = [
            "generator",
            "profile-reference",
            "--input",
            str(self.source),
            "--spec",
            str(self.spec),
            "--output",
            str(output),
        ]
        with patch("sys.argv", arguments), redirect_stdout(io.StringIO()) as stdout:
            main()
        profile = json.loads(output.read_text())
        self.assertIn(profile["profile_id"], stdout.getvalue())
        self.assertEqual(profile["status"], "measured_reference_not_applied_to_generator")


if __name__ == "__main__":
    unittest.main()
