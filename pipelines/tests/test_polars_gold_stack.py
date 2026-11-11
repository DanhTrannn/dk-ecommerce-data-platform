"""Opt-in all-27-Gold parity, persistent gates and immutable publication acceptance."""

import json
import os
import unittest
from datetime import UTC, datetime

from polars_checks.fixture import PolarsStack


@unittest.skipUnless(os.environ.get("RUN_POLARS_GOLD_STACK_TESTS") == "1", "opt-in private native Gold publication")
class NativeGoldStackTests(unittest.TestCase):
    def test_gold_parity_and_failed_publication_recovery(self):
        stack = PolarsStack()
        self.addCleanup(stack.close)

        def native(mode, *args, check=True, script="gold_checks.py"):
            response = stack.command(
                ["run", "--rm", "--no-deps", "-T", "polars-pilot", "/polars-checks/" + script, mode, *args],
                timeout=900,
                check=check,
            )
            return stack.result(response.stdout) if response.returncode == 0 else response

        def reference(mode, run_id, observed, *args):
            return stack.result(
                stack.command(
                    [
                        "run",
                        "--rm",
                        "--no-deps",
                        "-T",
                        "spark-client",
                        "/opt/spark/bin/spark-submit",
                        "--conf",
                        "spark.sql.shuffle.partitions=2",
                        "/polars-checks/gold_reference.py",
                        mode,
                        run_id,
                        observed,
                        *args,
                    ],
                    timeout=1500,
                ).stdout
            )

        def parity(expected):
            actual = native("fingerprints")
            self.assertEqual(set(actual), set(expected))
            for name in expected:
                self.assertEqual(actual[name], expected[name], name)
            return {name: value["count"] for name, value in actual.items()}

        def pinned(manifest):
            snapshots = json.loads(manifest["snapshots_json"])
            name = "lakehouse.polars_batch_acceptance_gold.mart_order_daily"
            return stack.query(
                f"SELECT sum(total_orders) orders,sum(order_total_vnd) placed,sum(net_revenue_vnd) revenue FROM {name} FOR VERSION AS OF {snapshots[name]}"
            )[0]

        stack.start_source()
        stack.up("trino")
        stack.result(
            stack.command(["exec", "-T", "platform-app", "python", "/polars-checks/seed_operations.py"]).stdout
        )
        native("run", "initial", script="batch_checks.py")
        observed = datetime.now(UTC).isoformat()
        initial = native("build", "gold_initial", observed)
        self.assertEqual(initial["quality"]["passed_checks"], 55)
        self.assertEqual(len(initial["snapshots"]), 54)
        manifest = initial["publication"]
        self.assertEqual(manifest["schema_version"], 3)
        self.assertEqual(manifest["source_mode"], "batch")
        before = pinned(manifest)
        expected = reference("initial", "gold_initial", observed)
        self.assertEqual(expected["quality"]["passed_checks"], 55)
        stack.evidence["initial_parity"] = parity(expected["gold"])
        stack.evidence["initial_quality"] = initial["quality"]
        print("GOLD_PROGRESS 27-table/55-check initial parity passed", flush=True)
        native("corrupt")
        bad = native("verify", "gold_initial", observed, check=False)
        self.assertNotIsInstance(bad, dict)
        self.assertRegex(bad.stdout + bad.stderr, "quality gate failed")
        self.assertIn("additive_order_mart_counts", native("audit")["failed_checks"])
        self.assertEqual(native("publication")["latest"]["snapshots_json"], manifest["snapshots_json"])
        self.assertEqual(pinned(manifest), before)
        recovered = native("build", "gold_initial", observed)
        self.assertEqual(recovered["quality"]["passed_checks"], 55)
        self.assertEqual(native("publication")["count"], 1)
        self.assertEqual(pinned(manifest), before)
        audited = native("verify", "gold_initial", observed)
        self.assertIsNone(audited["publication"])
        self.assertEqual(native("publication")["count"], 1)
        stack.evidence["failed_gate_preserved_publication"] = True
        stack.evidence["same_run_recovery"] = True
        stack.evidence["verify_only_does_not_publish"] = True
        native("history", observed)
        traffic = stack.runtime("purchases")
        native("run", "interval", traffic["interval_start"], traffic["interval_end"], script="batch_checks.py")
        observed2 = datetime.now(UTC).isoformat()
        current = native("build", "gold_interval", observed2)
        expected = reference("interval", "gold_interval", observed2, traffic["interval_start"], traffic["interval_end"])
        stack.evidence["interval_parity"] = parity(expected["gold"])
        self.assertEqual(current["quality"]["passed_checks"], 55)
        self.assertEqual(native("publication")["count"], 2)
        after = pinned(current["publication"])
        self.assertEqual(after["orders"], before["orders"] + 2)
        self.assertEqual(after["revenue"], before["revenue"])
        self.assertEqual(pinned(manifest), before)
        self.assertEqual(
            stack.query(
                "SELECT count(DISTINCT snapshot_date) n FROM lakehouse.polars_batch_acceptance_gold.fact_inventory_daily_snapshot"
            )[0]["n"],
            2,
        )
        self.assertEqual(
            stack.query(
                "SELECT count(DISTINCT snapshot_date) n FROM lakehouse.polars_batch_acceptance_gold.fact_inventory_current"
            )[0]["n"],
            1,
        )
        stack.evidence["pinned_totals"] = {"initial": before, "current": after}
        stack.evidence["history_preserved"] = True
        stack.evidence["interval_quality"] = current["quality"]
        stack.evidence["status"] = "passed"
