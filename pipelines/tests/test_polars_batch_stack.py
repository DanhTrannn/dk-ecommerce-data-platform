"""Opt-in all-27-source Bronze/Silver parity with the existing Spark implementation."""

import os
import unittest

from polars_checks.fixture import PolarsStack


@unittest.skipUnless(os.environ.get("RUN_POLARS_BATCH_STACK_TESTS") == "1", "opt-in all-source native batch parity")
class NativeBatchStackTests(unittest.TestCase):
    def test_full_interval_replay_privacy_and_quarantine(self):
        stack = PolarsStack()
        self.addCleanup(stack.close)

        def native(mode, *args, check=True):
            response = stack.command(
                ["run", "--rm", "--no-deps", "-T", "polars-pilot", "/polars-checks/batch_checks.py", mode, *args],
                timeout=900,
                check=check,
            )
            return stack.result(response.stdout) if response.returncode == 0 else response

        def reference(mode, *args):
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
                        "/polars-checks/batch_reference.py",
                        mode,
                        *args,
                    ],
                    timeout=1200,
                ).stdout
            )

        def parity(expected):
            actual = native("fingerprints")
            self.assertEqual(set(actual), set(expected))
            for name in expected:
                if name == "quarantine" and actual[name] != expected[name]:
                    print("NATIVE_QUARANTINE_DIFF " + str(native("quarantine-diff")), flush=True)
                self.assertEqual(actual[name], expected[name], name)
            return {name: row["count"] for name, row in actual.items()}

        stack.start_source()
        stack.up("trino")
        stack.evidence["source_operations"] = stack.result(
            stack.command(["exec", "-T", "platform-app", "python", "/polars-checks/seed_operations.py"]).stdout
        )
        stack.sql(
            "POSTGRES_ECOMMERCE_DB",
            """UPDATE return_requests SET image_urls='[ \"https://example.test/ảnh.png\", null ]'::json
            WHERE return_id=(SELECT min(return_id) FROM return_requests)""",
        )
        print("NATIVE_PROGRESS source ready", flush=True)
        initial = native("run", "initial")
        self.assertEqual(len(initial["tables"]), 27)
        self.assertTrue(all(row["source_rows"] > 0 for row in initial["tables"].values()))
        self.assertTrue(all(row["source_rows"] == row["silver_rows"] for row in initial["tables"].values()))
        stack.evidence["initial"] = initial
        expected = reference("initial")
        stack.evidence["initial_parity"] = parity(expected)
        print("NATIVE_PROGRESS 27-table initial parity passed", flush=True)
        repeated = native("run", "replay")
        for name, row in repeated["tables"].items():
            self.assertEqual(row["bronze_rows"], initial["tables"][name]["bronze_rows"], name)
        parity(expected)
        empty = native("run", "empty", "2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z")
        self.assertTrue(all(row["source_rows"] == 0 for row in empty["tables"].values()))
        parity(expected)
        snapshot = stack.query(
            "SELECT snapshot_id FROM lakehouse.polars_batch_acceptance_silver.\"silver_orders$refs\" WHERE name='main'"
        )[0]["snapshot_id"]
        traffic = stack.runtime("purchases")
        changed = native("run", "interval", traffic["interval_start"], traffic["interval_end"])
        expected = reference("interval", traffic["interval_start"], traffic["interval_end"])
        stack.evidence["interval_parity"] = parity(expected)
        self.assertEqual(changed["tables"]["orders"]["silver_rows"], initial["tables"]["orders"]["silver_rows"] + 2)
        self.assertEqual(
            stack.query(
                f"SELECT count(*) n FROM lakehouse.polars_batch_acceptance_silver.silver_orders FOR VERSION AS OF {snapshot}"
            )[0]["n"],
            initial["tables"]["orders"]["silver_rows"],
        )
        native("backfill")
        parity(expected)
        print("NATIVE_PROGRESS interval/replay/empty/backfill passed", flush=True)
        expected = reference("faults")
        native("faults")
        stack.evidence["quarantine_parity"] = parity(expected)
        self.assertGreater(expected["quarantine"]["count"], 0)
        native("faults")
        parity(expected)
        stack.evidence["quarantine_replay"] = True
        native("legacy")
        rejected = native("run", "legacy_denied", check=False)
        self.assertNotIsInstance(rejected, dict)
        self.assertRegex(rejected.stdout + rejected.stderr, "legacy or unclassified")
        native("restore")
        parity(expected)
        stack.evidence["legacy_guard"] = True
        system = {row["Table"] for row in stack.query("SHOW TABLES FROM lakehouse.system")}
        self.assertTrue(system.isdisjoint({"analytics_publications", "logs_analytics_publications"}))
        stack.evidence["no_publication_written"] = True
        stack.evidence["status"] = "passed"
