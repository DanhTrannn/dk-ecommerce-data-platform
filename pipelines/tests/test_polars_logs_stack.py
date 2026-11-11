"""Private native rollup acceptance over actual API envelopes and Spark facts."""

import json
import os
import unittest
from datetime import UTC, datetime

from polars_checks.fixture import PolarsStack


@unittest.skipUnless(os.environ.get("RUN_POLARS_LOGS_STACK_TESTS") == "1", "opt-in private native log marts")
class NativeLogsStackTests(unittest.TestCase):
    def test_native_rollups_publication_recovery_and_frozen_inputs(self):
        stack = PolarsStack()
        self.addCleanup(stack.close)

        def native(mode, *args, script="logs_checks.py", check=True):
            response = stack.command(
                ["run", "--rm", "--no-deps", "-T", "polars-pilot", "/polars-checks/" + script, mode, *args],
                check=check,
                timeout=900,
            )
            return stack.result(response.stdout) if response.returncode == 0 else response

        def reference(mode, run_id, observed):
            response = stack.command(
                [
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "spark-client",
                    "/opt/spark/bin/spark-submit",
                    "--conf",
                    "spark.sql.shuffle.partitions=2",
                    "/polars-checks/logs_reference.py",
                    mode,
                    run_id,
                    observed,
                ],
                timeout=900,
            )
            return stack.result(response.stdout)

        def pinned_count(manifest):
            snapshots = json.loads(manifest["snapshots_json"])
            name = "lakehouse.polars_batch_acceptance_gold.mart_hourly_route_metrics"
            return stack.query(
                f"SELECT coalesce(sum(total_requests),0) n FROM {name} FOR VERSION AS OF {snapshots[name]}"
            )[0]["n"]

        stack.start_source()
        stack.up("trino")
        traffic = stack.helper("traffic")
        stack.evidence["api_statuses"] = traffic["statuses"]
        stack.evidence["purchases"] = stack.runtime("purchases")
        # The handoff is a fixture, not Kafka/Flink replay acceptance. Raw envelopes stay private.
        payloads = stack.result(
            stack.command(
                ["exec", "-T", "platform-app", "python", "/polars-checks/logs_traffic.py"], private=True
            ).stdout
        )["payloads"]
        stack.evidence["actual_api_payloads"] = len(payloads)
        # The parent temp directory is private; only this file is bound read-only for Spark's user.
        transport = stack.work / "events.json"
        transport.write_text(json.dumps(payloads))
        transport.chmod(0o644)
        override = stack.work / "log-transport.yml"
        override.write_text(
            json.dumps({"services": {"spark-client": {"volumes": [f"{transport}:/native-log-events.json:ro"]}}})
        )
        stack.base.extend(["-f", str(override)])
        native("run", "initial", script="batch_checks.py")
        observed = datetime.now(UTC).isoformat()
        native("build", "catalog_initial", observed, script="gold_checks.py")
        oltp = native("oltp-publication")
        expected = reference("initial", "logs_initial", observed)
        self.assertEqual(expected["rejected"], 1)
        first = native("build", "logs_initial", observed)
        self.assertEqual(first["events"], expected["events"])
        self.assertEqual(len(first["checks"]), 15)
        self.assertTrue(all(c["status"] == "PASSED" for c in first["checks"]))
        self.assertEqual(native("fingerprints"), expected["fingerprints"])
        manifest = first["publication"]
        self.assertEqual(len(json.loads(manifest["snapshots_json"])), 3)
        self.assertEqual(len(json.loads(manifest["source_snapshots_json"])), 4)
        self.assertEqual(pinned_count(manifest), first["events"])
        self.assertGreater(first["events"], 3)
        stack.evidence["initial_spark_parity"] = expected
        print("NATIVE_LOG_PROGRESS actual API/Spark/native parity passed", flush=True)

        native("build", "logs_initial", observed)
        self.assertEqual(native("publication")["count"], 1)
        native("corrupt")
        failed = native("verify", "logs_initial", observed, check=False)
        self.assertNotIsInstance(failed, dict)
        self.assertIn("quality gate failed", failed.stdout + failed.stderr)
        self.assertIn("route_event_conservation", native("audit")["failed_checks"])
        self.assertEqual(pinned_count(manifest), first["events"])
        native("build", "logs_initial", observed)
        last = native("publication")
        self.assertEqual(last["count"], 1)
        self.assertIsNone(native("verify", "logs_initial", observed)["publication"])
        duplicate = native("duplicate")
        bad = native("build", "logs_duplicate", observed, check=False)
        self.assertNotIsInstance(bad, dict)
        self.assertIn("event_identity", bad.stdout + bad.stderr)
        self.assertEqual(native("publication"), last)
        native("restore", str(duplicate["previous_snapshot"]))
        stack.evidence["corruption_recovery_and_duplicate_gate"] = True
        stack.evidence["retry_and_verify_only"] = True

        native("catalog-unpublished")
        native("future", observed)
        frozen = native("build", "logs_catalog", observed)
        self.assertEqual(frozen["events"], first["events"])
        self.assertEqual(native("fingerprints"), expected["fingerprints"])
        self.assertEqual(frozen["catalog_run_id"], "catalog_initial")
        self.assertEqual(native("oltp-publication"), oltp)
        stack.evidence["unpublished_catalog_and_future_event_ignored"] = True

        native("late", observed)
        late = native("build", "logs_late", observed)
        expected_late = reference("current", "logs_late", observed)
        self.assertEqual(native("fingerprints"), expected_late["fingerprints"])
        self.assertEqual(late["events"], first["events"] + 1)
        self.assertEqual(pinned_count(manifest), first["events"])
        stack.evidence["late_spark_parity"] = expected_late

        empty = native("build", "logs_empty", "2000-01-01T00:00:00Z")
        self.assertEqual(empty["events"], 0)
        self.assertIsNone(empty["publication"]["source_event_at"])
        self.assertTrue(all(v["count"] == 0 for v in native("fingerprints").values()))
        self.assertEqual(pinned_count(empty["publication"]), 0)
        self.assertEqual(pinned_count(manifest), first["events"])
        self.assertEqual(native("publication")["count"], 4)
        self.assertEqual(native("oltp-publication"), oltp)
        stack.evidence["empty_clears_current_and_retains_old_publication"] = True
        stack.evidence["status"] = "passed"
