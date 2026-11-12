"""Opt-in acceptance of real native, dbt, streaming and scheduler production paths."""

import os
import unittest
from datetime import UTC, datetime, timedelta

from native.fixture import NativeStack


@unittest.skipUnless(os.environ.get("RUN_NATIVE_STACK_TESTS") == "1", "opt-in private native/dbt stack")
class NativeStackTests(unittest.TestCase):
    def test_source_dbt_publication(self):
        stack = NativeStack()
        self.addCleanup(stack.close)
        print("NATIVE_REPORT " + str(stack.report), flush=True)
        stack.start_source()
        stack.result(stack.command(["exec", "-T", "platform-app", "python", "/native-checks/seed_operations.py"]).stdout)
        stack.up("trino")
        runtime = stack.native("/native-checks/run.py", "runtime")
        self.assertEqual(runtime, {"pyspark": False, "java": False})
        stack.evidence["runtime"] = runtime
        initial = stack.native("/native-checks/run.py", "initial", "native_initial")
        self.assertEqual(len(initial["tables"]), 27)
        self.assertTrue(all(row["source_rows"] > 0 for row in initial["tables"].values()))
        as_of = datetime.now(UTC).isoformat()
        built = stack.native("/native-checks/run.py", "commerce", "native_initial", as_of)
        self.assertEqual(built["quality"]["passed_checks"], 55)
        stack.evidence["initial_quality"] = built["quality"]
        stack.evidence["dbt"] = built["dbt"]
        oracle = stack.native("/native-checks/run.py", "oracle", "native_initial", as_of)
        for name, value in oracle.items():
            self.assertEqual(value["expected"], value["actual"], name)
        stack.evidence["gold_parity"] = {name: value["actual"]["count"] for name, value in oracle.items()}
        self.assertEqual(stack.native("/native-checks/run.py", "reconcile", "native_reconcile")["passed_checks"], 30)
        first = stack.runtime("trino")
        self.assertEqual(first["mart"]["orders"], 30)
        for sql in (
            "INSERT INTO lakehouse.gold.fact_order(order_id) VALUES(-1)",
            "DROP TABLE lakehouse.gold.fact_order",
            "SELECT * FROM lakehouse_etl.gold.fact_order LIMIT 1",
        ):
            self.assertTrue(stack.runtime("query-denied", "--sql", sql)["denied"])
        stack.evidence["authorization"] = stack.native("/native-checks/security.py")
        stack.start_streaming()
        traffic = stack.traffic("traffic")
        self.assertEqual(traffic["statuses"], [200, 200, 404])
        purchases = stack.runtime("purchases")
        stack.native("/native-checks/run.py", "batch", "native_incremental", purchases["interval_start"], purchases["interval_end"])
        next_asof = datetime.now(UTC).isoformat()
        self.assertEqual(stack.native("/native-checks/run.py", "commerce", "native_incremental", next_asof)["quality"]["passed_checks"], 55)
        second = stack.runtime("trino")
        self.assertEqual(second["mart"]["orders"], 32)
        self.assertEqual(second["mart"]["net_revenue_vnd"], first["mart"]["net_revenue_vnd"])
        self.assertEqual(second["mart"]["order_total_vnd"], first["mart"]["order_total_vnd"] + sum(order["total_vnd"] for order in purchases["orders"]))
        old_snapshot = first["snapshots"]["lakehouse.gold.mart_order_daily"]
        self.assertEqual(stack.query(f"SELECT sum(total_orders) n FROM lakehouse.gold.mart_order_daily FOR VERSION AS OF {old_snapshot}")[0]["n"], 30)
        stack.native("/native-checks/run.py", "corrupt")
        failure = stack.native("/native-checks/run.py", "verify", "native_failed", next_asof, check=False)
        self.assertNotEqual(failure.returncode, 0)
        self.assertIn("quality", failure.stdout.lower() + failure.stderr.lower())
        self.assertEqual(stack.runtime("trino")["mart"], second["mart"])
        stack.native("/native-checks/run.py", "commerce", "native_incremental", next_asof)
        self.assertEqual(stack.native("/native-checks/run.py", "publication")["count"], 2)
        stack.evidence["incremental"] = dict(before=first["mart"], after=second["mart"], failed_gate_retained_publication=True, retry_publications=2)
        stack.traffic("inject")
        stack.wait("raw replay and invalid envelope landed", lambda: stack.query("SELECT count(*) n FROM lakehouse.landing.access_logs WHERE raw_payload='invalid-native-log-event'")[0]["n"] == 1)
        log_asof = datetime.now(UTC).isoformat()
        logs = stack.native("/native-checks/run.py", "logs", "native_logs", log_asof)
        self.assertEqual(len(logs["checks"]), 15)
        self.assertTrue(all(row["status"] == "PASSED" for row in logs["checks"]))
        self.assertGreaterEqual(logs["events"], 21)
        replay = stack.native("/native-checks/run.py", "logs", "native_logs", log_asof)
        self.assertEqual(replay["events"], logs["events"])
        self.assertEqual(stack.query("SELECT count(*) n FROM lakehouse.system.logs_analytics_publications")[0]["n"], 1)
        stack.evidence["logs"] = dict(events=logs["events"], passed_checks=15, replay_idempotent=True)
        self.check_hard_delete(stack)
        stack.evidence["maintenance"] = stack.native("/native-checks/run.py", "maintenance")
        self.check_scheduler(stack, purchases)
        stack.evidence["status"] = "passed"
        print("NATIVE_STACK_OK " + str(stack.report / "report.json"), flush=True)

    def check_hard_delete(self, stack):
        # Exercise a real Debezium delete, with a future observation only for the 30-day age policy.
        key = int(stack.sql("POSTGRES_ECOMMERCE_DB", "SELECT min(wishlist_item_id) FROM wishlist_items;"))
        stack.sql("POSTGRES_ECOMMERCE_DB", f"UPDATE wishlist_items SET is_present=false,removed_at=timezone('UTC',now()),updated_at=timezone('UTC',now()) WHERE wishlist_item_id={key};")
        stack.native("/native-checks/run.py", "initial", "native_before_delete")
        stack.sql("POSTGRES_ECOMMERCE_DB", f"DELETE FROM wishlist_items WHERE wishlist_item_id={key};")
        stack.wait("real CDC delete captured", lambda: stack.query(f"SELECT count(*) n FROM lakehouse.bronze.oltp_cdc_events WHERE table_name='wishlist_items' AND operation='d' AND json_extract_scalar(before_payload,'$.wishlist_item_id')='{key}'")[0]["n"] >= 1)
        now = datetime.now(UTC)
        recent = stack.native("/native-checks/run.py", "hard-delete", "native_delete", now.isoformat())
        self.assertEqual(recent["wishlist_items"], 0)
        old = stack.native("/native-checks/run.py", "hard-delete", "native_delete", (now + timedelta(days=31)).isoformat())
        self.assertEqual(old["wishlist_items"], 1)
        self.assertEqual(stack.query(f"SELECT count(*) n FROM lakehouse.silver.silver_wishlist_items WHERE wishlist_item_id={key}")[0]["n"], 0)
        stack.evidence["cdc"] = dict(real_delete_captured=True, recent_delete_skipped=True, eligible_delete_applied=True)

    def check_scheduler(self, stack, purchases):
        stack.up("airflow-api-server", "airflow-dag-processor", "airflow-scheduler")
        dags = {
            "init_lakehouse_oltp": 2,
            "lakehouse_oltp_pipeline": 2,
            "lakehouse_logs_marts": 1,
            "manual_lakehouse_hard_delete": 1,
            "lakehouse_streaming_maintenance": 4,
            "lakehouse_oltp_maintenance": 4,
        }
        evidence = {}
        for dag, tasks in dags.items():
            stack.runtime("airflow", "--action", "wait", "--dag", dag)
            stack.runtime("airflow", "--action", "unpause", "--dag", dag)
            run = "native_acceptance_" + dag
            arguments = ["--action", "trigger", "--dag", dag, "--run-id", run]
            if dag == "lakehouse_oltp_pipeline":
                arguments += ["--lower", purchases["interval_start"], "--upper", purchases["interval_end"]]
            stack.runtime("airflow", *arguments)
            evidence[dag] = stack.wait_run(dag, run, expected_tasks=tasks)
            stack.runtime("airflow", "--action", "pause", "--dag", dag)
        stack.evidence["scheduler"] = evidence
        self.assertEqual(stack.runtime("trino")["mart"]["orders"], 32)
