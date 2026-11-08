"""Opt-in acceptance of the actual Airflow/Polaris/MinIO/Trino deployment."""

import os
import unittest

from stack.fixture import StackFixture


@unittest.skipUnless(os.environ.get("RUN_LAKEHOUSE_STACK_TESTS") == "1", "opt-in private full-stack acceptance")
class LakehouseStackTests(unittest.TestCase):
    def test_initial_and_incremental_publications(self):
        stack = StackFixture()
        try:
            print("STACK_REPORT " + str(stack.report), flush=True)
            stack.start()
            initial_id = stack.project + "_initial"
            stack.runtime("airflow", "--action", "unpause", "--dag", "init_lakehouse_oltp")
            stack.runtime("airflow", "--action", "trigger", "--dag", "init_lakehouse_oltp", "--run-id", initial_id)
            stack.evidence["initial_dag"] = stack.wait_run("init_lakehouse_oltp", initial_id)
            initial = stack.runtime("trino")
            self.assertEqual(initial["run_id"], initial_id)
            self.assertEqual(initial["fact"]["orders"], stack.evidence["source"]["orders"])
            self.assertEqual(initial["fact"]["order_total_vnd"], stack.evidence["source"]["order_total_vnd"])
            self.assertEqual(initial["mart"]["orders"], initial["fact"]["orders"])
            self.assertEqual(initial["mart"]["order_total_vnd"], initial["fact"]["order_total_vnd"])
            stack.evidence["initial_publication"] = initial
            stack.evidence["initial_reconciliation"] = stack.spark("reconcile")
            stack.spark("permission-table")
            self.assertEqual(
                stack.runtime("query", "--sql", "SELECT count(*) AS n FROM lakehouse.gold.stack_permission_probe")[0][
                    "n"
                ],
                1,
            )
            stack.evidence["trino_write_denials"] = [
                stack.runtime("query-denied", "--sql", sql)
                for sql in (
                    "INSERT INTO lakehouse.gold.stack_permission_probe VALUES (2)",
                    "DROP TABLE lakehouse.gold.stack_permission_probe",
                )
            ]
            traffic = stack.runtime("purchases")
            stack.evidence["runner"] = traffic
            increment_id = stack.project + "_increment"
            stack.runtime("airflow", "--action", "unpause", "--dag", "lakehouse_oltp_pipeline")
            stack.runtime(
                "airflow",
                "--action",
                "trigger",
                "--dag",
                "lakehouse_oltp_pipeline",
                "--run-id",
                increment_id,
                "--lower",
                traffic["interval_start"],
                "--upper",
                traffic["interval_end"],
            )
            stack.evidence["incremental_dag"] = stack.wait_run("lakehouse_oltp_pipeline", increment_id)
            stack.runtime("airflow", "--action", "pause", "--dag", "lakehouse_oltp_pipeline")
            current = stack.runtime("trino")
            self.assertEqual(current["run_id"], increment_id)
            self.assertEqual(current["fact"]["orders"], initial["fact"]["orders"] + 2)
            self.assertEqual(current["mart"]["orders"], current["fact"]["orders"])
            self.assertEqual(
                current["fact"]["order_total_vnd"],
                initial["fact"]["order_total_vnd"] + sum(row["total_vnd"] for row in traffic["orders"]),
            )
            self.assertEqual(current["mart"]["order_total_vnd"], current["fact"]["order_total_vnd"])
            self.assertEqual(current["mart"]["net_revenue_vnd"], initial["mart"]["net_revenue_vnd"])
            stack.evidence["incremental_publication"] = current
            stack.evidence["incremental_reconciliation"] = stack.spark("reconcile")
            # Pinning the old publication still exposes the exact initial dataset.
            old_snapshot = initial["snapshots"]["lakehouse.gold.fact_order"]
            old = stack.runtime(
                "query",
                "--sql",
                f"SELECT count(*) AS n FROM lakehouse.gold.fact_order FOR VERSION AS OF {old_snapshot}",
            )
            self.assertEqual(old[0]["n"], initial["fact"]["orders"])
            # A denied Spark job cannot publish or change the reader's last completed view.
            stack.permission("revoke")
            try:
                stack.evidence["writer_permission_fault"] = stack.denied_writer()
            finally:
                stack.permission("restore")
            self.assertEqual(stack.runtime("trino")["run_id"], increment_id)
            before = stack.spark("counts")
            stack.replay_orders(increment_id + "_retry")
            after = stack.spark("counts")
            self.assertEqual(before, after)
            stack.evidence["replay_counts"] = after
            # Corrupt only this fixture's unversioned fact, then exercise the production gate CLI.
            stack.spark("corrupt")
            stack.evidence["quality_fault"] = stack.failed_gate(increment_id)
            retained = stack.runtime("trino")
            self.assertEqual(retained["run_id"], increment_id)
            self.assertEqual(retained["fact"], current["fact"])
            unpinned = stack.runtime(
                "query", "--sql", "SELECT sum(order_total_vnd) AS total FROM lakehouse.gold.fact_order"
            )
            self.assertNotEqual(unpinned[0]["total"], current["fact"]["order_total_vnd"])
            self.assertEqual(
                stack.runtime(
                    "query",
                    "--sql",
                    f"SELECT count(*) AS n FROM lakehouse.system.analytics_publications WHERE run_id='{increment_id}'",
                )[0]["n"],
                1,
            )
            stack.evidence["restored_quality"] = stack.spark("restore")
            stack.evidence["status"] = "passed"
            print("LAKEHOUSE_STACK_CHECK_OK " + str(stack.report / "report.json"), flush=True)
        finally:
            stack.close()
