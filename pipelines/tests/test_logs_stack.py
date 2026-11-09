"""Opt-in real HTTP -> Kafka -> Flink -> Iceberg -> Airflow/Spark -> Trino acceptance."""

import json
import os
import unittest

from logs.fixture import LogsStack


@unittest.skipUnless(os.environ.get("RUN_ACCESS_LOG_STACK_TESTS") == "1", "opt-in private access-log stack")
class AccessLogStackTests(unittest.TestCase):
    def test_observed_traffic_publication_replay_and_failure(self):
        stack = LogsStack()
        try:
            print("LOGS_REPORT " + str(stack.report), flush=True)
            # API starts before the optional broker; requests must still work.
            stack.start_source()
            offline = stack.helper("traffic")
            dlq = stack.helper("dlq")
            self.assertTrue(set(offline["ids"]).issubset(dlq["ids"]))
            stack.evidence["startup_outage"] = dict(http_statuses=offline["statuses"], retained_events=3)
            stack.up(
                "kafka",
                "flink-jobmanager",
                "flink-taskmanager",
                "trino",
                "airflow-api-server",
                "airflow-dag-processor",
                "airflow-scheduler",
            )
            stack.command(["run", "--rm", "--no-deps", "-T", "kafka-init"])
            stack.command(["run", "--rm", "--no-deps", "-T", "flink-submit"], timeout=300)
            jobs = stack.helper("flink", "/jobs/overview")["jobs"]
            active = [job for job in jobs if job["state"] == "RUNNING"]
            self.assertEqual(len(active), 1, jobs)
            job_id = active[0]["jid"]
            stack.evidence["flink_job_id"] = job_id
            online = stack.helper("traffic")
            ids = ",".join("'" + value + "'" for value in online["ids"])
            stack.wait(
                "API automatically reconnected and Landing visible",
                lambda: (
                    stack.query(f"SELECT count(*) AS n FROM lakehouse.landing.access_logs WHERE event_id IN ({ids})")[
                        0
                    ]["n"]
                    == 3
                ),
            )
            # Capture actual cart/checkout workflows, rather than fabricated access envelopes.
            stack.evidence["purchases"] = stack.runtime("purchases")
            stack.wait(
                "checkout events captured",
                lambda: (
                    stack.query(
                        "SELECT count(*) AS n FROM lakehouse.landing.access_logs WHERE ecommerce_action='checkout_submit' AND http_status_code<400"
                    )[0]["n"]
                    == 2
                ),
            )
            stack.evidence["injected_transport_faults"] = stack.helper("inject", json.dumps(online["ids"]))
            stack.wait(
                "replay and malformed payload captured",
                lambda: (
                    stack.query(
                        "SELECT count(*) AS n FROM lakehouse.landing.access_logs WHERE raw_payload='not-json-acceptance'"
                    )[0]["n"]
                    == 1
                ),
            )
            stack.evidence["catalog_bootstrap"] = stack.spark_logs("catalog", "logs-source-catalog")
            dag = "lakehouse_logs_marts"
            stack.runtime("airflow", "--action", "wait", "--dag", dag)
            stack.runtime("airflow", "--action", "unpause", "--dag", dag)
            run = stack.project + "_logs"
            stack.runtime("airflow", "--action", "trigger", "--dag", dag, "--run-id", run)
            stack.evidence["log_dag"] = stack.wait_run(dag, run, timeout=600, expected_tasks=1)
            stack.runtime("airflow", "--action", "pause", "--dag", dag)
            publication = stack.helper("reader")
            self.assertEqual(publication["run_id"], "logs-" + run)
            self.assertEqual(len(publication["snapshots"]), 3)
            self.assertTrue(all(value is not None for value in publication["snapshots"].values()))
            for table in (
                "lakehouse.bronze.web_events",
                "lakehouse.silver.silver_logs",
                "lakehouse.gold.fact_web_events",
            ):
                self.assertEqual(stack.query(f"SELECT count(*) AS n FROM {table} WHERE event_id IN ({ids})")[0]["n"], 3)
            fact_count = stack.query("SELECT count(*) AS n FROM lakehouse.gold.fact_web_events")[0]["n"]
            self.assertGreater(fact_count, 3)
            self.assertEqual(sum(row["total_requests"] for row in publication["routes"]), fact_count)
            self.assertEqual(sum(row["observed_event_count"] for row in publication["funnel"]), fact_count)
            self.assertEqual(sum(row["orders_completed"] for row in publication["funnel"]), 2)
            self.assertTrue(
                any(
                    row["product_id"] == online["product_id"] and row["detail_view_count"] >= 1
                    for row in publication["demand"]
                )
            )
            self.assertEqual(
                sum(row["cart_add_count"] for row in publication["demand"] if row["product_id"] is not None), 2
            )
            self.assertFalse(any(row["product_id"] is None for row in publication["demand"]))
            self.assertEqual(
                stack.query(
                    "SELECT count(*) AS n FROM lakehouse.bronze.web_events WHERE _validation_error IS NOT NULL"
                )[0]["n"],
                1,
            )
            self.assertEqual(
                stack.query(
                    f"SELECT count(*) AS n FROM lakehouse.landing.access_logs WHERE event_id='{online['ids'][0]}'"
                )[0]["n"],
                2,
            )
            self.assertEqual(
                stack.query("SELECT count(*)-count(DISTINCT event_id) AS n FROM lakehouse.gold.fact_web_events")[0][
                    "n"
                ],
                0,
            )
            self.assertEqual(
                stack.query("SELECT DISTINCT data_origin FROM lakehouse.silver.silver_logs"),
                [{"data_origin": "observed"}],
            )
            stack.evidence["published"] = dict(
                run_id=publication["run_id"],
                observed_events=fact_count,
                marts=3,
                rejected_raw=1,
                successful_checkouts=2,
                product_identity_resolved=True,
            )
            stack.evidence["retry"] = stack.spark_logs("replay", "logs-replay")
            self.assertEqual(
                stack.query("SELECT count(*) AS n FROM lakehouse.gold.fact_web_events")[0]["n"], fact_count
            )
            reader = stack.helper("reader")
            self.assertEqual(reader["run_id"], "logs-replay")
            self.assertEqual(
                reader["funnel"][0]["observed_event_count"], publication["funnel"][0]["observed_event_count"]
            )
            before_restart = stack.helper("flink", f"/jobs/{job_id}/checkpoints")["latest"]["completed"]["id"]
            stack.command(["restart", "flink-taskmanager"])
            recovered = stack.helper("traffic")
            recovered_ids = ",".join("'" + value + "'" for value in recovered["ids"])
            stack.wait(
                "Flink recovery keeps capturing",
                lambda: (
                    stack.query(
                        f"SELECT count(*) AS n FROM lakehouse.landing.access_logs WHERE event_id IN ({recovered_ids})"
                    )[0]["n"]
                    == 3
                ),
            )
            after_restart = stack.helper("flink", f"/jobs/{job_id}/checkpoints")["latest"]["completed"]["id"]
            self.assertGreater(after_restart, before_restart)
            stack.evidence["checkpoint_recovery"] = dict(before=before_restart, after=after_restart, new_events=3)
            # New Landing events do not change the last completed published snapshots.
            self.assertEqual(stack.helper("reader")["run_id"], "logs-replay")
            stack.evidence["quality_failure"] = stack.spark_logs("conflict", "logs-conflicting-identity")
            self.assertEqual(stack.helper("reader")["run_id"], "logs-replay")
            self.assertEqual(stack.helper("reader")["funnel"], reader["funnel"])
            self.assertEqual(
                stack.query("SELECT count(*) AS n FROM lakehouse.system.analytics_publications")[0]["n"], 1
            )
            http = stack.helper("http-report")
            self.assertEqual(http["source_run_id"], "logs-replay")
            self.assertEqual(http["data_source"], "lakehouse")
            self.assertFalse(http["served_from_cache"])
            self.assertEqual(http["totals"]["total_requests"], fact_count)
            stack.evidence["http_report"] = http
            stack.evidence["status"] = "passed"
            print("ACCESS_LOG_STACK_OK " + str(stack.report / "report.json"), flush=True)
        finally:
            stack.close()
