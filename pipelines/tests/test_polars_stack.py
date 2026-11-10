"""Opt-in PostgreSQL/Polaris/MinIO/Spark/Flink/Trino compatibility pilot, private resources only."""

import os
import unittest

from polars_checks.fixture import PolarsStack


@unittest.skipUnless(os.environ.get("RUN_POLARS_STACK_TESTS") == "1", "opt-in private Polars compatibility stack")
class PolarsStackTests(unittest.TestCase):
    def test_categories_replay_and_cross_engine_io(self):
        stack = PolarsStack()
        self.addCleanup(stack.close)
        stack.start_source()
        stack.up("trino")
        stack.evidence["source_denials"] = stack.pilot("source-denial")
        stack.sql(
            "POSTGRES_ECOMMERCE_DB",
            """INSERT INTO categories
            (category_id,public_id,code,name,created_at,updated_at) VALUES
            (90001,'00112233-4455-6677-8899-aabbccddee01','pilot-lower','Lower','2020-01-01','2020-01-01'),
            (90002,'00112233-4455-6677-8899-aabbccddee02','pilot-inside','Inside','2020-01-01 00:00:00.123456','2020-01-01 00:00:00.123456'),
            (90003,'00112233-4455-6677-8899-aabbccddee03','pilot-upper','Upper','2020-01-01 01:00:00','2020-01-01 01:00:00')""",
        )
        print("POLARS_PROGRESS source ready", flush=True)
        first = stack.pilot("run", "initial")
        self.assertEqual(first["bronze_rows"], first["source_rows"])
        self.assertEqual(first["silver_rows"], first["source_rows"])
        self.assertGreater(first["staging_pages"], 1)
        replay = stack.pilot("run", "replay")
        self.assertEqual(replay["bronze_rows"], first["bronze_rows"])
        self.assertEqual(replay["silver_rows"], first["silver_rows"])
        bounded = stack.pilot("run", "bounds", "2020-01-01T07:00:00+07:00", "2020-01-01T08:00:00+07:00")
        self.assertEqual(bounded["source_rows"], 2)
        self.assertEqual(bounded["bronze_rows"], first["bronze_rows"])
        old_snapshot = bounded["silver_snapshot"]
        stack.sql(
            "POSTGRES_ECOMMERCE_DB",
            "UPDATE categories SET name='Polars update' WHERE category_id=(SELECT min(category_id) FROM categories)",
        )
        changed = stack.pilot("run", "changed")
        self.assertEqual(changed["bronze_rows"], first["bronze_rows"] + 1)
        self.assertEqual(changed["silver_rows"], first["silver_rows"])
        current = stack.query(
            "SELECT name FROM lakehouse.polars_pilot_acceptance.silver_categories ORDER BY category_id LIMIT 1"
        )
        self.assertEqual(current[0]["name"], "Polars update")
        old = stack.query(
            f"SELECT name FROM lakehouse.polars_pilot_acceptance.silver_categories FOR VERSION AS OF {old_snapshot} ORDER BY category_id LIMIT 1"
        )
        self.assertNotEqual(old[0]["name"], current[0]["name"])
        empty = stack.pilot("run", "empty", "2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z")
        self.assertEqual(empty["source_rows"], 0)
        self.assertEqual(empty["silver_snapshot"], changed["silver_snapshot"])
        self.assertEqual(empty["bronze_rows"], changed["bronze_rows"])
        self.assertEqual(
            stack.query(
                "SELECT count(*) n FROM lakehouse.polars_pilot_acceptance.silver_categories WHERE length(public_id)<>16"
            )[0]["n"],
            0,
        )
        stack.evidence["categories"] = {
            "initial": first,
            "replay": replay,
            "bounded": bounded,
            "changed": changed,
            "empty": empty,
            "old_snapshot_retained": True,
        }
        print("POLARS_PROGRESS category replay/interval/snapshots passed", flush=True)
        stack.evidence["spark_created"] = stack.spark_compat("create")
        stack.evidence["python_operations"] = stack.pilot("operations")
        stack.evidence["spark_reads_python"] = stack.spark_compat("verify")
        self.assertEqual(
            stack.query("SELECT amount FROM lakehouse.polars_pilot_acceptance.python_probe")[0]["amount"], "99.50"
        )
        stack.permission("revoke")
        try:
            denied = stack.pilot("run", "denied", check=False)
            self.assertNotIsInstance(denied, dict)
            self.assertNotEqual(denied.returncode, 0)
            self.assertRegex((denied.stdout + denied.stderr).lower(), "403|forbidden|denied")
        finally:
            stack.permission("restore")
        self.assertEqual(stack.pilot("run", "restored")["source_rows"], first["source_rows"])
        stack.evidence["writer_denial_and_recovery"] = True
        stack.up("kafka", "flink-jobmanager", "flink-taskmanager")
        stack.command(["run", "--rm", "--no-deps", "-T", "kafka-init"])
        stack.command(["run", "--rm", "--no-deps", "-T", "flink-submit"], timeout=300)
        traffic = stack.helper("traffic")
        ids = ",".join("'" + value + "'" for value in traffic["ids"])
        stack.wait(
            "Flink Landing",
            lambda: (
                stack.query(f"SELECT count(*) n FROM lakehouse.landing.access_logs WHERE event_id IN ({ids})")[0]["n"]
                == 3
            ),
        )
        stack.evidence["flink_reads"] = stack.pilot("flink")
        system_tables = {row["Table"] for row in stack.query("SHOW TABLES FROM lakehouse.system")}
        self.assertTrue(system_tables.isdisjoint({"analytics_publications", "logs_analytics_publications"}))
        stack.evidence["no_publication_written"] = True
        stack.evidence["status"] = "passed"
