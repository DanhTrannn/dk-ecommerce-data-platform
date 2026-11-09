"""CDC contracts without Kafka, Flink, credentials or network access."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace

from lakehouse.streaming.cdc import insert_sql, kafka_source_ddl, topic_pattern
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.streaming.tables import ensure_cdc_tables

ROOT = Path(__file__).resolve().parents[2]


class PostgresCDCTests(unittest.TestCase):
    def test_legacy_schema_upgrade_only_adds_missing_postgres_fields(self):
        fields = {"event_id", "mysql_server_id", "binlog_file", "binlog_position", "binlog_row"}
        calls = []

        def execute(sql):
            calls.append(sql)
            added = re.search(r"ADD \((\w+) (\w+)\)", sql)
            if added:
                fields.add(added.group(1))
            return SimpleNamespace(collect=lambda: [(field,) for field in fields])

        env = SimpleNamespace(execute_sql=execute)
        ensure_cdc_tables(env)
        ensure_cdc_tables(env)
        self.assertEqual(sum(sql.startswith("ALTER TABLE") for sql in calls), 4)
        self.assertIn("binlog_file", fields)
        self.assertTrue({"source_connector", "source_schema", "postgres_lsn", "postgres_tx_id"} <= fields)
        self.assertFalse(any("DROP" in sql for sql in calls))

    def test_only_public_analytical_tables_and_transactions_are_subscribed(self):
        pattern = topic_pattern("retail.test+")
        for table in SOURCE_TABLES:
            self.assertIsNotNone(re.fullmatch(pattern, "retail.test+.public." + table.name))
        self.assertIsNotNone(re.fullmatch(pattern, "retail.test+.transaction"))
        for suffix in ("public.customer_credentials", "private.orders", "ecommerce.orders", "public.orders_extra"):
            self.assertIsNone(re.fullmatch(pattern, "retail.test+." + suffix))
        self.assertIsNone(re.fullmatch(pattern, "retailXtest+.public.orders"))
        with self.assertRaises(ValueError):
            topic_pattern("")

    def test_raw_source_retains_key_payload_and_committed_startup(self):
        ddl = kafka_source_ddl("kafka:9092", "ecommerce", "group'one")
        self.assertIn("'group''one'", ddl)
        self.assertIn("'value.format' = 'raw'", ddl)
        self.assertIn("'key.format' = 'raw'", ddl)
        self.assertIn("'scan.startup.mode' = 'group-offsets'", ddl)
        self.assertIn("'properties.auto.offset.reset' = 'earliest'", ddl)

    def test_insert_preserves_postgres_metadata_and_tombstones(self):
        sql = insert_sql("retail'one")
        self.assertIn("'retail''one'", sql)
        self.assertIn("WHEN payload IS NULL THEN 't'", sql)
        self.assertIn("'$.source.lsn'", sql)
        self.assertIn("'$.source.txId'", sql)
        self.assertIn("'$.source.schema'", sql)
        self.assertNotIn("'$.source.file'", sql)
        self.assertIn("FROM postgres_cdc_kafka", sql)
        self.assertIn("mysql_server_id, binlog_file, binlog_position, binlog_row", sql)

    def test_source_setup_and_connector_match_the_shared_inventory(self):
        expected = {table.name for table in SOURCE_TABLES}
        setup = (ROOT / "infrastructure/postgres/configure-source-cdc.sh").read_text()
        inventory = setup.split("INSERT INTO analytical_sources VALUES", 1)[1].split(";", 1)[0]
        self.assertEqual(set(re.findall(r"\('([a-z_]+)'\)", inventory)), expected)
        self.assertIn("REPLICA IDENTITY FULL", setup)
        self.assertNotIn("pg_drop_replication_slot", setup)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            curl = root / "curl"
            curl.write_text(
                "#!"
                + sys.executable
                + "\n"
                + """import os,sys,json
from pathlib import Path
if '--data' in sys.argv:
    Path(os.environ['TEST_CONFIG_PATH']).write_text(sys.argv[sys.argv.index('--data')+1])
else:
    print(json.dumps({'connector':{'state':'RUNNING'},'tasks':[{'state':'RUNNING'}]}))
"""
            )
            curl.chmod(0o755)
            env = dict(
                os.environ,
                PATH=str(root) + os.pathsep + os.environ["PATH"],
                POSTGRES_CDC_USER="cdc",
                POSTGRES_CDC_PASSWORD='fixture-"\\secret',
                TEST_CONFIG_PATH=str(root / "config.json"),
            )
            subprocess.run(
                ["sh", str(ROOT / "infrastructure/debezium/register-connector.sh")],
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            config = json.loads((root / "config.json").read_text())
            self.assertEqual(config["database.password"], env["POSTGRES_CDC_PASSWORD"])
            self.assertEqual(config["publication.autocreate.mode"], "disabled")
            self.assertTrue(config["tombstones.on.delete"] == "true")
            self.assertEqual(
                {name for name in expected if re.fullmatch(config["table.include.list"], "public." + name)}, expected
            )
            self.assertIsNone(re.fullmatch(config["table.include.list"], "public.customer_credentials"))
