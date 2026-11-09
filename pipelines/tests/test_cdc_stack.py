"""Actual PostgreSQL -> Debezium -> Kafka -> Flink -> Polaris/Iceberg acceptance."""

import json
import os
import unittest
import uuid

from cdc.fixture import CDCStack
from lakehouse.utils.config import SOURCE_TABLES


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_CDC_TESTS") == "1", "opt-in private PostgreSQL CDC stack")
class PostgresCDCStackTests(unittest.TestCase):
    def test_snapshot_changes_and_checkpoint_recovery(self):
        stack = CDCStack()
        try:
            print("CDC_REPORT " + str(stack.report), flush=True)
            stack.start_source()
            stack.sql(
                "POSTGRES_ECOMMERCE_DB",
                f"GRANT {stack.variables['POSTGRES_ECOMMERCE_USER']} TO {stack.variables['POSTGRES_CDC_USER']}",
            )
            stack.sql("POSTGRES_ECOMMERCE_DB", "CREATE DATABASE cdc_setup_missing")
            missing = stack.command(
                [
                    "exec",
                    "-T",
                    "-e",
                    "POSTGRES_ECOMMERCE_DB=cdc_setup_missing",
                    "postgres",
                    "sh",
                    "/opt/project/configure-source-cdc.sh",
                ],
                check=False,
            )
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("Source CDC prerequisites not met", missing.stderr)
            stack.configure_source()
            stack.configure_source()
            rows = json.loads(
                stack.sql(
                    "POSTGRES_ECOMMERCE_DB",
                    """SELECT json_agg(tablename ORDER BY tablename)
                FROM pg_publication_tables WHERE pubname='ecommerce_publication' """,
                )
            )
            self.assertEqual(set(rows), {table.name for table in SOURCE_TABLES})
            self.assertEqual(
                stack.sql(
                    "POSTGRES_ECOMMERCE_DB",
                    """SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relreplident='f' """,
                ),
                "27",
            )
            cdc = stack.variables["POSTGRES_CDC_USER"]
            app = stack.variables["POSTGRES_ECOMMERCE_USER"]
            self.assertEqual(stack.sql("POSTGRES_ECOMMERCE_DB", f"SELECT pg_has_role('{cdc}','{app}','MEMBER')"), "f")
            for statement in (
                "SELECT * FROM customer_credentials LIMIT 1",
                "INSERT INTO customers(public_id,display_name) VALUES(gen_random_uuid(),'denied')",
            ):
                result = stack.command(
                    [
                        "exec",
                        "-T",
                        "postgres",
                        "psql",
                        "-X",
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-U",
                        cdc,
                        "-d",
                        stack.variables["POSTGRES_ECOMMERCE_DB"],
                        "-c",
                        statement,
                    ],
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("permission denied", result.stderr.lower())
            stack.evidence["source_setup"] = {
                "publication_tables": len(rows),
                "full_replica_identity": 27,
                "owner_membership": False,
                "read_write_denials": 2,
                "rerun": "passed",
            }
            stack.command(
                [
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "spark-client",
                    "/opt/spark/bin/spark-submit",
                    "/cdc-checks/legacy.py",
                ],
                timeout=300,
            )
            stack.up("trino", "kafka", "debezium-connect", "flink-jobmanager", "flink-taskmanager")
            stack.command(["run", "--rm", "--no-deps", "-T", "debezium-init"], timeout=180)
            stack.submit()
            job = stack.job()
            snapshot_sql = (
                "SELECT json_build_object("
                + ",".join(f"'{table.name}',(SELECT count(*) FROM public.{table.name})" for table in SOURCE_TABLES)
                + ")"
            )
            expected_snapshot = json.loads(stack.sql("POSTGRES_ECOMMERCE_DB", snapshot_sql))

            def snapshot_complete():
                actual = {
                    row["table_name"]: row["n"]
                    for row in stack.query(
                        "SELECT table_name,count(*) AS n FROM lakehouse.bronze.oltp_cdc_events WHERE operation='r' GROUP BY table_name"
                    )
                }
                return (
                    actual if all(actual.get(name, 0) == count for name, count in expected_snapshot.items()) else None
                )

            observed_snapshot = stack.wait("snapshot-all-tables-match", snapshot_complete)
            stack.evidence["snapshot"] = {
                "expected": expected_snapshot,
                "observed": {name: observed_snapshot.get(name, 0) for name in expected_snapshot},
                "rows": sum(expected_snapshot.values()),
            }
            topics = stack.command(
                ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list"]
            ).stdout.splitlines()
            self.assertNotIn(stack.variables["CDC_TOPIC_PREFIX"] + ".public.customer_credentials", topics)
            legacy = stack.query(
                "SELECT mysql_server_id,binlog_file,binlog_position,source_connector,postgres_lsn FROM lakehouse.bronze.oltp_cdc_events WHERE event_id='legacy:0:42'"
            )[0]
            self.assertEqual(
                legacy,
                dict(
                    mysql_server_id="17",
                    binlog_file="mysql-bin.000001",
                    binlog_position=42,
                    source_connector=None,
                    postgres_lsn=None,
                ),
            )
            stack.evidence["legacy_schema_preserved"] = legacy
            customer = stack.sql(
                "POSTGRES_ECOMMERCE_DB",
                f"INSERT INTO customers(public_id,display_name) VALUES('{uuid.uuid4()}','CDC before') RETURNING customer_id",
            ).splitlines()[0]
            predicate = f"table_name='customers' AND json_extract_scalar(record_key,'$.customer_id')='{customer}'"

            def event(op):
                rows = stack.query(
                    f"SELECT * FROM lakehouse.bronze.oltp_cdc_events WHERE {predicate} AND operation='{op}'"
                )
                return rows[0] if rows else None

            created = stack.wait("insert-visible", lambda: event("c"))
            stack.sql(
                "POSTGRES_ECOMMERCE_DB",
                f"UPDATE customers SET display_name='CDC after', is_cod_blocked=true WHERE customer_id={customer}",
            )
            updated = stack.wait("update-visible", lambda: event("u"))
            self.assertEqual(json.loads(updated["before_payload"])["display_name"], "CDC before")
            self.assertEqual(json.loads(updated["after_payload"])["display_name"], "CDC after")
            self.assertFalse(json.loads(updated["before_payload"])["is_cod_blocked"])
            self.assertTrue(json.loads(updated["after_payload"])["is_cod_blocked"])
            stack.sql("POSTGRES_ECOMMERCE_DB", f"DELETE FROM customers WHERE customer_id={customer}")
            deleted = stack.wait("delete-visible", lambda: event("d"))
            tombstone = stack.wait("tombstone-visible", lambda: event("t"))
            self.assertEqual(json.loads(deleted["before_payload"])["display_name"], "CDC after")
            self.assertIsNone(deleted["after_payload"])
            self.assertIsNone(tombstone["raw_payload"])
            for row in (created, updated, deleted):
                self.assertEqual(row["source_connector"], "postgresql")
                self.assertEqual(row["source_schema"], "public")
                self.assertGreater(row["postgres_lsn"], 0)
                self.assertGreater(row["postgres_tx_id"], 0)
                self.assertIsNone(row["binlog_position"])
                self.assertEqual(row["event_id"], f"{row['topic']}:{row['kafka_partition']}:{row['kafka_offset']}")
            self.assertEqual(tombstone["database_name"], stack.variables["POSTGRES_ECOMMERCE_DB"])
            stack.wait(
                "transaction-metadata",
                lambda: (
                    stack.query(
                        "SELECT count(*) AS n FROM lakehouse.bronze.oltp_cdc_events WHERE operation='END' AND table_name='__transaction__'"
                    )[0]["n"]
                    > 0
                ),
            )
            self.assertEqual(
                stack.query(
                    "SELECT count(*) AS n FROM lakehouse.bronze.oltp_cdc_events WHERE table_name='customer_credentials'"
                )[0]["n"],
                0,
            )
            stack.evidence["events"] = {
                "customer_id": customer,
                "operations": ["r", "c", "u", "d", "t", "BEGIN", "END"],
                "lsn": updated["postgres_lsn"],
                "tx_id": updated["postgres_tx_id"],
                "full_before_images": True,
            }
            checkpoint = stack.wait("checkpoint-completed", lambda: stack.checkpoints(job)["latest"].get("completed"))
            stack.command(["restart", "flink-taskmanager"])
            stack.wait(
                "taskmanager-recovered",
                lambda: stack.http("flink", "GET", f"/jobs/{job}")["state"] == "RUNNING",
                timeout=180,
            )
            recovered_customer = stack.sql(
                "POSTGRES_ECOMMERCE_DB",
                f"INSERT INTO customers(public_id,display_name) VALUES('{uuid.uuid4()}','CDC recovery') RETURNING customer_id",
            ).splitlines()[0]
            stack.wait(
                "post-restart-insert",
                lambda: (
                    stack.query(
                        f"SELECT count(*) AS n FROM lakehouse.bronze.oltp_cdc_events WHERE operation='c' AND json_extract_scalar(record_key,'$.customer_id')='{recovered_customer}'"
                    )[0]["n"]
                    == 1
                ),
            )
            stack.wait(
                "post-restart-checkpoint",
                lambda: (stack.checkpoints(job)["latest"].get("completed") or {}).get("id", -1) > checkpoint["id"],
            )
            self.assertEqual(
                stack.query("SELECT count(*)-count(DISTINCT event_id) AS n FROM lakehouse.bronze.oltp_cdc_events")[0][
                    "n"
                ],
                0,
            )
            stack.evidence["checkpoint_recovery"] = {
                "initial_checkpoint": checkpoint["id"],
                "job_id": job,
                "new_insert_visible": True,
                "duplicate_event_ids": 0,
            }
            slot_before = json.loads(
                stack.sql(
                    "POSTGRES_ECOMMERCE_DB",
                    """SELECT json_build_object(
                'slot',slot_name,'plugin',plugin,'lsn',confirmed_flush_lsn::text)
                FROM pg_replication_slots WHERE slot_name='ecommerce_cdc'""",
                )
            )
            self.assertEqual(slot_before["plugin"], "pgoutput")
            stack.command(["restart", "debezium-connect"])
            stack.up("debezium-connect")
            stack.wait("connector-recovered", stack.connector_running)
            stack.sql(
                "POSTGRES_ECOMMERCE_DB",
                f"UPDATE customers SET display_name='CDC connector recovered' WHERE customer_id={recovered_customer}",
            )
            stack.wait(
                "post-connector-restart-update",
                lambda: (
                    stack.query(
                        f"SELECT count(*) AS n FROM lakehouse.bronze.oltp_cdc_events WHERE operation='u' AND json_extract_scalar(record_key,'$.customer_id')='{recovered_customer}'"
                    )[0]["n"]
                    == 1
                ),
            )
            self.assertEqual(
                stack.query("SELECT count(*) AS n FROM lakehouse.bronze.oltp_cdc_events WHERE operation='r'")[0]["n"],
                sum(expected_snapshot.values()),
            )
            operations = {
                row["operation"]: row["n"]
                for row in stack.query(
                    f"SELECT operation,count(*) AS n FROM lakehouse.bronze.oltp_cdc_events WHERE {predicate} GROUP BY operation"
                )
            }
            self.assertEqual(operations, dict(c=1, u=1, d=1, t=1))
            self.assertEqual(
                stack.query("SELECT count(*)-count(DISTINCT event_id) AS n FROM lakehouse.bronze.oltp_cdc_events")[0][
                    "n"
                ],
                0,
            )
            self.assertEqual(
                stack.sql(
                    "POSTGRES_ECOMMERCE_DB",
                    f"SELECT pg_wal_lsn_diff(confirmed_flush_lsn,'{slot_before['lsn']}'::pg_lsn)>=0 FROM pg_replication_slots WHERE slot_name='ecommerce_cdc'",
                ),
                "t",
            )
            stack.evidence["connector_recovery"] = {
                "slot": slot_before["slot"],
                "plugin": "pgoutput",
                "snapshot_repeated": False,
                "post_restart_update_visible": True,
                "prior_row_operations": operations,
            }
            self.assertEqual(
                stack.query(
                    "SELECT count(*) AS n FROM lakehouse.information_schema.tables WHERE table_schema IN ('silver','gold')"
                )[0]["n"],
                0,
            )
            stack.evidence["status"] = "passed"
            print("POSTGRES_CDC_STACK_OK " + str(stack.report / "report.json"), flush=True)
        finally:
            stack.close()
