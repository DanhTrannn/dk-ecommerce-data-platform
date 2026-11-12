"""Native ingestion, CDC deletion and publication-aware maintenance contracts."""

import json
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import polars as pl
import pyarrow as pa

from lakehouse.logs.polars.ingestion import bronze_delta, silver_delta, fact_delta
from lakehouse.oltp.hard_delete import deletion_keys
from lakehouse.oltp.polars.schema import ddl_schema
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.maintenance import expiry_candidates
from lakehouse.utils.namespaces import namespace

AT = datetime(2026, 11, 11, 1, tzinfo=UTC)


def empty(resource):
    return pl.from_arrow(pa.Table.from_pylist([], schema=ddl_schema("contracts/logs/" + resource + ".sql")))


def raw(event_id="a" * 32, **updates):
    event = dict(
        request={"id": event_id},
        timestamp=AT.isoformat(),
        http={"status_code": 200, "request_method": "GET", "route": "/products"},
        event={"duration_ns": 1234500},
    )
    event.update(updates)
    return json.dumps(event)


class NativeContracts(unittest.TestCase):
    def landing(self, rows):
        return pl.DataFrame({"raw_payload": rows, "ingest_ts": [AT] * len(rows)})

    def test_native_log_layers_retain_rejected_raws_and_deduplicate_replay(self):
        landing = self.landing([raw(), raw(), "invalid-json"])
        bronze = bronze_delta(landing, empty("bronze_web_events"), "run", observed=AT, source_ref="fixture")
        self.assertEqual(bronze.height, 2)
        self.assertEqual(bronze.filter(pl.col("_validation_error").is_not_null()).height, 1)
        silver = silver_delta(bronze, empty("silver_logs"), observed=AT)
        fact = fact_delta(silver, empty("fact_web_events"), "run", observed=AT)
        self.assertEqual(silver.height, 1)
        self.assertEqual(fact["duration_ms"][0], 1.234)
        self.assertEqual(bronze_delta(landing, bronze, "retry", observed=AT, source_ref="fixture").height, 0)
        self.assertEqual(silver_delta(bronze, silver, observed=AT).height, 0)
        self.assertEqual(fact_delta(silver, fact, "retry", observed=AT).height, 0)

    def test_native_logs_future_event_waits_for_cutoff_without_being_lost(self):
        future = raw("b" * 32, timestamp=(AT + timedelta(days=1)).isoformat())
        bronze = bronze_delta(
            self.landing([raw(), future]), empty("bronze_web_events"), "run", observed=AT, source_ref="fixture"
        )
        first = silver_delta(bronze, empty("silver_logs"), observed=AT)
        self.assertEqual(first.height, 1)
        self.assertEqual(silver_delta(bronze, first, observed=AT + timedelta(days=1)).height, 1)

    def test_conflicting_valid_payloads_cannot_silently_replace_event(self):
        bronze = bronze_delta(
            self.landing([raw(), raw(event={"duration_ns": 2})]),
            empty("bronze_web_events"),
            "run",
            observed=AT,
            source_ref="fixture",
        )
        with self.assertRaisesRegex(ValueError, "Conflicting valid"):
            silver_delta(bronze, empty("silver_logs"), observed=AT)

    def test_landing_null_payload_or_ingest_timestamp_fails(self):
        for landing in (
            self.landing([None]),
            self.landing([raw()]).with_columns(pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("ingest_ts")),
        ):
            with self.assertRaises(ValueError):
                bronze_delta(landing, empty("bronze_web_events"), "run", observed=AT, source_ref="fixture")

    def test_hard_delete_requires_bounded_soft_delete_evidence_in_same_database(self):
        table = next(t for t in SOURCE_TABLES if t.name == "cart_items")
        rows = []
        for index, before, database, when in [
            (1, {"is_present": False, "removed_at": "earlier"}, "ecommerce", AT - timedelta(days=30)),
            (2, {"is_present": True, "removed_at": "earlier"}, "ecommerce", AT - timedelta(days=40)),
            (3, {"is_present": False, "removed_at": "earlier"}, "other", AT - timedelta(days=40)),
            (4, {"is_present": False, "removed_at": "earlier"}, "ecommerce", AT - timedelta(days=29)),
            (5, {"is_present": False}, "ecommerce", AT - timedelta(days=40)),
        ]:
            rows.append(
                dict(
                    operation="d",
                    database_name=database,
                    table_name="cart_items",
                    source_ts=when,
                    before_payload=json.dumps(before),
                    record_key=json.dumps({"cart_item_id": index}),
                )
            )
        self.assertEqual(deletion_keys(pl.DataFrame(rows), table, "ecommerce", AT), [1])

    def test_expiration_keeps_publications_refs_and_latest_retention(self):
        table = SimpleNamespace(
            metadata=SimpleNamespace(
                snapshots=[SimpleNamespace(snapshot_id=i, timestamp_ms=i) for i in range(1, 5)],
                refs={"main": SimpleNamespace(snapshot_id=4), "tag": SimpleNamespace(snapshot_id=2)},
            )
        )
        self.assertEqual(
            expiry_candidates(table, "lakehouse.gold.x", {"lakehouse.gold.x": {1}}, retain=1, cutoff=AT), [3]
        )

    def test_namespace_and_nested_contract_types(self):
        self.assertEqual(namespace("serving", "gold"), "gold")
        self.assertEqual(namespace("polars_batch_demo", "silver"), "polars_batch_demo_silver")
        with self.assertRaises(ValueError):
            namespace("gold", "silver")
        schema = ddl_schema("contracts/logs/bronze_web_events.sql")
        self.assertTrue(pa.types.is_struct(schema.field("ecommerce").type))
        self.assertTrue(pa.types.is_map(schema.field("ecommerce").type.field("filters").type))
