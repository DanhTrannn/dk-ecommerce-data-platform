"""Native log semantics independent of Kafka, Spark, storage or external data."""

import importlib.util
import json
import math
import unittest
from datetime import UTC, datetime, timedelta
from uuid import UUID
from unittest.mock import patch

AVAILABLE = all(importlib.util.find_spec(name) for name in ("polars", "pyiceberg", "pyarrow"))
if AVAILABLE:
    import polars as pl
    import pyarrow as pa

    from lakehouse.logs.polars.identity import resolve
    from lakehouse.logs.polars.marts import TABLES, build_all, schema
    from lakehouse.logs.polars.pipeline import run_marts
    from lakehouse.logs.polars.quality import REQUIRED_CHECKS, input_checks, output_checks
    from lakehouse.logs.polars.storage import PUBLICATION_SCHEMA, pinned_catalog, publish
    from lakehouse.oltp.polars.schema import ddl_schema

AT = datetime(2026, 11, 10, 1, 30, tzinfo=UTC)
PRODUCT_UUID = UUID("00112233-4455-6677-8899-aabbccddeeff")
VARIANT_UUID = UUID("11223344-5566-7788-99aa-bbccddeeff00")


def typed(rows, resource):
    return pl.from_arrow(pa.Table.from_pylist(rows, schema=ddl_schema(resource)))


def catalog_frames(legacy=False):
    def encode(value):
        return str(value).encode() if legacy else value.bytes

    products = typed(
        [dict(product_id=1, name="Áo thử nghiệm", slug="ao-thun", public_id=encode(PRODUCT_UUID))],
        "spark/silver/silver_products.sql",
    )
    variants = typed(
        [dict(variant_id=7, product_id=1, public_id=encode(VARIANT_UUID))], "spark/silver/silver_product_variants.sql"
    )
    return products, variants


def facts(rows):
    values = []
    for index, changes in enumerate(rows):
        row = dict(
            event_id=str(index),
            event_ts=AT,
            event_date=AT.date(),
            actor_key="a",
            actor_type="customer",
            http_request_method="GET",
            http_route="/products/{slug}",
            http_status_code=200,
            ecommerce_action="product_detail",
            product_key="ao-thun",
            variant_key=None,
            duration_ms=1.0,
            is_success=True,
            is_client_error=False,
            is_server_error=False,
            is_slow_request=False,
        )
        row.update(changes)
        values.append(row)
    return typed(values, "spark/logs/fact_web_events.sql")


@unittest.skipUnless(AVAILABLE, "install batch-pipeline[polars] for native log checks")
class NativeLogTests(unittest.TestCase):
    def build(self, rows):
        return build_all(facts(rows), *catalog_frames(), "test", ingested_at=AT)

    def test_empty_marts_keep_all_existing_contract_columns(self):
        frames = self.build([])
        self.assertEqual(tuple(frames), TABLES)
        for name, frame in frames.items():
            self.assertEqual(frame.height, 0)
            self.assertEqual(frame.columns, schema(name).names)

    def test_identity_slug_integer_uuid_variant_and_unresolved(self):
        events = facts(
            [
                {"product_key": "ao-thun"},
                {"product_key": "1"},
                {"product_key": str(PRODUCT_UUID).upper()},
                {"product_key": "", "variant_key": str(VARIANT_UUID).upper()},
                {"product_key": "missing-product"},
                {"product_key": "", "variant_key": "missing-variant"},
            ]
        )
        for legacy in (False, True):
            resolved = resolve(events, *catalog_frames(legacy)).sort("event_id")
            self.assertEqual(resolved["product_id"].to_list(), [1, 1, 1, 1, None, None])
            self.assertEqual(
                resolved["product_key"].to_list(),
                ["1", "1", "1", "1", "raw-product:missing-product", "raw-variant:missing-variant"],
            )

    def test_ambiguous_catalog_cannot_multiply_events(self):
        products, variants = catalog_frames()
        other = products.with_columns(pl.lit(2).cast(pl.Int64).alias("product_id"))
        with self.assertRaisesRegex(ValueError, "multiplied"):
            resolve(facts([{}]), pl.concat([products, other]), variants)

    def test_request_counts_success_3xx_null_actor_and_null_rates(self):
        frames = self.build(
            [
                {"ecommerce_action": "cart_add", "http_status_code": 302, "actor_key": None},
                {"ecommerce_action": "checkout_submit"},
                {"ecommerce_action": "checkout_submit"},
                {"http_status_code": 404, "is_success": False, "is_client_error": True},
                {"http_status_code": 500, "is_success": False, "is_server_error": True},
            ]
        )
        route, product, funnel = [frames[name].row(0, named=True) for name in TABLES]
        self.assertEqual(route["total_requests"], 5)
        self.assertEqual(route["success_2xx_count"], 3)  # Existing name also counts successful 3xx.
        self.assertEqual(route["error_rate_pct"], 40.0)
        self.assertEqual(product["cart_add_count"], 1)
        self.assertEqual(product["unique_visitors_count"], 0)
        self.assertIsNone(product["cart_to_view_rate_pct"])
        self.assertEqual(funnel["orders_completed"], 2)  # Request events, not distinct orders.
        self.assertEqual(funnel["unique_visitors"], 1)
        self.assertIsNone(funnel["overall_conversion_rate_pct"])

    def test_exact_nearest_rank_quantiles_ignore_nulls_and_round_half_away(self):
        frames = self.build([{"duration_ms": v} for v in [1.0, 2.0, 3.0, 4.0, None]])
        row = frames[TABLES[0]].row(0, named=True)
        self.assertEqual([row[f"p{q}_duration_ms"] for q in (50, 95, 99)], [2.0, 4.0, 4.0])
        self.assertEqual(row["avg_duration_ms"], 2.5)
        row = self.build([{"duration_ms": 1.2345}])[TABLES[0]].row(0, named=True)
        self.assertEqual(row["avg_duration_ms"], 1.235)

    def test_utc_hour_and_distinct_dates(self):
        later = AT + timedelta(days=1, hours=3)
        frames = self.build([{}, {"event_ts": later, "event_date": later.date()}])
        self.assertEqual(set(frames[TABLES[0]]["metric_hour"].to_list()), {1, 4})
        self.assertEqual(frames[TABLES[2]].height, 2)

    def test_gate_rejects_duplicate_id_invalid_flags_nan_and_wrong_date(self):
        events = facts(
            [
                {},
                {
                    "event_id": "0",
                    "is_success": False,
                    "duration_ms": math.nan,
                    "event_date": AT.date() - timedelta(days=1),
                },
            ]
        )
        failed = [r["check_name"] for r in input_checks(events) if r["status"] == "FAILED"]
        self.assertEqual(failed, ["event_identity", "event_date_utc", "http_status_flags", "duration_domain"])

    def test_readback_gate_conserves_counts_and_detects_corruption(self):
        events = facts([{}, {"ecommerce_action": "checkout_submit"}])
        expected = build_all(events, *catalog_frames(), "test", ingested_at=AT)
        self.assertTrue(all(r["status"] == "PASSED" for r in output_checks(events, expected, expected)))
        corrupted = dict(expected)
        corrupted[TABLES[0]] = expected[TABLES[0]].with_columns((pl.col("total_requests") + 1).alias("total_requests"))
        failed = [r["check_name"] for r in output_checks(events, expected, corrupted) if r["status"] == "FAILED"]
        self.assertIn("route_event_conservation", failed)
        self.assertIn("persisted_" + TABLES[0], failed)

    def test_publication_rejects_partial_failed_or_empty_audit(self):
        with self.assertRaisesRegex(ValueError, "Complete successful"):
            publish(None, "polars_batch_test", "run", {}, {}, [], observed=AT, latest=AT)

    def test_catalog_selection_reads_published_ids_including_empty_snapshots(self):
        prefix = "polars_batch_test"
        for source, namespace, silver in (
            ("native", prefix + "_system", prefix + "_silver"),
            ("serving", "system", "silver"),
        ):
            manifest = {f"lakehouse.{silver}.silver_products": 12, f"lakehouse.{silver}.silver_product_variants": None}
            rows = pl.DataFrame(
                [
                    dict(
                        run_id="older",
                        published_at=AT - timedelta(seconds=1),
                        schema_version=3,
                        source_mode="batch",
                        snapshots_json=json.dumps(manifest),
                    ),
                    dict(
                        run_id="chosen",
                        published_at=AT,
                        schema_version=3,
                        source_mode="batch",
                        snapshots_json=json.dumps(manifest),
                    ),
                ]
            )

            class Catalog:
                def load_table(inner, name):
                    return type(
                        "Table",
                        (),
                        dict(
                            name=name,
                            current_snapshot=lambda self: type("Snapshot", (), {"snapshot_id": 99})(),
                            snapshot_by_id=lambda self, value: object() if value == 12 else None,
                        ),
                    )()

            reads = []

            def read(table, snapshot):
                reads.append((table.name, snapshot))
                return rows if table.name[1] == "analytics_publications" else pl.DataFrame()

            with patch("lakehouse.logs.polars.storage.read_snapshot", side_effect=read):
                _, snapshots, run_id = pinned_catalog(Catalog(), prefix, source)
                self.assertEqual(run_id, "chosen")
                self.assertEqual(snapshots[f"lakehouse.{namespace}.analytics_publications"], 99)
                self.assertEqual(
                    reads[1:], [((silver, "silver_products"), 12), ((silver, "silver_product_variants"), None)]
                )
                with self.assertRaisesRegex(ValueError, "Publish the requested"):
                    pinned_catalog(Catalog(), prefix, source, "missing")

    def test_publication_keeps_audited_output_ids_without_recapturing_latest(self):
        prefix = "polars_batch_test"
        snapshots = {f"lakehouse.{prefix}_gold.{name}": (12 if i == 0 else None) for i, name in enumerate(TABLES)}

        class Output:
            def schema(inner):
                return type("Schema", (), {"as_arrow": lambda self: PUBLICATION_SCHEMA})()

            def upsert(inner, data, **kwargs):
                self.assertEqual(json.loads(data.to_pylist()[0]["snapshots_json"]), snapshots)

        class Catalog:
            def load_table(inner, name):
                return type("Table", (), {"snapshot_by_id": lambda self, value: object() if value == 12 else None})()

            def create_table_if_not_exists(inner, *args, **kwargs):
                return Output()

        checks = [dict(check_name=name, status="PASSED") for name in REQUIRED_CHECKS]
        sources = {
            "lakehouse.gold.fact_web_events": 12,
            f"lakehouse.{prefix}_system.analytics_publications": 12,
            f"lakehouse.{prefix}_silver.silver_products": 12,
            f"lakehouse.{prefix}_silver.silver_product_variants": None,
        }
        row = publish(Catalog(), prefix, "run", snapshots, sources, checks, observed=AT, latest=None)
        self.assertEqual(json.loads(row["snapshots_json"]), snapshots)
        with self.assertRaisesRegex(ValueError, "all three published"):
            publish(None, prefix, "run", snapshots, {}, checks, observed=AT, latest=None)
        checks[0]["status"] = "FAILED"
        with self.assertRaises(ValueError):
            publish(None, prefix, "run", snapshots, sources, checks, observed=AT, latest=None)

    def test_timezone_and_namespace_are_validated_before_io(self):
        for prefix, observed in (("gold", AT.isoformat()), ("polars_batch_test", "2026-11-10T01:30:00")):
            with self.assertRaises(ValueError):
                run_marts(None, None, prefix, bucket="lakehouse", run_id="test", as_of=observed)
