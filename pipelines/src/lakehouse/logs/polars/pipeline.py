"""Optional native rollups from a persisted fact and a published product catalog."""

from datetime import UTC, datetime

import polars as pl

from lakehouse.oltp.polars.gold.storage import capture, read_snapshot
from lakehouse.oltp.polars.pipeline import validate_run
from lakehouse.utils.postgres import namespace_lock
from .identity import AmbiguousProductIdentity
from .marts import TABLES, build_all
from .quality import input_checks, output_checks, result
from .storage import audit, pinned_catalog, publish, write_mart

FACT = "lakehouse.gold.fact_web_events"


def enforce(catalog, prefix, run_id, checks):
    audit(catalog, prefix, run_id, checks)
    if any(c["status"] != "PASSED" for c in checks):
        raise ValueError(
            "Native log quality gate failed: " + ", ".join(c["check_name"] for c in checks if c["status"] != "PASSED")
        )


def run_marts(
    catalog,
    connection,
    prefix,
    *,
    bucket,
    run_id,
    as_of,
    catalog_source="native",
    catalog_run_id=None,
    verify_only=False,
):
    validate_run(prefix, run_id, bucket, 10000)
    if catalog_source not in {"native", "serving"}:
        raise ValueError("Catalog source must be native or serving")
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Log --as-of must include timezone")
    observed = observed.astimezone(UTC)
    with namespace_lock(connection, prefix):
        catalog.create_namespace_if_not_exists(prefix + "_gold")
        catalog.create_namespace_if_not_exists(prefix + "_system")
        fact = catalog.load_table(("gold", "fact_web_events"))
        fact_snapshot = capture(fact)
        events = read_snapshot(fact, fact_snapshot)
        timestamps = result("event_timestamp_present", events["event_ts"].null_count())
        enforce(catalog, prefix, run_id, [timestamps])
        events = events.filter(pl.col("event_ts") <= observed)
        checks = [timestamps, *input_checks(events)]
        enforce(catalog, prefix, run_id, checks[1:])
        (products, variants), sources, source_run = pinned_catalog(catalog, prefix, catalog_source, catalog_run_id)
        sources[FACT] = fact_snapshot
        try:
            expected = build_all(events, products, variants, run_id)
        except AmbiguousProductIdentity as exc:
            audit(catalog, prefix, run_id, [result("product_identity_cardinality", 1)])
            raise ValueError("Native log identity gate failed") from exc
        snapshots, actual = {}, {}
        for name in TABLES:
            table = (
                catalog.load_table((prefix + "_gold", name))
                if verify_only
                else write_mart(catalog, prefix, name, expected[name])
            )
            own_snapshot = capture(table)
            snapshots[f"lakehouse.{prefix}_gold.{name}"] = own_snapshot
            actual[name] = read_snapshot(table, own_snapshot)
        outputs = output_checks(events, expected, actual)
        enforce(catalog, prefix, run_id, outputs)
        checks.extend(outputs)
        publication = (
            None
            if verify_only
            else publish(
                catalog,
                prefix,
                run_id,
                snapshots,
                sources,
                checks,
                observed=observed,
                latest=events["event_ts"].max(),
            )
        )
        return dict(
            events=events.height,
            catalog_run_id=source_run,
            checks=checks,
            publication=publication,
            snapshots=snapshots,
            source_snapshots=sources,
        )
