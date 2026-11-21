"""Native log ingestion and dbt rollups with an independently published product catalog."""

from datetime import UTC, datetime

import polars as pl

from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.utils.runs import validate_run
from lakehouse.utils.postgres import LOGS_LOCK, writer_lock
from .polars.identity import AmbiguousProductIdentity
from .polars.ingestion import ingest
from .polars.marts import TABLES, build_all, schema
from lakehouse.analytics.dbt import ensure_target, run_models
from .polars.quality import input_checks, output_checks, result
from .polars.storage import audit, pinned_catalog, publish

FACT = "lakehouse.gold.fact_web_events"


def enforce(catalog, run_id, checks):
    audit(catalog, run_id, checks)
    if any(c["status"] != "PASSED" for c in checks):
        raise ValueError(
            "Native log quality gate failed: " + ", ".join(c["check_name"] for c in checks if c["status"] != "PASSED")
        )


def run_marts(
    catalog,
    connection,
    *,
    bucket,
    run_id,
    as_of,
    catalog_run_id=None,
    verify_only=False,
    layer_snapshots=None,
):
    validate_run(run_id, bucket)
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Log --as-of must include timezone")
    observed = observed.astimezone(UTC)
    with writer_lock(connection, LOGS_LOCK):
        catalog.create_namespace_if_not_exists("gold")
        catalog.create_namespace_if_not_exists("system")
        fact = catalog.load_table(("gold", "fact_web_events"))
        fact_snapshot = layer_snapshots[FACT] if layer_snapshots is not None else capture(fact)
        events = read_snapshot(fact, fact_snapshot)
        timestamps = result("event_timestamp_present", events["event_ts"].null_count())
        enforce(catalog, run_id, [timestamps])
        events = events.filter(pl.col("event_ts") <= observed)
        checks = [timestamps, *input_checks(events)]
        enforce(catalog, run_id, checks[1:])
        (products, variants), sources, source_run = pinned_catalog(catalog, catalog_run_id)
        sources[FACT] = fact_snapshot
        sources.update(layer_snapshots or {})
        try:
            expected = build_all(events, products, variants, run_id)
        except AmbiguousProductIdentity as exc:
            audit(catalog, run_id, [result("product_identity_cardinality", 1)])
            raise ValueError("Native log identity gate failed") from exc
        for name in TABLES:
            ensure_target(catalog, name, schema(name))
        if not verify_only:
            inputs = {
                "products": next(v for k, v in sources.items() if k.endswith(".products")),
                "product_variants": next(v for k, v in sources.items() if k.endswith(".product_variants")),
                "fact_web_events": fact_snapshot,
            }
            run_models(run_id, observed, inputs, group="logs")
        snapshots, actual = {}, {}
        for name in TABLES:
            table = catalog.load_table(("gold", name))
            own_snapshot = capture(table)
            snapshots[f"lakehouse.gold.{name}"] = own_snapshot
            actual[name] = read_snapshot(table, own_snapshot)
        outputs = output_checks(events, expected, actual)
        enforce(catalog, run_id, outputs)
        checks.extend(outputs)
        publication = (
            None
            if verify_only
            else publish(
                catalog,
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


def run_logs(catalog, connection, *, bucket, run_id, as_of):
    """One writer boundary for native ingestion, audit and log publication."""
    validate_run(run_id, bucket)
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Log --as-of must include timezone")
    observed = observed.astimezone(UTC)
    with writer_lock(connection, LOGS_LOCK):
        snapshots = ingest(catalog, run_id, observed=observed)
        return run_marts(
            catalog,
            connection,
            bucket=bucket,
            run_id=run_id,
            as_of=observed.isoformat(),
            layer_snapshots=snapshots,
        )
