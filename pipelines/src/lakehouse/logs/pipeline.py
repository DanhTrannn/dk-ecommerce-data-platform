"""Native log ingestion and dbt rollups with an independently published product catalog."""

from lakehouse.utils.namespaces import namespace as ns
from datetime import UTC, datetime

import polars as pl

from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.oltp.polars.pipeline import validate_run
from lakehouse.utils.postgres import namespace_lock
from .polars.identity import AmbiguousProductIdentity
from .polars.marts import TABLES, build_all, schema
from lakehouse.analytics.dbt import ensure_target, run_models
from .polars.quality import input_checks, output_checks, result
from .polars.storage import audit, pinned_catalog, publish

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
    layer_snapshots=None,
):
    validate_run(prefix, run_id, bucket, 10000)
    if catalog_source not in {"native", "serving"}:
        raise ValueError("Catalog source must be native or serving")
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Log --as-of must include timezone")
    observed = observed.astimezone(UTC)
    with namespace_lock(connection, prefix):
        catalog.create_namespace_if_not_exists(ns(prefix, "gold"))
        catalog.create_namespace_if_not_exists(ns(prefix, "system"))
        fact_name = f"lakehouse.{ns(prefix, 'gold')}.fact_web_events" if layer_snapshots is not None else FACT
        fact = catalog.load_table(tuple(fact_name.split(".")[1:]))
        fact_snapshot = layer_snapshots[fact_name] if layer_snapshots is not None else capture(fact)
        events = read_snapshot(fact, fact_snapshot)
        timestamps = result("event_timestamp_present", events["event_ts"].null_count())
        enforce(catalog, prefix, run_id, [timestamps])
        events = events.filter(pl.col("event_ts") <= observed)
        checks = [timestamps, *input_checks(events)]
        enforce(catalog, prefix, run_id, checks[1:])
        (products, variants), sources, source_run = pinned_catalog(catalog, prefix, catalog_source, catalog_run_id)
        sources[fact_name] = fact_snapshot
        sources.update(layer_snapshots or {})
        try:
            expected = build_all(events, products, variants, run_id)
        except AmbiguousProductIdentity as exc:
            audit(catalog, prefix, run_id, [result("product_identity_cardinality", 1)])
            raise ValueError("Native log identity gate failed") from exc
        for name in TABLES:
            ensure_target(catalog, prefix, name, schema(name))
        if not verify_only:
            inputs = {
                "products": next(v for k, v in sources.items() if k.endswith(".silver_products")),
                "product_variants": next(v for k, v in sources.items() if k.endswith(".silver_product_variants")),
                "fact_web_events": fact_snapshot,
            }
            run_models(
                prefix,
                run_id,
                observed,
                inputs,
                group="logs",
                silver_schema=next(k.split(".")[1] for k in sources if k.endswith(".silver_products")),
                events_schema=fact_name.split(".")[1],
            )
        snapshots, actual = {}, {}
        for name in TABLES:
            table = catalog.load_table((ns(prefix, "gold"), name))
            own_snapshot = capture(table)
            snapshots[f"lakehouse.{ns(prefix, 'gold')}.{name}"] = own_snapshot
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


def run_logs(catalog, connection, prefix, *, bucket, run_id, as_of):
    """One writer boundary for native ingestion, audit and log publication."""
    from .polars.ingestion import ingest

    validate_run(prefix, run_id, bucket, 10000)
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Log --as-of must include timezone")
    observed = observed.astimezone(UTC)
    with namespace_lock(connection, prefix):
        snapshots = ingest(catalog, prefix, run_id, observed=observed)
        return run_marts(
            catalog,
            connection,
            prefix,
            bucket=bucket,
            run_id=run_id,
            as_of=observed.isoformat(),
            layer_snapshots=snapshots,
        )
