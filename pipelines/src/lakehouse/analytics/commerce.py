"""Frozen Silver to dbt/Trino Gold, native quality audit and snapshot publication."""

from datetime import UTC, date, datetime

from lakehouse.analytics.dbt import ensure_target, run_models
from lakehouse.oltp.gold.schema import GOLD_TABLE_DDL
from lakehouse.oltp.gold.schema import TABLES, schema
from lakehouse.oltp.quality.publication import pinned_inputs, audit_and_publish
from lakehouse.utils.snapshots import capture
from lakehouse.oltp.polars.pipeline import validate_run
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.postgres import namespace_lock


def build_commerce(catalog, connection, prefix, *, bucket, run_id, as_of, batch_date=None, verify_only=False):
    validate_run(prefix, run_id, bucket, 10000)
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Gold --as-of must include timezone")
    observed = observed.astimezone(UTC)
    logical_date = date.fromisoformat(batch_date).isoformat() if batch_date else observed.date().isoformat()
    with namespace_lock(connection, prefix):
        catalog.create_namespace_if_not_exists(ns(prefix, "system"))
        silver, snapshots = pinned_inputs(catalog, prefix)
        inputs = {t.name: snapshots[f"lakehouse.{ns(prefix, 'silver')}.{t.silver_table}"] for t in SOURCE_TABLES}
        for name in TABLES:
            ensure_target(catalog, prefix, name, schema(name), GOLD_TABLE_DDL[name])
        dbt = None if verify_only else run_models(prefix, run_id, observed, inputs, group="oltp")
        committed = {name: capture(catalog.load_table((ns(prefix, "gold"), name))) for name in TABLES}
        result = audit_and_publish(
            catalog,
            prefix,
            run_id,
            logical_date,
            silver,
            snapshots,
            committed=committed,
            publish_result=not verify_only,
        )
        return {**result, "dbt": dbt}
