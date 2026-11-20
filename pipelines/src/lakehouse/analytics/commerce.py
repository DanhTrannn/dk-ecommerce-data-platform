"""Frozen Silver to dbt/Trino Gold, native quality audit and snapshot publication."""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from lakehouse.analytics.dbt import ensure_target, run_models
from lakehouse.oltp.contract_migration import prepare_schema
from lakehouse.oltp.date_bounds import infer_date_bounds, validate_bounds
from lakehouse.oltp.gold.schema import GOLD_TABLE_DDL, TABLES, schema
from lakehouse.oltp.polars.pipeline import validate_run
from lakehouse.oltp.quality.publication import audit_and_publish, capture_layer, validate_layer
from lakehouse.utils.postgres import namespace_lock


def run_clock(as_of):
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Gold --as-of must include timezone")
    return observed.astimezone(UTC)


def resolve_date_bounds(catalog, prefix, snapshots, observed, start=None, end=None):
    configured = validate_bounds(start, end)
    if configured is not None:
        return configured
    inputs = validate_layer(catalog, prefix, "silver", snapshots)
    business_date = observed.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
    return infer_date_bounds(prefix, {name: snapshot for name, (_, snapshot) in inputs.items()}, business_date)


def build_gold(catalog, prefix, run_id, observed, silver_snapshots, bounds):
    sources = validate_layer(catalog, prefix, "silver", silver_snapshots)
    inputs = {name: snapshot for name, (_, snapshot) in sources.items()}
    for name in TABLES:
        ensure_target(catalog, prefix, name, schema(name), GOLD_TABLE_DDL[name])
    result = run_models(prefix, run_id, observed, inputs, group="oltp", date_bounds=bounds)
    return capture_layer(catalog, prefix, "gold"), result


def build_commerce(catalog, connection, prefix, *, bucket, run_id, as_of, batch_date=None, verify_only=False):
    validate_run(prefix, run_id, bucket, 10000)
    observed = run_clock(as_of)
    logical_date = date.fromisoformat(batch_date).isoformat() if batch_date else observed.date().isoformat()
    with namespace_lock(connection, prefix):
        prepare_schema(catalog, prefix, migrate=False)
        silver = capture_layer(catalog, prefix, "silver")
        if verify_only:
            gold, dbt = capture_layer(catalog, prefix, "gold"), None
        else:
            bounds = resolve_date_bounds(catalog, prefix, silver, observed)
            gold, dbt = build_gold(catalog, prefix, run_id, observed, silver, bounds)
        result = audit_and_publish(
            catalog, prefix, run_id, logical_date, silver, gold, publish_result=not verify_only, observed=observed,
        )
        return {**result, "dbt": dbt}
