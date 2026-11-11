"""Frozen-input native Gold rebuild, persistent gate and isolated publication."""

from datetime import UTC, datetime, date

from lakehouse.oltp.polars.pipeline import validate_run
from lakehouse.oltp.quality.contracts import GoldQualityGateError
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.postgres import namespace_lock
from .builders import TABLES, build_all
from .publication import publish
from .quality import evaluate
from .storage import capture, persist_quality, read_snapshot, write_gold


def pinned_inputs(catalog, prefix):
    frames, snapshots = {}, {}
    for spec in SOURCE_TABLES:
        table = catalog.load_table((prefix + "_silver", spec.silver_table))
        snapshot = capture(table)
        snapshots[f"lakehouse.{prefix}_silver.{spec.silver_table}"] = snapshot
        frames[spec.name] = read_snapshot(table, snapshot)
    return frames, snapshots


def audit_and_publish(catalog, prefix, run_id, batch_date, silver, snapshots, *, committed=None, publish_result=True):
    frames = dict(silver)
    audited = dict(snapshots)
    for name in TABLES:
        table = catalog.load_table((prefix + "_gold", name))
        snapshot = committed[name] if committed is not None else capture(table)
        frames[name] = read_snapshot(table, snapshot)
        audited[f"lakehouse.{prefix}_gold.{name}"] = snapshot
    summary = evaluate(frames, run_id)
    persist_quality(catalog, prefix, summary, run_id, batch_date)
    if summary["failed_checks"]:
        failed = [r["check_name"] for r in summary["results"] if not r["passed"]]
        raise GoldQualityGateError("Native Gold quality gate failed: " + ", ".join(failed))
    publication = publish(catalog, prefix, run_id, audited, summary) if publish_result else None
    return {"quality": summary, "publication": publication, "snapshots": audited}


def run_gold(catalog, connection, prefix, *, bucket, run_id, as_of, verify_only=False, batch_date=None):
    validate_run(prefix, run_id, bucket, 10000)
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("Gold --as-of must include timezone")
    observed = observed.astimezone(UTC)
    batch_date = date.fromisoformat(batch_date).isoformat() if batch_date else observed.date().isoformat()
    with namespace_lock(connection, prefix):
        catalog.create_namespace_if_not_exists(prefix + "_gold")
        catalog.create_namespace_if_not_exists(prefix + "_system")
        silver, snapshots = pinned_inputs(catalog, prefix)
        committed = None
        if not verify_only:
            committed = {}
            history = None
            if catalog.table_exists((prefix + "_gold", "fact_inventory_daily_snapshot")):
                table = catalog.load_table((prefix + "_gold", "fact_inventory_daily_snapshot"))
                history = read_snapshot(table, capture(table))
            frames = build_all(silver, run_id, observed, inventory_history=history)
            for name in TABLES:
                table = write_gold(catalog, prefix, name, frames[name], observed)
                committed[name] = capture(table)
                print("NATIVE_GOLD table=" + name, flush=True)
        return audit_and_publish(
            catalog, prefix, run_id, batch_date, silver, snapshots, committed=committed, publish_result=not verify_only
        )
