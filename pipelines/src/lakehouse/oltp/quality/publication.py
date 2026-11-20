"""Audit the exact serving snapshot set and conditionally publish it."""

from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.oltp.gold.schema import TABLES
from lakehouse.oltp.publication import publish
from .gold import evaluate
from .storage import persist_quality
from .contracts import GoldQualityGateError


def capture_snapshot_map(catalog, prefix, *, silver_only=False):
    """Capture snapshot IDs without loading table data frames into memory."""
    snapshots = {}
    for spec in SOURCE_TABLES:
        table = catalog.load_table((ns(prefix, "silver"), spec.silver_table))
        snapshots[f"lakehouse.{ns(prefix, 'silver')}.{spec.silver_table}"] = capture(table)
    if not silver_only:
        for name in TABLES:
            table = catalog.load_table((ns(prefix, "gold"), name))
            snapshots[f"lakehouse.{ns(prefix, 'gold')}.{name}"] = capture(table)
    return snapshots


def pinned_inputs(catalog, prefix):
    frames, snapshots = {}, {}
    for spec in SOURCE_TABLES:
        table = catalog.load_table((ns(prefix, "silver"), spec.silver_table))
        snapshot = capture(table)
        snapshots[f"lakehouse.{ns(prefix, 'silver')}.{spec.silver_table}"] = snapshot
        frames[spec.name] = read_snapshot(table, snapshot)
    return frames, snapshots


def audit_and_publish(catalog, prefix, run_id, batch_date, silver, snapshots, *, committed=None, publish_result=True):
    frames = dict(silver)
    audited = dict(snapshots)
    for name in TABLES:
        table = catalog.load_table((ns(prefix, "gold"), name))
        snapshot = committed[name] if committed is not None else capture(table)
        frames[name] = read_snapshot(table, snapshot)
        audited[f"lakehouse.{ns(prefix, 'gold')}.{name}"] = snapshot
    summary = evaluate(frames, run_id)
    try:
        persist_quality(catalog, prefix, summary, run_id, batch_date)
    except Exception:
        # If persistence fails, we must still fail the gate
        raise
    if summary.get("failed_checks", 0) > 0 or summary.get("status") != "PASSED":
        failed = [r["check_name"] for r in summary.get("results", []) if not r.get("passed")]
        raise GoldQualityGateError("Native Gold quality gate failed: " + ", ".join(failed))
    publication = publish(catalog, prefix, run_id, audited, summary) if publish_result else None
    return {"quality": summary, "publication": publication, "snapshots": audited}
