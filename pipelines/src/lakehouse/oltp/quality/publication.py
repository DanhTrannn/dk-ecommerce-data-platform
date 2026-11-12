"""Audit the exact serving snapshot set and conditionally publish it."""

from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.oltp.gold.schema import TABLES
from lakehouse.oltp.publication import publish
from .gold import evaluate
from .storage import persist_quality
from .contracts import GoldQualityGateError


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
    persist_quality(catalog, prefix, summary, run_id, batch_date)
    if summary["failed_checks"]:
        failed = [r["check_name"] for r in summary["results"] if not r["passed"]]
        raise GoldQualityGateError("Native Gold quality gate failed: " + ", ".join(failed))
    publication = publish(catalog, prefix, run_id, audited, summary) if publish_result else None
    return {"quality": summary, "publication": publication, "snapshots": audited}
