"""Audit the exact serving snapshot set and conditionally publish it."""

from dataclasses import asdict

from lakehouse.oltp.gold.schema import TABLES
from lakehouse.oltp.publication import publish
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.snapshots import capture, read_snapshot

from .contracts import GoldQualityGateError, QualityCheckResult, validate_required_checks
from .gold import AUDIT_TABLES, evaluate
from .reconciliation import run_snapshot_reconciliation
from .required_rules import REQUIRED_CHECK_IDS
from .storage import persist_quality


def table_names(layer):
    return {
        "bronze": {spec.name: spec.name for spec in SOURCE_TABLES},
        "silver": {spec.name: spec.silver_table for spec in SOURCE_TABLES},
        "gold": {name: name for name in TABLES},
    }[layer]


def capture_layer(catalog, prefix, layer):
    namespace = ns(prefix, layer)
    return {
        f"lakehouse.{namespace}.{name}": capture(catalog.load_table((namespace, name)))
        for name in table_names(layer).values()
    }


def validate_layer(catalog, prefix, layer, snapshots):
    namespace = ns(prefix, layer)
    names = table_names(layer)
    expected = {f"lakehouse.{namespace}.{name}" for name in names.values()}
    if set(snapshots) != expected:
        raise ValueError(f"Incomplete snapshot map for {namespace}")
    tables = {}
    for logical, physical in names.items():
        table = catalog.load_table((namespace, physical))
        snapshot = snapshots[f"lakehouse.{namespace}.{physical}"]
        if snapshot is not None and (
            type(snapshot) is not int or snapshot <= 0 or table.snapshot_by_id(snapshot) is None
        ):
            raise ValueError(f"Snapshot is no longer readable for {namespace}.{physical}")
        tables[logical] = (table, snapshot)
    return tables


def audit(catalog, prefix, run_id, batch_date, silver_snapshots, gold_snapshots, *, observed=None, trino_cursor=None):
    try:
        inputs = validate_layer(catalog, prefix, "silver", silver_snapshots)
        outputs = validate_layer(catalog, prefix, "gold", gold_snapshots)
        source_tables = inputs | outputs
        frames = {name: read_snapshot(*source_tables[name]) for name in sorted(AUDIT_TABLES)}
        summary = evaluate(frames, run_id, observed)
        if trino_cursor is not None:
            results = run_snapshot_reconciliation(trino_cursor, prefix, silver_snapshots | gold_snapshots)
            native = {result["rule_id"]: result for result in summary["results"]}
            for sql_result in results:
                expected = native[sql_result.rule_id]
                if asdict(sql_result) != expected:
                    raise GoldQualityGateError(f"Trino differs from native audit for {sql_result.rule_id}")
    except Exception as error:
        result = QualityCheckResult(
            "audit_execution", "candidate_snapshots", type(error).__name__, "success", False,
            "Candidate audit could not be completed", status="ERROR",
        )
        persist_quality(catalog, prefix, {"results": [asdict(result)]}, run_id, batch_date)
        raise

    persist_quality(catalog, prefix, summary, run_id, batch_date)
    if summary["status"] != "PASSED":
        failed = [result["check_name"] for result in summary["results"] if not result["passed"]]
        raise GoldQualityGateError("Native Gold quality gate failed: " + ", ".join(failed))
    validate_required_checks(summary, REQUIRED_CHECK_IDS)
    return summary


def audit_and_publish(
    catalog, prefix, run_id, batch_date, silver_snapshots, gold_snapshots, *,
    publish_result=True, observed=None, trino_cursor=None,
):
    summary = audit(
        catalog, prefix, run_id, batch_date, silver_snapshots, gold_snapshots,
        observed=observed, trino_cursor=trino_cursor,
    )
    snapshots = silver_snapshots | gold_snapshots
    publication = publish(catalog, prefix, run_id, snapshots, summary) if publish_result else None
    return {"quality": summary, "publication": publication, "snapshots": snapshots}
