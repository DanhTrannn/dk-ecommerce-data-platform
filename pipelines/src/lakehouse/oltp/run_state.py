"""Batch run state tracking, identity hashing and retry recovery in Iceberg."""

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.expressions import EqualTo

from lakehouse.oltp.polars.writer import PROPERTIES
from lakehouse.utils.iceberg import evolve_table_schema, load_or_create
from lakehouse.utils.namespaces import namespace as ns

BATCH_RUN_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("mode", pa.string()),
        ("status", pa.string()),
        ("resume_status", pa.string()),
        ("attempt", pa.int32()),
        ("config_fingerprint", pa.string()),
        ("interval_start", pa.string()),
        ("interval_end", pa.string()),
        ("as_of", pa.string()),
        ("batch_date", pa.string()),
        ("contract_version", pa.int32()),
        ("bronze_snapshots_json", pa.string()),
        ("silver_snapshots_json", pa.string()),
        ("gold_snapshots_json", pa.string()),
        ("date_bounds_json", pa.string()),
        ("counters_json", pa.string()),
        ("quality_summary_json", pa.string()),
        ("error_phase", pa.string()),
        ("error_message", pa.string()),
        ("started_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class BatchRunIdentity:
    run_id: str
    mode: str  # "full" or "incremental"
    namespace_prefix: str
    interval_start: str | None
    interval_end: str | None
    as_of: str
    batch_date: str
    contract_version: int = 4
    date_start: str | None = None
    date_end: str | None = None
    migrate_schema: bool = False
    trino_reconciliation: bool = False


def compute_config_fingerprint(identity: BatchRunIdentity) -> str:
    """Canonical deterministic hash of run configuration without credentials."""
    from lakehouse.analytics.dbt import project_directory

    canonical = asdict(identity)
    lakehouse = Path(__file__).resolve().parents[1]
    roots = {"oltp": lakehouse / "oltp", "analytics": lakehouse / "analytics", "utils": lakehouse / "utils",
             "dbt": project_directory(), "sql_templates": lakehouse / "sql_templates" / "contracts"}
    revision = hashlib.sha256()
    for name, root in sorted(roots.items()):
        for file in sorted(root.rglob("*")):
            if file.is_file() and file.suffix in {".py", ".sql", ".yml"} and ".user.yml" != file.name:
                if any(part in {"logs", "target", "dbt_packages", "__pycache__"} for part in file.relative_to(root).parts):
                    continue
                revision.update(f"{name}/{file.relative_to(root).as_posix()}\0".encode())
                revision.update(hashlib.sha256(file.read_bytes()).digest())
    canonical["transform_revision"] = revision.hexdigest()
    payload = json.dumps(canonical, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resume_cutoff():
    days = int(os.environ.get("BATCH_RESUME_RETENTION_DAYS", "7"))
    if days < 1:
        raise ValueError("Batch resume retention must be at least one day")
    return datetime.now(UTC) - timedelta(days=days)


def load_run(catalog, prefix: str, run_id: str) -> dict[str, Any] | None:
    """Load latest state of batch run if table and row exist."""
    key = (ns(prefix, "system"), "batch_runs")
    if not catalog.table_exists(key):
        return None
    rows = catalog.load_table(key).scan(row_filter=EqualTo("run_id", run_id)).to_arrow().to_pylist()
    if len(rows) > 1:
        raise ValueError("Multiple batch states match the run ID")
    return rows[0] if rows else None


def save_run(catalog, prefix: str, state: dict[str, Any]) -> None:
    """Persist batch run state using PyIceberg upsert on run_id."""
    catalog.create_namespace_if_not_exists(ns(prefix, "system"))
    table = load_or_create(
        catalog,
        (ns(prefix, "system"), "batch_runs"),
        schema=BATCH_RUN_SCHEMA,
        properties=PROPERTIES,
    )
    evolve_table_schema(table, BATCH_RUN_SCHEMA)
    now = datetime.now(UTC)
    row = {
        "run_id": str(state.get("run_id", "")),
        "mode": str(state.get("mode", "incremental")),
        "status": str(state.get("status", "STARTED")),
        "resume_status": state.get("resume_status"),
        "attempt": int(state.get("attempt", 1)),
        "config_fingerprint": str(state.get("config_fingerprint", "")),
        "interval_start": str(state.get("interval_start") or ""),
        "interval_end": str(state.get("interval_end") or ""),
        "as_of": str(state.get("as_of", "")),
        "batch_date": str(state.get("batch_date", "")),
        "contract_version": int(state.get("contract_version", 4)),
        "bronze_snapshots_json": str(state.get("bronze_snapshots_json", "{}")),
        "silver_snapshots_json": str(state.get("silver_snapshots_json", "{}")),
        "gold_snapshots_json": str(state.get("gold_snapshots_json", "{}")),
        "date_bounds_json": state.get("date_bounds_json"),
        "counters_json": state.get("counters_json"),
        "quality_summary_json": state.get("quality_summary_json"),
        "error_phase": str(state.get("error_phase") or ""),
        "error_message": str(state.get("error_message") or ""),
        "started_at": state.get("started_at") or now,
        "updated_at": now,
    }
    table.upsert(pa.Table.from_pylist([row], schema=BATCH_RUN_SCHEMA), join_cols=["run_id"])


def transition_run_state(catalog, prefix: str, current_state: dict[str, Any], to_status: str, **updates) -> dict[str, Any]:
    """Transition state to new status, persist to Iceberg and return updated state."""
    state = dict(current_state)
    state["status"] = to_status
    if to_status != "FAILED":
        state.update(resume_status=None, error_phase=None, error_message=None)
    state.update(updates)
    save_run(catalog, prefix, state)
    return state
