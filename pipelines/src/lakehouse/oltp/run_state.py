"""Batch run state tracking, identity hashing and retry recovery in Iceberg."""

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

import pyarrow as pa

from lakehouse.oltp.polars.writer import PROPERTIES
from lakehouse.utils.namespaces import namespace as ns

BATCH_RUN_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("mode", pa.string()),
        ("status", pa.string()),
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


def compute_config_fingerprint(identity: BatchRunIdentity) -> str:
    """Canonical deterministic hash of run configuration without credentials."""
    canonical = {
        "run_id": identity.run_id,
        "mode": identity.mode,
        "namespace_prefix": identity.namespace_prefix,
        "interval_start": identity.interval_start,
        "interval_end": identity.interval_end,
        "as_of": identity.as_of,
        "batch_date": identity.batch_date,
        "contract_version": identity.contract_version,
    }
    payload = json.dumps(canonical, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_run(catalog, prefix: str, run_id: str) -> dict[str, Any] | None:
    """Load latest state of batch run if table and row exist."""
    try:
        table = catalog.load_table((ns(prefix, "system"), "batch_runs"))
    except Exception:
        return None

    try:
        scanned = table.scan().to_arrow()
        if not scanned.num_rows:
            return None
        rows = [r for r in scanned.to_pylist() if r.get("run_id") == run_id]
        if not rows:
            return None
        # Sort by updated_at descending to get latest state
        rows.sort(key=lambda r: r.get("updated_at") or datetime.min.replace(tzinfo=UTC), reverse=True)
        return rows[0]
    except Exception:
        return None


def save_run(catalog, prefix: str, state: dict[str, Any]) -> None:
    """Persist batch run state using PyIceberg upsert on run_id."""
    catalog.create_namespace_if_not_exists(ns(prefix, "system"))
    table = catalog.create_table_if_not_exists(
        (ns(prefix, "system"), "batch_runs"),
        schema=BATCH_RUN_SCHEMA,
        properties=PROPERTIES,
    )
    now = datetime.now(UTC)
    row = {
        "run_id": str(state.get("run_id", "")),
        "mode": str(state.get("mode", "incremental")),
        "status": str(state.get("status", "STARTED")),
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
    state.update(updates)
    save_run(catalog, prefix, state)
    return state
