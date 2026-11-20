"""Batch pipeline orchestration holding lock from ingestion to publication."""

from datetime import UTC, datetime
import json

from lakehouse.analytics.commerce import execute_build_commerce
from lakehouse.oltp.polars.pipeline import execute_batch, selected_tables, validate_run
from lakehouse.oltp.quality.publication import capture_snapshot_map
from lakehouse.oltp.run_state import (
    BatchRunIdentity,
    compute_config_fingerprint,
    load_run,
    save_run,
    transition_run_state,
)
from lakehouse.utils.postgres import namespace_lock, source_query


def run_pipeline(
    catalog,
    connection,
    prefix: str,
    *,
    bucket: str,
    identity: BatchRunIdentity,
    page_size: int = 10000,
) -> dict:
    """Orchestrate entire batch run holding namespace advisory lock throughout."""
    validate_run(prefix, identity.run_id, bucket, page_size)
    fingerprint = compute_config_fingerprint(identity)
    specs = selected_tables()

    # Pre-validate interval and source queries
    if (identity.interval_start is None) != (identity.interval_end is None):
        raise ValueError("Both interval bounds are required")
    for table in specs:
        source_query(table, identity.interval_start, identity.interval_end)

    with namespace_lock(connection, prefix):
        state = load_run(catalog, prefix, identity.run_id)

        # Idempotency check: if already PUBLISHED, return existing publication
        if state and state.get("status") == "PUBLISHED":
            if state.get("config_fingerprint") != fingerprint:
                raise ValueError("Run ID exists with different configuration fingerprint")
            return state

        # If existing run has mismatched configuration fingerprint, reject
        if state and state.get("config_fingerprint") != fingerprint:
            raise ValueError(f"Run {identity.run_id} resumed with mismatched configuration")

        now = datetime.now(UTC)
        if not state:
            state = {
                "run_id": identity.run_id,
                "mode": identity.mode,
                "status": "STARTED",
                "attempt": 1,
                "config_fingerprint": fingerprint,
                "interval_start": identity.interval_start or "",
                "interval_end": identity.interval_end or "",
                "as_of": identity.as_of,
                "batch_date": identity.batch_date,
                "contract_version": identity.contract_version,
                "bronze_snapshots_json": "{}",
                "silver_snapshots_json": "{}",
                "gold_snapshots_json": "{}",
                "started_at": now,
                "updated_at": now,
            }
            save_run(catalog, prefix, state)
        else:
            state["attempt"] = int(state.get("attempt", 1)) + 1
            save_run(catalog, prefix, state)

        # Phase 1: Ingestion (if not already INGESTED or higher)
        if state.get("status") in ("STARTED", "FAILED"):
            try:
                execute_batch(
                    catalog,
                    connection,
                    prefix,
                    bucket,
                    identity.run_id,
                    specs,
                    identity.interval_start,
                    identity.interval_end,
                    page_size,
                )
                # Capture and record snapshot maps
                silver_snapshots = capture_snapshot_map(catalog, prefix, silver_only=True)
                state = transition_run_state(
                    catalog,
                    prefix,
                    state,
                    "INGESTED",
                    silver_snapshots_json=json.dumps(silver_snapshots),
                )
            except Exception as e:
                transition_run_state(
                    catalog,
                    prefix,
                    state,
                    "FAILED",
                    error_phase="ingestion",
                    error_message=str(e),
                )
                raise

        # Retrieve pinned Silver snapshots from state
        saved_silver = json.loads(state.get("silver_snapshots_json") or "{}")

        # Phase 2: Build Gold, Quality Gate and Publication
        try:
            build_result = execute_build_commerce(
                catalog,
                prefix,
                identity.run_id,
                as_of=identity.as_of,
                batch_date=identity.batch_date,
                verify_only=False,
                silver_snapshots=saved_silver,
            )
            gold_snapshots = build_result.get("snapshots", {})
            state = transition_run_state(
                catalog,
                prefix,
                state,
                "PUBLISHED",
                gold_snapshots_json=json.dumps(gold_snapshots),
            )
            return state
        except Exception as e:
            transition_run_state(
                catalog,
                prefix,
                state,
                "FAILED",
                error_phase="build_and_publish",
                error_message=str(e),
            )
            raise
