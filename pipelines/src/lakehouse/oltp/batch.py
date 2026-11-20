"""Batch pipeline orchestration holding the namespace lock through publication."""

from contextlib import closing, nullcontext
from datetime import UTC, date, datetime
import json

from lakehouse.analytics.commerce import build_gold, resolve_date_bounds, run_clock
from lakehouse.oltp.contract_migration import prepare_schema
from lakehouse.oltp.date_bounds import validate_bounds
from lakehouse.oltp.polars.pipeline import execute_batch, selected_tables, validate_run
from lakehouse.oltp.publication import load_publication, publish
from lakehouse.oltp.quality.publication import audit, capture_layer
from lakehouse.oltp.run_state import compute_config_fingerprint, load_run, resume_cutoff, save_run, transition_run_state
from lakehouse.utils.postgres import namespace_lock, source_query
from lakehouse.utils.trino import get_trino_connection


def continue_run(catalog, connection, prefix, bucket, identity, page_size, specs, state, observed):
    stage = state["status"]
    if stage == "FAILED":
        stage = state.get("resume_status") or ("INGESTED" if state.get("error_phase") == "build_and_publish" else "STARTED")

    phase = "ingestion"
    try:
        if stage == "STARTED":
            prepare_schema(catalog, prefix, migrate=identity.migrate_schema)
            counters = execute_batch(
                catalog, connection, prefix, bucket, identity.run_id, specs,
                identity.interval_start, identity.interval_end, page_size,
            )
            bronze = capture_layer(catalog, prefix, "bronze")
            silver = capture_layer(catalog, prefix, "silver")
            state = transition_run_state(
                catalog, prefix, state, "INGESTED",
                bronze_snapshots_json=json.dumps(bronze),
                silver_snapshots_json=json.dumps(silver),
                counters_json=json.dumps(counters),
            )
            stage = "INGESTED"
        else:
            silver = json.loads(state["silver_snapshots_json"])

        phase = "gold_build"
        if stage == "INGESTED":
            bounds = json.loads(state.get("date_bounds_json") or "null")
            if bounds is None:
                bounds = resolve_date_bounds(catalog, prefix, silver, observed, identity.date_start, identity.date_end)
                state = transition_run_state(catalog, prefix, state, "INGESTED", date_bounds_json=json.dumps(bounds))
            gold, _ = build_gold(catalog, prefix, identity.run_id, observed, silver, bounds)
            state = transition_run_state(catalog, prefix, state, "BUILT", gold_snapshots_json=json.dumps(gold))
            stage = "BUILT"
        else:
            gold = json.loads(state["gold_snapshots_json"])

        phase = "audit"
        if stage == "BUILT":
            trino = closing(get_trino_connection()) if identity.trino_reconciliation else nullcontext()
            with trino as reader, closing(reader.cursor()) if reader else nullcontext() as cursor:
                quality = audit(
                    catalog, prefix, identity.run_id, identity.batch_date, silver, gold,
                    observed=observed, trino_cursor=cursor,
                )
            state = transition_run_state(
                catalog, prefix, state, "VALIDATED", quality_summary_json=json.dumps(quality),
            )
            stage = "VALIDATED"
        else:
            quality = json.loads(state["quality_summary_json"])

        phase = "publication"
        existing = load_publication(catalog, prefix, identity.run_id)
        if existing is None:
            publish(catalog, prefix, identity.run_id, silver | gold, quality)
        elif json.loads(existing["snapshots_json"]) != silver | gold:
            raise ValueError("Committed publication does not match batch state")
        return transition_run_state(catalog, prefix, state, "PUBLISHED")
    except Exception as error:
        try:
            transition_run_state(
                catalog, prefix, state, "FAILED", resume_status=stage,
                error_phase=phase, error_message=type(error).__name__,
            )
        except Exception as state_error:
            error.add_note(f"Failed to persist batch status: {type(state_error).__name__}")
        raise


def run_pipeline(catalog, connection, prefix, *, bucket, identity, page_size=10000):
    validate_run(prefix, identity.run_id, bucket, page_size)
    if identity.namespace_prefix != prefix:
        raise ValueError("Batch identity does not match the target namespace")
    if identity.mode not in {"full", "incremental"}:
        raise ValueError("Unknown batch mode")
    has_interval = identity.interval_start is not None and identity.interval_end is not None
    if (identity.interval_start is None) != (identity.interval_end is None) or has_interval != (identity.mode == "incremental"):
        raise ValueError("Incremental mode requires both interval bounds; full mode takes neither")
    if identity.migrate_schema and identity.mode != "full":
        raise ValueError("Schema migration requires a full batch run")
    observed = run_clock(identity.as_of)
    date.fromisoformat(identity.batch_date)
    validate_bounds(identity.date_start, identity.date_end)
    fingerprint = compute_config_fingerprint(identity)
    specs = selected_tables()
    for spec in specs:
        source_query(spec, identity.interval_start, identity.interval_end)

    with namespace_lock(connection, prefix):
        state = load_run(catalog, prefix, identity.run_id)
        if state is not None:
            if state["config_fingerprint"] != fingerprint:
                raise ValueError("Run ID exists with a different configuration fingerprint")
            if state["status"] == "PUBLISHED":
                return state
            updated_at = state.get("updated_at")
            if state["status"] == "ABANDONED" or (updated_at and updated_at < resume_cutoff()):
                if state["status"] != "ABANDONED":
                    existing = load_publication(catalog, prefix, identity.run_id)
                    if existing is not None:
                        snapshots = json.loads(state["silver_snapshots_json"]) | json.loads(state["gold_snapshots_json"])
                        if json.loads(existing["snapshots_json"]) != snapshots:
                            raise ValueError("Committed publication does not match batch state")
                        return transition_run_state(catalog, prefix, state, "PUBLISHED")
                    transition_run_state(catalog, prefix, state, "ABANDONED")
                raise ValueError("Batch resume retention expired; use a new run ID")
            state = {**state, "attempt": state["attempt"] + 1}
        else:
            state = {
                "run_id": identity.run_id,
                "mode": identity.mode,
                "status": "STARTED",
                "attempt": 1,
                "config_fingerprint": fingerprint,
                "interval_start": identity.interval_start,
                "interval_end": identity.interval_end,
                "as_of": identity.as_of,
                "batch_date": identity.batch_date,
                "contract_version": identity.contract_version,
                "started_at": datetime.now(UTC),
            }
        save_run(catalog, prefix, state)
        return continue_run(catalog, connection, prefix, bucket, identity, page_size, specs, state, observed)
