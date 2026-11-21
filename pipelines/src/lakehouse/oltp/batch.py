"""One OLTP batch run: source to Bronze, pre-merge checks, Silver merge, dbt Gold.

A failed check stops the run before Silver changes. An Airflow retry reruns everything with the same run ID:
Bronze drops that run's earlier rows, Silver merges by primary key and dbt rebuilds Gold from Silver.
"""

from datetime import UTC, datetime

from lakehouse.analytics.dbt import run_models
from lakehouse.oltp.bronze import ingest_table
from lakehouse.oltp.checks import check_run, source_reconciliation
from lakehouse.utils.runs import validate_run
from lakehouse.oltp.silver import merge_silver
from lakehouse.oltp.sources import SOURCE_TABLES
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.postgres import namespace_lock, source_query, source_read_transaction


def run_clock(as_of):
    observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("--as-of must include a timezone")
    return observed.astimezone(UTC)


def ingest(catalog, connection, prefix, specs, run_id, ingested_at, start, end, page_size):
    return {
        table.name: ingest_table(catalog, connection, prefix, table, run_id, ingested_at, start=start, end=end, page_size=page_size)
        for table in specs
    }


def run_pipeline(catalog, connection, prefix, *, run_id, mode, as_of, interval_start=None, interval_end=None, bucket, page_size=10000):
    validate_run(prefix, run_id, bucket, page_size)
    if mode not in {"full", "incremental"}:
        raise ValueError("Unknown batch mode")
    if (interval_start is None) != (interval_end is None) or (interval_start is not None) != (mode == "incremental"):
        raise ValueError("Incremental mode requires both interval bounds; full mode takes neither")
    observed = run_clock(as_of)
    for table in SOURCE_TABLES:
        source_query(table, interval_start, interval_end)
    full = mode == "full"
    ingested_at = datetime.now(UTC)

    with namespace_lock(connection, prefix):
        bronze = ingest(catalog, connection, prefix, SOURCE_TABLES, run_id, ingested_at, interval_start, interval_end, page_size)
        with source_read_transaction(connection):
            source = source_reconciliation(connection, SOURCE_TABLES) if full else None
        check_run(catalog, prefix, SOURCE_TABLES, run_id, source)
        catalog.create_namespace_if_not_exists(ns(prefix, "silver"))
        silver = {table.name: merge_silver(catalog, prefix, table, run_id, full=full) for table in SOURCE_TABLES}
        catalog.create_namespace_if_not_exists(ns(prefix, "gold"))
        gold = run_models(prefix, run_id, observed, group="oltp")
    return {"run_id": run_id, "mode": mode, "bronze_rows": bronze, "silver_rows": silver, "gold": gold}
