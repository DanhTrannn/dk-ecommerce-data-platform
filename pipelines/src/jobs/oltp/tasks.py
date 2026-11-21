"""The OLTP DAGs' Python tasks; Airflow calls them in the pipeline venv with the run context it renders.

`run` holds that context: run_id, mode, run_started_at, interval_start and interval_end.
A full run reads every row created before run_started_at and ignores the interval. An incremental run reads the
rows whose cursor falls in [interval_start, interval_end).
"""

import os
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime

from lakehouse.oltp.bronze import ingest_table
from lakehouse.oltp.checks import check_foreign_keys, check_table, source_reconciliation
from lakehouse.oltp.schema import create_oltp_schema
from lakehouse.oltp.silver import merge_silver
from lakehouse.oltp.sources import TABLES_BY_NAME
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader, source_read_transaction
from lakehouse.utils.runs import validate_run

PAGE_SIZE = 10000


@dataclass(frozen=True)
class Run:
    run_id: str
    mode: str
    run_started_at: str
    interval_start: str = ""
    interval_end: str = ""

    def __post_init__(self):
        if self.mode not in ("full", "incremental"):
            raise ValueError("mode must be full or incremental")
        if datetime.fromisoformat(self.run_started_at).tzinfo is None:
            raise ValueError("run_started_at must include a timezone")
        if self.mode == "incremental" and not (self.interval_start and self.interval_end):
            raise ValueError(
                "An incremental run needs a data interval: let the schedule run it, or use `airflow backfill create`"
            )
        validate_run(self.run_id, os.environ["MINIO_LAKEHOUSE_BUCKET"], PAGE_SIZE)

    @property
    def full(self):
        return self.mode == "full"


def create_schema():
    with closing(postgres_reader()) as connection:
        create_oltp_schema(iceberg_catalog(), connection)


def bronze(table, run):
    table, run = TABLES_BY_NAME[table], Run(**run)
    with closing(postgres_reader()) as connection:
        ingest_table(
            iceberg_catalog(),
            connection,
            table,
            run.run_id,
            datetime.now(UTC),
            start=None if run.full else run.interval_start,
            end=None if run.full else run.interval_end,
            created_before=run.run_started_at if run.full else None,
            page_size=PAGE_SIZE,
        )


def check(table, run):
    """Row rules on the run's Bronze rows; a full run also reconciles them with the source at the same cutoff."""
    table, run = TABLES_BY_NAME[table], Run(**run)
    source = None
    if run.full:
        with closing(postgres_reader()) as connection, source_read_transaction(connection):
            source = source_reconciliation(connection, table, run.run_started_at)
    check_table(iceberg_catalog(), table, run.run_id, source)


def foreign_keys(run):
    """Only a full run holds every parent row, so an incremental run skips this check."""
    run = Run(**run)
    if not run.full:
        print("CHECKS foreign_keys skipped (incremental run)", flush=True)
        return
    check_foreign_keys(iceberg_catalog(), run.run_id)


def silver(table, run):
    table, run = TABLES_BY_NAME[table], Run(**run)
    merge_silver(iceberg_catalog(), table, run.run_id, full=run.full)
