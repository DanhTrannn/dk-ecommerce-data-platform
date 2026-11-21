"""Python tasks of the OLTP DAGs, called by PythonOperator.

Airflow fills the parameters named after its context (run_id, dag_run, data_interval_start, data_interval_end).
A full run reads every source row created before the run's logical start (`dag_run.run_after`, which stays the same
when a task is cleared), so every table of one run stops at the same moment. An incremental run reads the rows
whose cursor falls in the run's data interval. The source is the `oltp_postgres` Airflow connection; its Extra sets
a read-only UTC session.
"""

import json
from contextlib import closing
from datetime import UTC, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from lakehouse.oltp.bronze import ingest_table
from lakehouse.oltp.checks import check_foreign_keys, check_table, source_reconciliation
from lakehouse.oltp.schema import create_oltp_schema
from lakehouse.oltp.silver import merge_silver
from lakehouse.oltp.sources import SOURCE_TABLES, TABLES_BY_NAME
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.maintenance import maintain_tables
from lakehouse.utils.postgres import source_read_transaction
from tasks.dbt import gold_dependencies

SOURCE_CONN_ID = "oltp_postgres"


def source_connection():
    return closing(PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_conn())


def create_schema():
    with source_connection() as connection:
        create_oltp_schema(iceberg_catalog(), connection)


def bronze(table, mode, run_id, dag_run, data_interval_start=None, data_interval_end=None):
    if mode == "full":
        window = {"start": None, "end": None, "created_before": dag_run.run_after.isoformat()}
    elif data_interval_start is None or data_interval_end is None:
        raise ValueError("An incremental run needs a data interval: let the schedule run it, or use `airflow backfill create`")
    else:
        window = {"start": data_interval_start.isoformat(), "end": data_interval_end.isoformat(), "created_before": None}
    with source_connection() as connection:
        ingest_table(iceberg_catalog(), connection, TABLES_BY_NAME[table], run_id, datetime.now(UTC), **window)


def check(table, mode, run_id, dag_run):
    """Row rules on the run's Bronze rows; a full run also reconciles them with the source at the same cutoff."""
    table = TABLES_BY_NAME[table]
    source = None
    if mode == "full":
        with source_connection() as connection, source_read_transaction(connection):
            source = source_reconciliation(connection, table, dag_run.run_after.isoformat())
    check_table(iceberg_catalog(), table, run_id, source)


def foreign_keys(run_id):
    """Only a full run holds every parent row, so only the init DAG runs this check."""
    check_foreign_keys(iceberg_catalog(), run_id)


def silver(table, mode, run_id):
    merge_silver(iceberg_catalog(), TABLES_BY_NAME[table], run_id, full=mode == "full")


def maintain(action):
    """One maintenance action on every OLTP table: Bronze, Silver and each Gold model and seed."""
    tables = [
        *(("bronze", table.name) for table in SOURCE_TABLES),
        *(("silver", table.name) for table in SOURCE_TABLES),
        *(("gold", name) for name in gold_dependencies()),
    ]
    result = maintain_tables(iceberg_catalog(), tables, action)
    print(json.dumps(result, default=str), flush=True)
