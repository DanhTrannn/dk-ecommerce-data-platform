"""Python tasks of the OLTP DAGs, called by PythonOperator.

Airflow fills the parameters named after its context (run_id, dag_run, data_interval_start, data_interval_end).
The source is the `oltp_postgres` Airflow connection; its Extra sets a read-only UTC session.

- Full load (lakehouse_oltp_init): reads every source row created before the run's logical start
  (`dag_run.run_after`, which stays the same when a task is cleared), so every table stops at the same moment.
- Incremental load (lakehouse_oltp_incremental): reads the rows whose cursor falls in the run's data interval.
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


def bronze_full(table, run_id, dag_run):
    with source_connection() as connection:
        ingest_table(
            iceberg_catalog(),
            connection,
            TABLES_BY_NAME[table],
            run_id,
            datetime.now(UTC),
            created_before=dag_run.run_after.isoformat(),
        )


def bronze_incremental(table, run_id, data_interval_start=None, data_interval_end=None):
    if data_interval_start is None or data_interval_end is None:
        raise ValueError("An incremental run needs a data interval: let the schedule run it, or use `airflow backfill create`")
    with source_connection() as connection:
        ingest_table(
            iceberg_catalog(),
            connection,
            TABLES_BY_NAME[table],
            run_id,
            datetime.now(UTC),
            start=data_interval_start.isoformat(),
            end=data_interval_end.isoformat(),
        )


def check_full(table, run_id, dag_run):
    """Row rules on the run's Bronze rows, reconciled with the source at the same cutoff."""
    table = TABLES_BY_NAME[table]
    with source_connection() as connection, source_read_transaction(connection):
        source = source_reconciliation(connection, table, dag_run.run_after.isoformat())
    check_table(iceberg_catalog(), table, run_id, source)


def check_incremental(table, run_id):
    """Row rules on the run's Bronze rows; one interval cannot be reconciled with the whole source table."""
    check_table(iceberg_catalog(), TABLES_BY_NAME[table], run_id, None)


def foreign_keys(run_id):
    """Every child row has its parent; only a full load holds every parent row."""
    check_foreign_keys(iceberg_catalog(), run_id)


def silver_full(table, run_id):
    """Replace silver.<table> with the run's Bronze rows in one commit."""
    merge_silver(iceberg_catalog(), TABLES_BY_NAME[table], run_id, full=True)


def silver_incremental(table, run_id):
    """Upsert the run's Bronze rows into silver.<table>, never replacing a row with an older version."""
    merge_silver(iceberg_catalog(), TABLES_BY_NAME[table], run_id, full=False)


def maintain(action):
    """One maintenance action on every OLTP table: Bronze, Silver and each Gold model and seed."""
    tables = [
        *(("bronze", table.name) for table in SOURCE_TABLES),
        *(("silver", table.name) for table in SOURCE_TABLES),
        *(("gold", name) for name in gold_dependencies()),
    ]
    result = maintain_tables(iceberg_catalog(), tables, action)
    print(json.dumps(result, default=str), flush=True)
