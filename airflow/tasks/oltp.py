"""Python tasks of the OLTP DAGs, called by PythonOperator.

Airflow fills the parameters named after its context (dag_run, data_interval_start, data_interval_end).
Each load truncates staging.<table> and extracts into it, merges the extract into Bronze, then Bronze into
Silver, so rerunning a task gives the same result.
The source is the `oltp_postgres` Airflow connection; its Extra sets a read-only UTC session.

- Full load (lakehouse_oltp_init): reads every source row created before the run's logical start
  (`dag_run.run_after`, which stays the same when a task is cleared), so every table stops at the same moment.
- Incremental load (lakehouse_oltp_incremental): reads the rows whose cursor falls in the run's data interval.
"""

import json
from contextlib import closing
from datetime import UTC, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from lakehouse.oltp.bronze import merge_bronze
from lakehouse.oltp.checks import check_foreign_keys, check_table, source_reconciliation
from lakehouse.oltp.schema import create_oltp_schema
from lakehouse.oltp.silver import merge_silver
from lakehouse.oltp.sources import SOURCE_TABLES, TABLES_BY_NAME
from lakehouse.oltp.staging import extract_table
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


def staging_full(table, dag_run):
    with source_connection() as connection:
        extract_table(
            iceberg_catalog(),
            connection,
            TABLES_BY_NAME[table],
            datetime.now(UTC),
            created_before=dag_run.run_after.isoformat(),
        )


def staging_incremental(table, data_interval_start=None, data_interval_end=None):
    if data_interval_start is None or data_interval_end is None:
        raise ValueError("An incremental run needs a data interval: let the schedule run it, or use `airflow backfill create`")
    with source_connection() as connection:
        extract_table(
            iceberg_catalog(),
            connection,
            TABLES_BY_NAME[table],
            datetime.now(UTC),
            start=data_interval_start.isoformat(),
            end=data_interval_end.isoformat(),
        )


def bronze_full(table):
    """Replace bronze.<table> with the extract in one commit."""
    merge_bronze(iceberg_catalog(), TABLES_BY_NAME[table], full=True)


def bronze_incremental(table):
    """Upsert the extract into bronze.<table>, never replacing a row with an older version."""
    merge_bronze(iceberg_catalog(), TABLES_BY_NAME[table], full=False)


def check_full(table, dag_run):
    """Row rules on the extract, reconciled with the source at the same cutoff."""
    table = TABLES_BY_NAME[table]
    with source_connection() as connection, source_read_transaction(connection):
        source = source_reconciliation(connection, table, dag_run.run_after.isoformat())
    check_table(iceberg_catalog(), table, source)


def check_incremental(table):
    """Row rules on the extract; one interval cannot be reconciled with the whole source table."""
    check_table(iceberg_catalog(), TABLES_BY_NAME[table], None)


def foreign_keys():
    """Every child row has its parent; only a full load holds every parent row."""
    check_foreign_keys(iceberg_catalog())


def silver_full(table):
    """Replace silver.<table> with all of Bronze in one commit."""
    merge_silver(iceberg_catalog(), TABLES_BY_NAME[table], full=True)


def silver_incremental(table):
    """Upsert the Bronze rows this load changed into silver.<table>."""
    merge_silver(iceberg_catalog(), TABLES_BY_NAME[table], full=False)


def maintain(action):
    """One maintenance action on every OLTP table: staging, Bronze, Silver and each Gold model and seed."""
    tables = [
        *(("staging", table.name) for table in SOURCE_TABLES),
        *(("bronze", table.name) for table in SOURCE_TABLES),
        *(("silver", table.name) for table in SOURCE_TABLES),
        *(("gold", name) for name in gold_dependencies()),
    ]
    result = maintain_tables(iceberg_catalog(), tables, action)
    print(json.dumps(result, default=str), flush=True)
