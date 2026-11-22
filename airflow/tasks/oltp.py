"""Callables of the OLTP DAGs.

Airflow fills the parameters named after its context keys: dag_run, data_interval_start and data_interval_end.
A full load cuts off at `dag_run.run_after`, which stays the same when a task is cleared and retried.
"""

import json
from contextlib import closing
from datetime import UTC, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset

from lakehouse.oltp.bronze import merge_bronze, purge_bronze
from lakehouse.oltp.reconcile import reconcile_table, source_reconciliation
from lakehouse.oltp.schema import MART_STATE, create_oltp_schema
from lakehouse.oltp.sources import SOURCE_TABLES, TABLES_BY_NAME
from lakehouse.oltp.staging import extract_table
from lakehouse.oltp.sync import detect_deletes, load_bronze, sync_schema
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.maintenance import maintain_tables
from lakehouse.utils.postgres import source_read_transaction
from tasks.dbt import gold_dependencies

SOURCE_CONN_ID = "oltp_postgres"
# Emitted by each successful incremental run; the sync DAG runs on it, so the two never run at once.
OLTP_LOADED = Asset("lakehouse_oltp_loaded")


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


def schema(table):
    with source_connection() as connection:
        sync_schema(iceberg_catalog(), connection, TABLES_BY_NAME[table])


def detect(table, dag_run):
    with source_connection() as connection:
        detect_deletes(iceberg_catalog(), connection, TABLES_BY_NAME[table], dag_run.run_after)


def bronze_sync(table):
    load_bronze(iceberg_catalog(), TABLES_BY_NAME[table])


def bronze_full(table):
    merge_bronze(iceberg_catalog(), TABLES_BY_NAME[table], full=True)


def bronze_incremental(table):
    merge_bronze(iceberg_catalog(), TABLES_BY_NAME[table], full=False)


def purge(table):
    purge_bronze(iceberg_catalog(), TABLES_BY_NAME[table])


def reconcile(table, dag_run):
    table = TABLES_BY_NAME[table]
    with source_connection() as connection, source_read_transaction(connection):
        source = source_reconciliation(connection, table, dag_run.run_after.isoformat())
    reconcile_table(iceberg_catalog(), table, source)


def maintain(action):
    # Only Bronze is compacted here: every load rewrites staging, and the optimize task compacts the dbt-written
    # Silver and Gold, whose merges leave position deletes PyIceberg cannot read.
    tables = [("bronze", table.name) for table in SOURCE_TABLES]
    if action != "compact":
        tables += [
            *(("staging", table.name) for table in SOURCE_TABLES),
            *(("silver", table.name) for table in SOURCE_TABLES),
            *(("gold", name) for name in gold_dependencies()),
            MART_STATE,
        ]
    result = maintain_tables(iceberg_catalog(), tables, action)
    print(json.dumps(result, default=str), flush=True)
