"""Read immutable analytical snapshots through their SQL engine's native decoder."""

import os

import pyarrow as pa
from trino.auth import BasicAuthentication
from trino.dbapi import connect


def get_trino_connection():
    """Create Trino connection configured for DBT/ETL user."""
    return connect(
        host=os.environ.get("DBT_TRINO_HOST", "trino"),
        port=int(os.environ.get("DBT_TRINO_PORT", "8443")),
        user="dbt_writer",
        auth=BasicAuthentication("dbt_writer", os.environ["DBT_TRINO_PASSWORD"]),
        http_scheme="https",
        verify=os.environ.get("TRINO_CA_FILE", "/run/trino-ca/ca.pem"),
        timezone="UTC",
    )


def read_gold_snapshot(table, snapshot_id):
    namespace, name = table.name()[-2:]
    relation = ".".join('"' + part.replace('"', '""') + '"' for part in ("lakehouse_etl", namespace, name))
    connection = get_trino_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {relation} FOR VERSION AS OF {int(snapshot_id)}")
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, row)) for row in cursor.fetchall()]
        return pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    finally:
        connection.close()
