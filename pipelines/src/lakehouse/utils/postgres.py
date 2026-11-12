"""PostgreSQL source SQL, dedicated reader configuration and writer locking."""

import os
import hashlib
from contextlib import contextmanager
import re
from datetime import datetime, timezone


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid PostgreSQL source identifier")
    return f'"{value}"'


def reader_settings():
    required = ("POSTGRES_ECOMMERCE_DB", "POSTGRES_ECOMMERCE_READER_USER", "POSTGRES_ECOMMERCE_READER_PASSWORD")
    if any(not os.environ.get(name) for name in required):
        raise ValueError("Configure the PostgreSQL source database and dedicated reader credentials")
    host = os.environ.get("POSTGRES_SOURCE_HOST", "postgres")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+|\[[A-Fa-f0-9:]+\]", host):
        raise ValueError("Invalid PostgreSQL source host")
    port = int(os.environ.get("POSTGRES_SOURCE_PORT", "5432"))
    if not 1 <= port <= 65535:
        raise ValueError("Invalid PostgreSQL source port")
    return {
        "host": host,
        "port": port,
        "dbname": os.environ["POSTGRES_ECOMMERCE_DB"],
        "user": os.environ["POSTGRES_ECOMMERCE_READER_USER"],
        "password": os.environ["POSTGRES_ECOMMERCE_READER_PASSWORD"],
    }


def source_query(table, interval_start=None, interval_end=None):
    query = f'SELECT * FROM "public".{identifier(table.name)}'
    if interval_start is None and interval_end is None:
        return query
    lower = datetime.fromisoformat(interval_start.replace("Z", "+00:00"))
    upper = datetime.fromisoformat(interval_end.replace("Z", "+00:00"))
    if lower.tzinfo is None or upper.tzinfo is None:
        raise ValueError("Batch interval must include a timezone")
    lower, upper = lower.astimezone(timezone.utc), upper.astimezone(timezone.utc)
    if lower >= upper:
        raise ValueError("interval_start must precede interval_end")
    column = identifier(table.cursor_field)
    return (
        f"{query} WHERE {column} >= TIMESTAMPTZ '{lower.isoformat()}' AND {column} < TIMESTAMPTZ '{upper.isoformat()}'"
    )


def page_query(query, pk, last_pk, limit):
    if type(limit) is not int or limit <= 0 or (last_pk is not None and type(last_pk) is not int):
        raise ValueError("Invalid source pagination")
    key = identifier(pk)
    after = f" WHERE {key} > {last_pk}" if last_pk is not None else ""
    return f"SELECT * FROM ({query}) source_rows{after} ORDER BY {key} LIMIT {limit}"


def postgres_reader():
    import psycopg2
    from psycopg2.extras import register_default_json, register_default_jsonb, register_uuid

    settings = reader_settings()
    connection = psycopg2.connect(
        **settings,
        connect_timeout=10,
        options="-c timezone=UTC -c default_transaction_read_only=on",
    )
    register_uuid(conn_or_curs=connection)
    register_default_json(conn_or_curs=connection, loads=lambda value: value)
    register_default_jsonb(conn_or_curs=connection, loads=lambda value: value)
    connection.set_session(readonly=True)
    return connection


@contextmanager
def namespace_lock(connection, namespace):
    """Serialize native source and Gold jobs using one session-scoped namespace key."""
    lock = int.from_bytes(hashlib.sha256(namespace.encode()).digest()[:8], "big", signed=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock,))
        if not cursor.fetchone()[0]:
            raise RuntimeError("Another writer owns this batch namespace")
    try:
        yield
    finally:
        connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (lock,))
