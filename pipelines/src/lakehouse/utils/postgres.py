"""PostgreSQL source SQL and JDBC properties, independent of Spark runtime."""

import os
import re
from datetime import datetime, timezone
from urllib.parse import quote

JDBC_DRIVER = "org.postgresql.Driver"
JDBC_JAR = "/opt/spark/jars/postgresql.jar"


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid PostgreSQL source identifier")
    return f'"{value}"'


def jdbc_options() -> dict[str, str]:
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
        "driver": JDBC_DRIVER,
        "url": f"jdbc:postgresql://{host}:{port}/{quote(os.environ['POSTGRES_ECOMMERCE_DB'], safe='')}",
        "user": os.environ["POSTGRES_ECOMMERCE_READER_USER"],
        "password": os.environ["POSTGRES_ECOMMERCE_READER_PASSWORD"],
        "currentSchema": "public",
        "readOnly": "true",
        "readOnlyMode": "always",
        "sessionInitStatement": "SET TIME ZONE 'UTC'",
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

    # Reuse validation and source credential names without carrying JDBC into the runtime.
    options = jdbc_options()
    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_SOURCE_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_SOURCE_PORT", "5432")),
        dbname=os.environ["POSTGRES_ECOMMERCE_DB"],
        user=options["user"],
        password=options["password"],
        connect_timeout=10,
        options="-c timezone=UTC -c default_transaction_read_only=on",
    )
    register_uuid(conn_or_curs=connection)
    register_default_json(conn_or_curs=connection, loads=lambda value: value)
    register_default_jsonb(conn_or_curs=connection, loads=lambda value: value)
    connection.set_session(readonly=True)
    return connection
