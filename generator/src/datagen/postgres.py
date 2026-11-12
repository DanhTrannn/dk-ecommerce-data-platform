"""The only module that touches PostgreSQL: schema check, COPY loads and sequence sync."""

import io

import polars as pl
import psycopg2

from datagen.oltp.tables import ID_COLUMNS, LOAD_ORDER, SCHEMAS


class SchemaMismatch(Exception):
    pass


def connect(url: str):
    connection = psycopg2.connect(url, options="-c timezone=UTC")
    connection.autocommit = False
    return connection


def check_schema(connection) -> None:
    """Every declared column must exist, and every column PostgreSQL cannot fill itself must be declared."""
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT table_name, column_name,
                      is_nullable = 'NO' AND column_default IS NULL AND is_generated = 'NEVER' AND is_identity = 'NO'
               FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = ANY(%s)""",
            (list(SCHEMAS),),
        )
        rows = cursor.fetchall()
    live = {(table, column) for table, column, _ in rows}
    required = {(table, column) for table, column, needed in rows if needed}
    declared = {(table, column) for table, columns in SCHEMAS.items() for column in columns}
    problems = [f"{t}.{c} is declared but missing in the database" for t, c in sorted(declared - live)]
    problems += [f"{t}.{c} is required by the database but not declared" for t, c in sorted(required - declared)]
    if problems:
        raise SchemaMismatch("; ".join(problems))


def _exists(connection, sql: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT EXISTS ({sql})")
        return cursor.fetchone()[0]


def has_synthetic_data(connection) -> bool:
    return _exists(connection, "SELECT 1 FROM customers WHERE data_origin = 'synthetic'") or _exists(
        connection, "SELECT 1 FROM orders WHERE data_origin = 'synthetic'"
    )


def has_manual_orders(connection) -> bool:
    return _exists(connection, "SELECT 1 FROM orders WHERE data_origin = 'manual'")


def next_ids(connection) -> dict[str, int]:
    with connection.cursor() as cursor:
        result = {}
        for table, column in ID_COLUMNS.items():
            cursor.execute(f"SELECT coalesce(max({column}), 0) + 1 FROM public.{table}")
            result[table] = cursor.fetchone()[0]
        return result


def category_ids(connection) -> dict[str, int]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT lower(code), category_id FROM categories")
        return dict(cursor.fetchall())


def _csv(frame: pl.DataFrame) -> io.StringIO:
    # Quoting every string keeps '' apart from NULL, which COPY reads as an unquoted empty field.
    text = frame.write_csv(quote_style="non_numeric", datetime_format="%Y-%m-%dT%H:%M:%S%.6f%:z")
    return io.StringIO(text)


def copy_tables(connection, tables: dict[str, pl.DataFrame]) -> dict[str, int]:
    """COPY non-empty frames in foreign-key order inside the caller's transaction; returns rows per table."""
    counts = {}
    with connection.cursor() as cursor:
        for table in LOAD_ORDER:
            frame = tables.get(table)
            if frame is None or frame.height == 0:
                continue
            columns = ", ".join(frame.columns)
            cursor.copy_expert(f"COPY public.{table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)", _csv(frame))
            counts[table] = frame.height
    return counts


def sync_sequences(connection) -> None:
    with connection.cursor() as cursor:
        for table, column in ID_COLUMNS.items():
            cursor.execute(
                f"SELECT setval(pg_get_serial_sequence('public.{table}', '{column}'), max({column})) "
                f"FROM public.{table} HAVING max({column}) IS NOT NULL"
            )


def analyze(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("ANALYZE")
