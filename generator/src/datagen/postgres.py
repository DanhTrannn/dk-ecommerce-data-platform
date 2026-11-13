"""The only module that touches PostgreSQL: schema check, bulk-load preparation, COPY loads and sequence sync."""

import io
from dataclasses import dataclass

import polars as pl
import psycopg2

from datagen.oltp.tables import ID_COLUMNS, LOAD_ORDER, SCHEMAS


UNIQUE = "ràng buộc unique"
INDEX = "index"
FOREIGN_KEY = "khóa ngoại"
LOCK_TIMEOUT = "10s"  # fail fast when another session holds a table, instead of waiting silently
MAINTENANCE_WORK_MEM = "1GB"  # sort memory for rebuilding the indexes after the load

# Primary keys, CHECK and NOT NULL stay: they are cheap per row and catch a bad batch while it loads.
_DROPPABLE = """
    SELECT kind, name, drop_statement, create_statement
    FROM (
        SELECT 1 AS step, %(foreign_key)s AS kind, conname::text AS name,
               format('ALTER TABLE %%s DROP CONSTRAINT %%I', conrelid::regclass, conname) AS drop_statement,
               format('ALTER TABLE %%s ADD CONSTRAINT %%I %%s', conrelid::regclass, conname,
                      pg_get_constraintdef(oid)) AS create_statement
        FROM pg_constraint
        WHERE contype = 'f' AND conrelid = ANY(%(tables)s::regclass[])
        UNION ALL
        SELECT 2, %(unique)s, conname::text,
               format('ALTER TABLE %%s DROP CONSTRAINT %%I', conrelid::regclass, conname),
               format('ALTER TABLE %%s ADD CONSTRAINT %%I %%s', conrelid::regclass, conname, pg_get_constraintdef(oid))
        FROM pg_constraint
        WHERE contype = 'u' AND conrelid = ANY(%(tables)s::regclass[])
        UNION ALL
        SELECT 3, %(index)s, indexrelid::regclass::text,
               format('DROP INDEX %%s', indexrelid::regclass),
               pg_get_indexdef(indexrelid)
        FROM pg_index i
        WHERE indrelid = ANY(%(tables)s::regclass[])
          AND NOT EXISTS (
              SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid AND c.contype IN ('p', 'u')
          )
    ) droppable
    ORDER BY step, name"""


class SchemaMismatch(Exception):
    pass


@dataclass(frozen=True)
class Rebuild:
    kind: str
    name: str
    statement: str


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


def begin_bulk_load(connection, tables) -> list[Rebuild]:
    """Drop the foreign keys, unique constraints and secondary indexes of tables inside the open transaction.

    Returns the statements that recreate them from the live catalog, foreign keys last because each one
    needs the index of the key it references. A rollback brings every dropped object back.
    """
    names = [f"public.{table}" for table in tables]
    with connection.cursor() as cursor:
        cursor.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        cursor.execute(f"SET LOCAL maintenance_work_mem = '{MAINTENANCE_WORK_MEM}'")
        cursor.execute(_DROPPABLE, {"tables": names, "foreign_key": FOREIGN_KEY, "unique": UNIQUE, "index": INDEX})
        rows = cursor.fetchall()
        for _, _, drop_statement, _ in rows:
            cursor.execute(drop_statement)
    rebuilds = [Rebuild(kind, name, create_statement) for kind, name, _, create_statement in rows]
    return sorted(rebuilds, key=lambda item: item.kind == FOREIGN_KEY)


def rebuild(connection, item: Rebuild) -> None:
    with connection.cursor() as cursor:
        cursor.execute(item.statement)


def _csv(frame: pl.DataFrame) -> io.BytesIO:
    # Quoting every string keeps '' apart from NULL, which COPY reads as an unquoted empty field.
    buffer = io.BytesIO()
    frame.write_csv(buffer, quote_style="non_numeric", datetime_format="%Y-%m-%dT%H:%M:%S%.6f%:z")
    buffer.seek(0)
    return buffer


def copy_tables(connection, tables: dict[str, pl.DataFrame]) -> dict[str, int]:
    """COPY non-empty frames in foreign-key order inside the caller's transaction; returns rows per table."""
    counts = {}
    with connection.cursor() as cursor:
        for table in LOAD_ORDER:
            frame = tables.get(table)
            if frame is None or frame.height == 0:
                continue
            columns = ", ".join(frame.columns)
            cursor.copy_expert(
                f"COPY public.{table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)", _csv(frame)
            )
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


def number(connection, sql: str, parameters=None) -> float:
    """The single value of an aggregate query; NaN when the query has nothing to measure."""
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        value = cursor.fetchone()[0]
    return float("nan") if value is None else float(value)
