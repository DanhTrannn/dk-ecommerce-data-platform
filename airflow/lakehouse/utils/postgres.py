from contextlib import contextmanager
import re
from datetime import datetime, timezone

from psycopg import IsolationLevel


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid PostgreSQL source identifier")
    return f'"{value}"'


def _utc(value, label):
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")
    return moment.astimezone(timezone.utc)


def source_query(table, interval_start=None, interval_end=None, *, created_before=None):
    """Build the source SELECT for a cursor interval [start, end), or for rows created before a cutoff.

    The cutoff keeps every table of a full load at the same moment, although each is read at a different time.
    """
    columns = ", ".join(identifier(column) for column in table.columns)
    query = f'SELECT {columns} FROM "public".{identifier(table.name)}'
    if created_before is not None:
        if interval_start is not None or interval_end is not None:
            raise ValueError("Use either a cursor interval or a created_before cutoff")
        cutoff = _utc(created_before, "created_before")
        return f"{query} WHERE \"created_at\" < TIMESTAMPTZ '{cutoff.isoformat()}'"
    if interval_start is None and interval_end is None:
        return query
    lower, upper = _utc(interval_start, "Batch interval"), _utc(interval_end, "Batch interval")
    if lower >= upper:
        raise ValueError("interval_start must precede interval_end")
    column = identifier(table.cursor_field)
    return (
        f"{query} WHERE {column} >= TIMESTAMPTZ '{lower.isoformat()}' AND {column} < TIMESTAMPTZ '{upper.isoformat()}'"
    )


@contextmanager
def source_read_transaction(connection):
    connection.isolation_level = IsolationLevel.REPEATABLE_READ
    try:
        yield
    finally:
        connection.rollback()
