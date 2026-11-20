from contextlib import closing
from datetime import date, timedelta

from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.trino import get_trino_connection


def validate_bounds(start, end):
    if start is None and end is None:
        return None
    if start is None or end is None:
        raise ValueError("Provide both date bounds")
    lower, upper = date.fromisoformat(start), date.fromisoformat(end)
    if lower > upper:
        raise ValueError("The date start must not follow the date end")
    return lower.isoformat(), upper.isoformat()


def infer_date_bounds(prefix, inputs, business_date):
    namespace = ns(prefix, "silver")
    anchor = f"DATE '{business_date.isoformat()}'"
    sources = []
    for table, columns in (
        ("orders", ("created_at",)),
        ("shipments", ("created_at", "dispatched_at", "delivered_at")),
        ("return_requests", ("created_at",)),
    ):
        snapshot = inputs[table]
        if snapshot is None:
            continue
        relation = f'lakehouse_etl."{namespace}"."silver_{table}" FOR VERSION AS OF {snapshot}'
        lo = ", ".join(f"COALESCE(CAST(at_timezone(min({column}), 'Asia/Ho_Chi_Minh') AS date), {anchor})" for column in columns)
        hi = ", ".join(f"COALESCE(CAST(at_timezone(max({column}), 'Asia/Ho_Chi_Minh') AS date), {anchor})" for column in columns)
        sources.append(f"SELECT least({anchor}, {lo}) lo, greatest({anchor}, {hi}) hi FROM {relation}")

    lower = upper = business_date
    if sources:
        query = f"SELECT min(lo), max(hi) FROM ({' UNION ALL '.join(sources)})"
        with closing(get_trino_connection()) as connection, closing(connection.cursor()) as cursor:
            cursor.execute(query)
            start, end = cursor.fetchone()
        if start is not None:
            lower = min(lower, start)
            upper = max(upper, end)
    return (lower - timedelta(days=366)).isoformat(), (upper + timedelta(days=366)).isoformat()
