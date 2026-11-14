"""The only module of datagen.logs that reads PostgreSQL: the OLTP rows the access log is anchored to."""

from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

import polars as pl

from datagen.presets import OltpPreset, Window

UTC_MICROS = pl.Datetime("us", "UTC")


class OltpMismatch(Exception):
    pass


class EmptyWindow(Exception):
    pass


@dataclass(frozen=True)
class LogInputs:
    orders: pl.DataFrame  # order_number, created_at, local_date, customer_key
    lines: pl.DataFrame  # order_number, product_id, product_slug, variant_key
    products: pl.DataFrame  # product_id, product_slug, weight (share of all units ever sold)
    variants: pl.DataFrame  # product_id, variant_key (active variants)
    customers: pl.Series  # customer_key of active customers, sorted


def _rows(connection, sql: str, parameters=None) -> list[tuple]:
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetchall()


def _frame(rows: list[tuple], schema: dict) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=schema, orient="row")


def _window_bounds(window: Window, zone: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(zone)
    return datetime.combine(window.start, time(), tz), datetime.combine(window.end, time(), tz)


WINDOW_ORDERS = "o.channel = 'online' AND o.created_at >= %(start)s AND o.created_at < %(end)s"


def read_inputs(connection, oltp: OltpPreset, window: Window) -> LogInputs:
    generated = _rows(
        connection,
        "SELECT EXISTS (SELECT 1 FROM orders WHERE data_origin = 'synthetic' AND generation_run_id = %s)",
        (oltp.run_id,),
    )[0][0]
    if not generated:
        raise OltpMismatch(
            f"Database không chứa dữ liệu OLTP của preset {oltp.run_id}; hãy sinh OLTP bằng preset đó trước"
        )
    start, end = _window_bounds(window, oltp.business_timezone)
    bounds = {"start": start, "end": end}
    orders = _frame(
        _rows(
            connection,
            f"""SELECT o.order_number, o.created_at, c.public_id::text
                FROM orders o JOIN customers c ON c.customer_id = o.customer_id
                WHERE {WINDOW_ORDERS} ORDER BY o.created_at, o.order_number""",
            bounds,
        ),
        {"order_number": pl.String, "created_at": UTC_MICROS, "customer_key": pl.String},
    )
    if orders.is_empty():
        raise EmptyWindow(f"Không có đơn online nào từ {window.start} tới trước {window.end}")
    orders = orders.select(
        "order_number",
        "created_at",
        pl.col("created_at").dt.convert_time_zone(oltp.business_timezone).dt.date().alias("local_date"),
        "customer_key",
    )
    lines = _frame(
        _rows(
            connection,
            f"""SELECT o.order_number, v.product_id, p.slug, v.public_id::text
                FROM order_items i
                JOIN orders o ON o.order_id = i.order_id
                JOIN product_variants v ON v.variant_id = i.variant_id
                JOIN products p ON p.product_id = v.product_id
                WHERE {WINDOW_ORDERS} ORDER BY o.order_number, i.order_item_id""",
            bounds,
        ),
        {"order_number": pl.String, "product_id": pl.Int64, "product_slug": pl.String, "variant_key": pl.String},
    )
    sold = _frame(
        _rows(
            connection,
            """SELECT p.product_id, p.slug, sum(i.quantity)
               FROM order_items i
               JOIN product_variants v ON v.variant_id = i.variant_id
               JOIN products p ON p.product_id = v.product_id
               WHERE EXISTS (SELECT 1 FROM product_variants a WHERE a.product_id = p.product_id AND a.is_active)
               GROUP BY p.product_id, p.slug ORDER BY p.product_id""",
        ),
        {"product_id": pl.Int64, "product_slug": pl.String, "units": pl.Int64},
    )
    products = sold.select("product_id", "product_slug", (pl.col("units") / pl.col("units").sum()).alias("weight"))
    variants = _frame(
        _rows(
            connection,
            "SELECT product_id, public_id::text FROM product_variants WHERE is_active ORDER BY product_id, variant_id",
        ),
        {"product_id": pl.Int64, "variant_key": pl.String},
    )
    customers = _frame(
        _rows(
            connection,
            "SELECT public_id::text FROM customers WHERE role = 'customer' AND status = 'active' ORDER BY 1",
        ),
        {"customer_key": pl.String},
    )["customer_key"]
    return LogInputs(orders=orders, lines=lines, products=products, variants=variants, customers=customers)
