"""Export the synthetic dataset for database/schema.sql without connecting to a DB."""

import random
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path

from generator.catalog import build_catalog
from generator.config import GeneratorConfig
from generator.transactions import build_coupons, transaction_batches
from generator.validation import validate_scale

INSERT_BATCH_SIZE = 1000
TRANSACTION_TABLES = (
    "carts",
    "cart_items",
    "orders",
    "order_items",
    "payments",
    "order_status_history",
    "coupon_redemptions",
    "refunds",
    "return_requests",
    "return_items",
    "shipments",
    "shipment_attempts",
    "product_reviews",
    "inventory_transactions",
    "wishlist_items",
    "inventory",
)


@dataclass(frozen=True)
class DatasetSummary:
    sql_path: Path
    generation_run_id: str
    customers: int
    products: int
    variants: int
    orders: int


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, uuid.UUID):
        return f"X'{value.hex}'"
    if isinstance(value, datetime):
        return "'" + value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(value, (int, float)):
        return str(value)
    if not isinstance(value, str):
        raise TypeError(f"unsupported SQL value: {type(value).__name__}")
    # Hex encoding avoids dependence on MySQL's NO_BACKSLASH_ESCAPES setting.
    if "\\" in value or "\x00" in value:
        return f"CONVERT(X'{value.encode('utf-8').hex()}' USING utf8mb4)"
    return "'" + value.replace("'", "''") + "'"


def _write_insert(stream, table, columns, rows) -> None:
    iterator = iter(rows)
    while chunk := list(islice(iterator, INSERT_BATCH_SIZE)):
        stream.write(f"INSERT INTO `{table}` (" + ", ".join(f"`{column}`" for column in columns) + ") VALUES\n")
        stream.write(",\n".join("(" + ", ".join(_sql_literal(value) for value in row) + ")" for row in chunk))
        stream.write(";\n")


def export_sql(config: GeneratorConfig, output_path: Path) -> DatasetSummary:
    customers, products, variants, orders = validate_scale(config.scale)
    randomizer = random.Random(config.seed)
    catalog = build_catalog(config, randomizer)
    kinds = build_coupons(config, catalog)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(output_path.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(
                "-- MySQL data for database/schema.sql; initialize the empty schema separately.\n"
                "-- Product demand uses H&M 2019 rank weights; catalog mapping and other rates are scenario assumptions.\n"
                "-- See docs/project/GENERATOR_METRICS.md for definitions and provenance.\n"
                "SET NAMES utf8mb4;\nSTART TRANSACTION;\n"
            )
            for table, rows in catalog.tables.items():
                if rows:
                    columns = tuple(rows[0])
                    _write_insert(stream, table, columns, (tuple(row[column] for column in columns) for row in rows))
            pending = defaultdict(list)

            def flush():
                for table in TRANSACTION_TABLES:
                    rows = pending.get(table, [])
                    if rows:
                        columns = tuple(rows[0])
                        _write_insert(
                            stream, table, columns, (tuple(row[column] for column in columns) for row in rows)
                        )
                pending.clear()

            for batch in transaction_batches(config, catalog, randomizer, kinds):
                for table, rows in batch.items():
                    pending[table].extend(rows)
                if len(pending["orders"]) >= INSERT_BATCH_SIZE:
                    flush()
            flush()
            for coupon in catalog.tables["coupons"]:
                if coupon["used_count"]:
                    stream.write(
                        f"UPDATE `coupons` SET `used_count` = {coupon['used_count']}, "
                        f"`updated_at` = {_sql_literal(catalog.history_end)} WHERE `coupon_id` = {coupon['coupon_id']};\n"
                    )
            for customer in catalog.tables["customers"]:
                if customer.get("boom_count", 0):
                    stream.write(
                        f"UPDATE `customers` SET `boom_count` = {customer['boom_count']}, "
                        f"`is_cod_blocked` = {int(customer['is_cod_blocked'])}, "
                        f"`updated_at` = {_sql_literal(catalog.history_end)} WHERE `customer_id` = {customer['customer_id']};\n"
                    )
            stream.write("COMMIT;\n")
        temporary.replace(output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return DatasetSummary(output_path, config.generation_run_id, customers, products, variants, orders)
