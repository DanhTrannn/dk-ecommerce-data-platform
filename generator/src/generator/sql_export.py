"""Export the synthetic dataset using the PostgreSQL artifact/import contract."""

import json
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import islice
from pathlib import Path
from typing import TextIO

from generator import __version__
from generator.catalog import build_catalog
from generator.config import GeneratorConfig
from generator.locations import locations_sql
from generator.postgres_sql import SQL_FORMAT, lock_sql, restart_identities_sql, verification_sql
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
    cities: int = 0
    stores: int = 0


def _sql_literal(value) -> str:
    if isinstance(value, uuid.UUID):
        return f"'{value}'::uuid"
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        normalized = (value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)).replace(tzinfo=None)
        return f"'{normalized.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if "\x00" in value:
        raise ValueError("PostgreSQL text cannot contain NUL")
    escaped = (
        value.replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    )
    return f"E'{escaped}'"


def _write_insert(stream, table, columns, rows) -> None:
    iterator = iter(rows)
    while chunk := list(islice(iterator, INSERT_BATCH_SIZE)):
        stream.write(f'INSERT INTO public."{table}" (' + ", ".join(f'"{column}"' for column in columns) + ") VALUES\n")
        stream.write(",\n".join("(" + ", ".join(_sql_literal(value) for value in row) + ")" for row in chunk))
        stream.write(";\n")


def _write_header(stream: TextIO, config: GeneratorConfig, generation_run_id: str) -> None:
    metadata = {
        "sql_format": SQL_FORMAT,
        "generator_version": __version__,
        "logical_identity": config.logical_identity,
        "generation_run_id": generation_run_id,
        "counts": {
            **{key: config.scale[key] for key in ("customers", "products", "variants", "orders")},
            **config.locations.counts,
        },
        "locations": {"store_codes": list(config.locations.store_codes)},
    }
    stream.write(
        "-- TLCN deterministic PostgreSQL dataset\n"
        f"-- import_metadata: {json.dumps(metadata, sort_keys=True)}\n"
        f"-- scenario_id: {config.scenario_id}\n"
        f"-- seed: {config.seed}\n"
        f"-- anchor_time: {config.anchor_time.isoformat()}\n"
        "-- Prerequisite: PostgreSQL source schema from database/schema.sql.\n"
        "-- Trusted generated SQL only; import is fail-fast and rejects duplicate datasets.\n"
        "BEGIN;\n"
        "SET LOCAL search_path TO public;\n"
        "SET LOCAL TIME ZONE 'UTC';\n"
        "SET LOCAL standard_conforming_strings=on;\n"
    )
    stream.write(lock_sql() + "\n")


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
            _write_header(stream, config, config.generation_run_id)
            stream.write(
                locations_sql(
                    config.locations,
                    catalog.history_start - timedelta(days=config.policies["history"]["master_data_lead_days"]),
                    _sql_literal,
                )
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
                        f"UPDATE public.coupons SET used_count = {coupon['used_count']}, "
                        f"updated_at = {_sql_literal(catalog.history_end)} WHERE coupon_id = {coupon['coupon_id']};\n"
                    )
            for customer in catalog.tables["customers"]:
                if customer.get("boom_count", 0):
                    stream.write(
                        f"UPDATE public.customers SET boom_count = {customer['boom_count']}, "
                        f"is_cod_blocked = {_sql_literal(customer['is_cod_blocked'])}, "
                        f"updated_at = {_sql_literal(catalog.history_end)} WHERE customer_id = {customer['customer_id']};\n"
                    )
            stream.write(verification_sql(config.generation_run_id, catalog.id_base, config.scale))
            stream.write(restart_identities_sql())
            stream.write("COMMIT;\n")
        temporary.replace(output_path)
    finally:
        temporary.unlink(missing_ok=True)
    return DatasetSummary(
        output_path, config.generation_run_id, customers, products, variants, orders, **config.locations.counts
    )
