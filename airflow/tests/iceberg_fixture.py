import tempfile
import uuid
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from lakehouse.oltp.staging import LINEAGE_FIELDS
from lakehouse.oltp.schema import LAYERS, create_staging_and_bronze
from lakehouse.oltp.sources import SOURCE_TABLES

AT = datetime(2026, 11, 20, 3, tzinfo=UTC)
TABLES = {table.name: table for table in SOURCE_TABLES}


def sql_catalog(test_case):
    directory = tempfile.TemporaryDirectory()
    test_case.addCleanup(directory.cleanup)
    return SqlCatalog("test", uri=f"sqlite:///{directory.name}/catalog.db", warehouse=f"file://{directory.name}")


def create_tables(catalog):
    """Create the tables the init DAG creates, typed like the test pages.

    Tables the tests never load take types guessed from their column names.
    """
    for layer in LAYERS:
        catalog.create_namespace(layer)
    for name, table in TABLES.items():
        columns = page(name, []).schema if name in SOURCE_TYPES else guessed_schema(table)
        create_staging_and_bronze(catalog, table, pa.schema([*columns, *LINEAGE_FIELDS]))


def guessed_schema(table):
    counts = {"quantity", "rating", "attempt_count", "boom_count", "used_count", "quantity_delta", "discount_value",
              "coupon_value_snapshot", "total_usage_limit", "per_customer_usage_limit"}

    def kind(name):
        if name.endswith(("_id", "_vnd")) or name in counts:
            return pa.int64()
        if name.startswith("is_"):
            return pa.bool_()
        if name.endswith("_at"):
            return pa.timestamp("us", tz="UTC")
        return pa.string()

    return pa.schema([pa.field(name, kind(name)) for name in table.columns])


SOURCE_TYPES = {
    "categories": {
        "category_id": pa.int64(),
        "public_id": pa.string(),
        "parent_category_id": pa.int64(),
        "code": pa.string(),
        "name": pa.string(),
        "is_active": pa.bool_(),
        "created_at": pa.timestamp("us", tz="UTC"),
        "updated_at": pa.timestamp("us", tz="UTC"),
    },
    "orders": {name: pa.int64() for name in TABLES["orders"].columns}
    | {
        name: pa.string()
        for name in (
            "order_number",
            "checkout_idempotency_key",
            "status",
            "payment_method",
            "coupon_code_snapshot",
            "coupon_type_snapshot",
            "data_origin",
            "generation_run_id",
            "channel",
        )
    }
    | {
        name: pa.timestamp("us", tz="UTC")
        for name in ("created_at", "updated_at", "paid_at", "confirmed_at", "completed_at", "cancelled_at")
    },
    "order_items": {name: pa.int64() for name in TABLES["order_items"].columns}
    | {name: pa.string() for name in TABLES["order_items"].columns if name.endswith("_snapshot")}
    | {"public_id": pa.string(), "product_public_id_snapshot": pa.string(), "quantity": pa.int32()}
    | {"created_at": pa.timestamp("us", tz="UTC")},
    "inventory_transactions": {name: pa.int64() for name in TABLES["inventory_transactions"].columns}
    | {name: pa.string() for name in ("public_id", "location_type", "movement_type", "reference_code", "notes")}
    | {"quantity_delta": pa.int32(), "created_at": pa.timestamp("us", tz="UTC")},
}


def page(table, rows):
    columns = TABLES[table].columns
    schema = pa.schema([(name, SOURCE_TYPES[table][name]) for name in columns])
    return pa.Table.from_pylist([{name: row.get(name) for name in columns} for row in rows], schema=schema)


def category(category_id, name=None, later=False):
    return dict(
        category_id=category_id,
        public_id=str(uuid.UUID(int=category_id)),
        code=f"c{category_id}",
        name=name or f"cat-{category_id}",
        is_active=True,
        created_at=AT - timedelta(days=10),
        updated_at=AT if later else AT - timedelta(days=1),
    )


def order(order_id, total_vnd=100, subtotal_vnd=100, **changes):
    return dict(
        order_id=order_id,
        order_number=f"O{order_id}",
        status="completed",
        payment_method="cod",
        channel="online",
        subtotal_vnd=subtotal_vnd,
        discount_amount_vnd=0,
        shipping_fee_vnd=0,
        total_vnd=total_vnd,
        created_at=AT - timedelta(days=1),
        updated_at=AT - timedelta(days=1),
    ) | changes


def order_item(order_item_id, order_id):
    return dict(
        order_item_id=order_item_id,
        public_id=str(uuid.UUID(int=order_item_id)),
        order_id=order_id,
        variant_id=None,
        unit_price_vnd=50,
        cost_price_vnd=30,
        quantity=2,
        line_total_vnd=100,
        created_at=AT - timedelta(days=1),
    )


def movement(transaction_id, variant_id, quantity_delta, store_id=None, movement_type="inbound", created_at=AT):
    return dict(
        transaction_id=transaction_id,
        public_id=str(uuid.UUID(int=transaction_id)),
        variant_id=variant_id,
        location_type="store" if store_id else "central_warehouse",
        store_id=store_id,
        movement_type=movement_type,
        quantity_delta=quantity_delta,
        created_at=created_at,
    )
