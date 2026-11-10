"""Run actual pilot/commit operations inside the optional Python image."""

import json
from importlib.metadata import version
import os
import sys
from contextlib import closing
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import DayTransform

from lakehouse.oltp.polars_pilot import run_pilot
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader

NAMESPACE = "polars_pilot_acceptance"
catalog = iceberg_catalog()
mode = sys.argv[1]
if mode == "run":
    with closing(postgres_reader()) as connection:
        result = run_pilot(
            catalog,
            connection,
            NAMESPACE,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=sys.argv[2],
            start=sys.argv[3] if len(sys.argv) > 3 else None,
            end=sys.argv[4] if len(sys.argv) > 4 else None,
            page_size=2,
        )
elif mode == "source-denial":
    import psycopg2

    with closing(postgres_reader()) as connection:
        for statement, expected in (
            ("UPDATE categories SET name=name WHERE category_id=-1", psycopg2.errors.ReadOnlySqlTransaction),
            ("SELECT * FROM customer_credentials LIMIT 1", psycopg2.errors.InsufficientPrivilege),
        ):
            try:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
            except expected:
                connection.rollback()
            else:
                raise AssertionError("Reader unexpectedly authorized a forbidden source operation")
    result = {"source_write_denied": True, "credential_table_denied": True}
elif mode == "operations":
    spark = catalog.load_table((NAMESPACE, "spark_probe"))
    row = spark.scan().to_arrow().to_pylist()
    assert len(row) == 1 and row[0]["amount"] == Decimal("12.34") and row[0]["note"] is None
    assert row[0]["public_id"] == bytes.fromhex("00112233445566778899aabbccddeeff")
    assert row[0]["event_at"] == datetime(2026, 1, 1, 1, 2, 3, 123456, tzinfo=UTC)
    schema = spark.schema()
    partition = PartitionSpec(
        PartitionField(
            source_id=schema.find_field("event_at").field_id, field_id=1000, transform=DayTransform(), name="event_day"
        )
    )
    table = catalog.create_table(
        (NAMESPACE, "python_probe"), schema=schema, partition_spec=partition, properties={"format-version": "2"}
    )
    arrow = spark.scan().to_arrow()
    table.append(arrow)
    original = table.current_snapshot().snapshot_id
    row = row[0]
    row.update(id=2, amount=Decimal("20.00"), note="Tiếng Việt", active=False)
    table.append(pa.Table.from_pylist([row], schema=arrow.schema))
    row["amount"] = Decimal("21.00")
    table.upsert(pa.Table.from_pylist([row], schema=arrow.schema), join_cols=["id"])
    assert sorted(r["amount"] for r in table.scan().to_arrow().to_pylist()) == [Decimal("12.34"), Decimal("21.00")]
    table.delete(EqualTo("id", 1))
    assert len(table.scan().to_arrow()) == 1
    row.update(id=9, amount=Decimal("99.50"))
    table.overwrite(pa.Table.from_pylist([row], schema=arrow.schema))
    assert len(table.scan().to_arrow()) == 1
    assert table.scan(snapshot_id=original).to_arrow().to_pylist()[0]["id"] == 1
    assert table.schema() == schema
    result = {
        "spark_position_deletes_read": True,
        "append_upsert_delete_overwrite": True,
        "schema_field_ids_preserved": True,
        "original_snapshot": original,
        "current_snapshot": table.current_snapshot().snapshot_id,
        "maintenance_methods": [
            n for n in dir(table.maintenance) if not n.startswith("_") and callable(getattr(table.maintenance, n))
        ],
        "versions": {name: version(name) for name in ("polars", "pyiceberg", "pyarrow", "pyiceberg-core")},
    }
elif mode == "flink":
    table = catalog.load_table(("landing", "access_logs"))
    rows = table.scan().to_arrow().to_pylist()
    assert any(row["http_route"] == "/api/v1/products" for row in rows), "No actual API product event in Flink table"
    result = {"flink_landing_rows": len(rows), "flink_table_read": True}
else:
    raise ValueError("Unknown check mode")
print("STACK_RESULT " + json.dumps(result), flush=True)
