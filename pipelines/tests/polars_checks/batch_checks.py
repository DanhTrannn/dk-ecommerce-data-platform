"""Native all-source acceptance actions; secrets and raw records remain inside the fixture."""

import json
import os
import sys
from contextlib import closing
from datetime import UTC, datetime

import pyarrow.parquet as pq

from batch_contracts import FAULTS, PREFIX, fingerprint
from lakehouse.oltp.polars.pipeline import run_batch
from lakehouse.oltp.polars.privacy import required_salt
from lakehouse.oltp.polars.writer import ingest_page, merge_silver
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader

catalog = iceberg_catalog()
mode = sys.argv[1]
if mode == "run":
    with closing(postgres_reader()) as connection:
        result = run_batch(
            catalog,
            connection,
            PREFIX,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=sys.argv[2],
            start=sys.argv[3] if len(sys.argv) > 3 else None,
            end=sys.argv[4] if len(sys.argv) > 4 else None,
        )
elif mode == "fingerprints":
    result = {}
    for table in SOURCE_TABLES:
        arrow = catalog.load_table((PREFIX + "_silver", table.silver_table)).scan().to_arrow()
        result[table.name] = fingerprint(arrow.to_pylist(), arrow.column_names)
        bronze = catalog.load_table((PREFIX + "_bronze", table.name)).scan().to_arrow()
        result["bronze." + table.name] = fingerprint(bronze.to_pylist(), bronze.column_names)
    arrow = catalog.load_table((PREFIX + "_quarantine", "silver_oltp_violations")).scan().to_arrow()
    result["quarantine"] = fingerprint(arrow.to_pylist(), arrow.column_names)
elif mode == "quarantine-diff":
    import hashlib

    left = {
        row["source_table"]: json.loads(row["record_data"])
        for row in catalog.load_table(("quarantine", "silver_oltp_violations")).scan().to_arrow().to_pylist()
    }
    right = {
        row["source_table"]: json.loads(row["record_data"])
        for row in catalog.load_table((PREFIX + "_quarantine", "silver_oltp_violations")).scan().to_arrow().to_pylist()
    }
    result = []
    for name in sorted(left.keys() | right.keys()):
        for field in sorted(left.get(name, {}).keys() | right.get(name, {}).keys()):
            a, b = left.get(name, {}).get(field), right.get(name, {}).get(field)
            if a != b:
                result.append(
                    {
                        "table": name,
                        "field": field,
                        "spark_type": type(a).__name__,
                        "native_type": type(b).__name__,
                        "spark_length": len(str(a)),
                        "native_length": len(str(b)),
                        "spark_sha256": hashlib.sha256(str(a).encode()).hexdigest(),
                        "native_sha256": hashlib.sha256(str(b).encode()).hexdigest(),
                    }
                )
elif mode == "faults":
    result = {}
    for table in SOURCE_TABLES:
        if table.name not in FAULTS:
            continue
        arrow = catalog.load_table((PREFIX + "_reference", "fault_" + table.name)).scan().to_arrow()
        result[table.name] = merge_silver(
            catalog, PREFIX, table, arrow, "native_faults", datetime.now(UTC), required_salt()
        )
elif mode == "backfill":
    table = next(table for table in SOURCE_TABLES if table.name == "orders")
    bronze = catalog.load_table((PREFIX + "_bronze", "orders"))
    path = (
        f"s3://{os.environ['MINIO_LAKEHOUSE_BUCKET']}/landing/polars-batch/{PREFIX}/initial/orders/page-000000.parquet"
    )
    with bronze.io.new_input(path).open() as stream:
        page = pq.read_table(stream)
    result = ingest_page(catalog, PREFIX, table, page, "old_backfill", datetime.now(UTC), required_salt())
elif mode in ("legacy", "restore"):
    import pyarrow as pa

    table = catalog.load_table((PREFIX + "_bronze", "categories"))
    rows = table.scan().to_arrow()
    values = ["legacy_mysql" if mode == "legacy" else "postgresql_ecommerce"] * len(rows)
    rows = rows.set_column(
        rows.schema.get_field_index("_source_system"), "_source_system", pa.array(values, type=pa.large_string())
    )
    table.overwrite(rows)
    result = {"source_guard_fault": mode}
else:
    raise ValueError("Unknown native batch check")
print("STACK_RESULT " + json.dumps(result), flush=True)
