"""Native/dbt acceptance operations. Never print raw rows, payloads or credentials."""

import json
import os
import sys
from contextlib import closing
from datetime import UTC, datetime

from lakehouse.analytics.commerce import build_commerce
from lakehouse.logs.pipeline import run_logs
from lakehouse.oltp.polars.pipeline import run_batch
from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader

catalog = iceberg_catalog()
mode = sys.argv[1]
run_id = sys.argv[2] if len(sys.argv) > 2 else "native_initial"
as_of = sys.argv[3] if len(sys.argv) > 3 else datetime.now(UTC).isoformat()
with closing(postgres_reader()) as connection:
    if mode in {"initial", "batch"}:
        bounds = dict(start=sys.argv[3], end=sys.argv[4]) if mode == "batch" else {}
        result = run_batch(
            catalog,
            connection,
            "serving",
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=run_id,
            page_size=10000,
            **bounds,
        )
    elif mode in {"commerce", "verify"}:
        result = build_commerce(
            catalog,
            connection,
            "serving",
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=run_id,
            as_of=as_of,
            verify_only=mode == "verify",
        )
    elif mode == "logs":
        result = run_logs(
            catalog, connection, "serving", bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"], run_id=run_id, as_of=as_of
        )
    elif mode == "publication":
        table = catalog.load_table(("system", "analytics_publications"))
        rows = read_snapshot(table, capture(table)).to_dicts()
        result = dict(count=len(rows), latest=max(rows, key=lambda row: row["published_at"]))
    elif mode == "oracle":
        from lakehouse.oltp.quality.publication import pinned_inputs
        from oracles.gold.builders import TABLES, build_all
        from fingerprints import fingerprint

        observed = datetime.fromisoformat(as_of)
        silver, _ = pinned_inputs(catalog, "serving")
        target = catalog.load_table(("gold", "fact_inventory_daily_snapshot"))
        expected = build_all(silver, run_id, observed, inventory_history=read_snapshot(target, capture(target)))
        result = {}
        for name in TABLES:
            table = catalog.load_table(("gold", name))
            actual = read_snapshot(table, capture(table))
            a, b = fingerprint(expected[name]), fingerprint(actual)
            result[name] = {"expected": a, "actual": b}
    elif mode == "reconcile":
        from lakehouse.oltp.quality.reconcile import run_reconciliation_gate

        result = run_reconciliation_gate(
            catalog,
            connection,
            prefix="serving",
            tolerance_pct=0,
            run_id=run_id,
            batch_date=datetime.now(UTC).date().isoformat(),
        )
    elif mode == "corrupt":
        import polars as pl
        from lakehouse.oltp.polars.schema import align_arrow

        table = catalog.load_table(("gold", "mart_order_daily"))
        frame = read_snapshot(table, capture(table)).with_columns(
            (pl.col("order_total_vnd") + 1).alias("order_total_vnd")
        )
        table.overwrite(align_arrow(frame.to_arrow(), table.schema()))
        result = {"corrupted": True}
    elif mode == "maintenance":
        from maintenance import exercise

        result = exercise(catalog, connection)
    elif mode == "hard-delete":
        from lakehouse.oltp.hard_delete import sync_hard_deletes

        result = sync_hard_deletes(catalog, connection, datetime.fromisoformat(as_of))
    elif mode == "runtime":
        import importlib.util
        import shutil

        result = {"pyspark": bool(importlib.util.find_spec("pyspark")), "java": bool(shutil.which("java"))}
    else:
        raise ValueError("Unknown native acceptance mode")
print("STACK_RESULT " + json.dumps(result, default=str), flush=True)
