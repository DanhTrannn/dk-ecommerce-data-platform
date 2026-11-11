"""Native log acceptance actions; output counts/hashes/manifests, never raw events."""

import json
import os
import sys
from contextlib import closing
from datetime import datetime, timedelta

import polars as pl

from batch_contracts import PREFIX
from gold_contracts import gold_fingerprint
from lakehouse.logs.polars.marts import TABLES
from lakehouse.logs.polars.pipeline import run_marts
from lakehouse.oltp.polars.gold.storage import capture, read_snapshot
from lakehouse.oltp.polars.schema import align_arrow
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader

catalog = iceberg_catalog()
mode = sys.argv[1]
if mode in {"build", "verify"}:
    with closing(postgres_reader()) as connection:
        result = run_marts(
            catalog,
            connection,
            PREFIX,
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=sys.argv[2],
            as_of=sys.argv[3],
            verify_only=mode == "verify",
        )
elif mode == "fingerprints":
    result = {}
    for name in TABLES:
        table = catalog.load_table((PREFIX + "_gold", name))
        frame = read_snapshot(table, capture(table))
        result[name] = gold_fingerprint(frame.to_dicts(), frame.columns)
elif mode in {"publication", "oltp-publication"}:
    name = "logs_analytics_publications" if mode == "publication" else "analytics_publications"
    table = catalog.load_table((PREFIX + "_system", name))
    rows = read_snapshot(table, capture(table)).to_dicts()
    result = dict(count=len(rows), latest=max(rows, key=lambda row: row["published_at"]))
elif mode == "corrupt":
    table = catalog.load_table((PREFIX + "_gold", TABLES[0]))
    frame = read_snapshot(table, capture(table)).with_columns((pl.col("total_requests") + 1).alias("total_requests"))
    table.overwrite(align_arrow(frame.to_arrow(), table.schema()))
    result = {"corrupted": True}
elif mode == "audit":
    table = catalog.load_table((PREFIX + "_system", "logs_quality_results"))
    frame = read_snapshot(table, capture(table)).filter(pl.col("status") == "FAILED")
    result = {"failed_checks": frame["check_name"].to_list()}
elif mode == "catalog-unpublished":
    table = catalog.load_table((PREFIX + "_silver", "silver_products"))
    frame = read_snapshot(table, capture(table)).with_columns(pl.lit("unpublished-name-fixture").alias("name"))
    table.overwrite(align_arrow(frame.to_arrow(), table.schema()))
    result = {"unpublished_catalog_change": True}
elif mode in {"duplicate", "restore", "late", "future"}:
    table = catalog.load_table(("gold", "fact_web_events"))
    snapshot = capture(table)
    original = read_snapshot(table, int(sys.argv[2])) if mode == "restore" else read_snapshot(table, snapshot)
    if mode == "restore":
        table.overwrite(align_arrow(original.to_arrow(), table.schema()))
    else:
        frame = original.head(1)
        if mode in {"late", "future"}:
            observed = datetime.fromisoformat(sys.argv[2]) + timedelta(days=1 if mode == "future" else -1)
            frame = frame.with_columns(
                pl.lit(("e" if mode == "future" else "f") * 32).alias("event_id"),
                pl.lit(observed).alias("event_ts"),
                pl.lit(observed.date()).alias("event_date"),
            )
        table.append(align_arrow(frame.to_arrow(), table.schema()))
    result = {"previous_snapshot": snapshot}
else:
    raise ValueError("Unknown log acceptance mode")
print("STACK_RESULT " + json.dumps(result, default=str), flush=True)
