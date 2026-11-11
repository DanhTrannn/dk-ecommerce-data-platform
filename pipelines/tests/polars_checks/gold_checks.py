"""Native Gold actions and version-pinned Trino verification on private resources."""

import json
import os
import sys
from contextlib import closing
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

from batch_contracts import PREFIX
from gold_contracts import gold_fingerprint
from lakehouse.oltp.polars.gold.builders import TABLES
from lakehouse.oltp.polars.gold.pipeline import run_gold
from lakehouse.oltp.polars.gold.storage import capture, read_snapshot
from lakehouse.oltp.polars.schema import align_arrow
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader

catalog = iceberg_catalog()
mode = sys.argv[1]
if mode in ("build", "verify"):
    with closing(postgres_reader()) as connection:
        result = run_gold(
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
        arrow = read_snapshot(table, capture(table)).to_arrow()
        result[name] = gold_fingerprint(arrow.to_pylist(), arrow.column_names)
elif mode == "publication":
    table = catalog.load_table((PREFIX + "_system", "analytics_publications"))
    rows = table.scan().to_arrow().to_pylist()
    latest = max(rows, key=lambda row: row["published_at"])
    result = {"count": len(rows), "latest": latest}
elif mode == "corrupt":
    table = catalog.load_table((PREFIX + "_gold", "mart_order_daily"))
    rows = read_snapshot(table, capture(table)).with_columns((pl.col("order_total_vnd") + 1).alias("order_total_vnd"))
    table.overwrite(align_arrow(rows.to_arrow(), table.schema()))
    result = {"placed_value_corrupted": True}
elif mode == "audit":
    table = catalog.load_table((PREFIX + "_system", "data_quality_results"))
    rows = table.scan().to_arrow().to_pylist()
    result = {"rows": len(rows), "failed_checks": [row["check_name"] for row in rows if row["status"] == "FAILED"]}
elif mode == "history":
    # Known previous-day balances test partition retention; this is fixture data, not reconstructed history.
    previous = datetime.fromisoformat(sys.argv[2]).astimezone(ZoneInfo("Asia/Ho_Chi_Minh")) - timedelta(days=1)
    for namespace in ("gold", PREFIX + "_gold"):
        table = catalog.load_table((namespace, "fact_inventory_daily_snapshot"))
        rows = read_snapshot(table, capture(table)).with_columns(
            pl.lit(previous.date()).alias("snapshot_date"),
            pl.lit(int(previous.strftime("%Y%m%d"))).alias("snapshot_date_key"),
            pl.lit(previous).alias("_gold_ingested_at"),
            pl.lit("fixture_history").alias("_source_run_id"),
        )
        table.append(align_arrow(rows.to_arrow(), table.schema()))
    result = {"previous_snapshot_date": previous.date().isoformat()}
else:
    raise ValueError("Unknown native Gold check")
print("STACK_RESULT " + json.dumps(result, default=str), flush=True)
