"""Create native/dbt publications and reversible faults in the private BI fixture."""

import json
import os
import sys
from contextlib import closing
from datetime import UTC, datetime

import polars as pl
from lakehouse.analytics.commerce import build_commerce
from lakehouse.oltp.polars.pipeline import run_batch
from lakehouse.oltp.polars.schema import align_arrow
from lakehouse.oltp.quality.reconcile import run_reconciliation_gate
from lakehouse.utils.iceberg import iceberg_catalog
from lakehouse.utils.postgres import postgres_reader
from lakehouse.utils.snapshots import capture, read_snapshot

catalog = iceberg_catalog()
mode = sys.argv[1]
if mode in {"bootstrap", "restore"}:
    run_id = "bi-initial" if mode == "bootstrap" else "bi-next"
    observed = datetime.now(UTC)
    with closing(postgres_reader()) as connection:
        run_batch(catalog, connection, "serving", bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"], run_id=run_id)
        reconciliation = run_reconciliation_gate(
            catalog,
            connection,
            prefix="serving",
            run_id=run_id,
            batch_date=observed.date().isoformat(),
            tolerance_pct=0,
        )
        quality = build_commerce(
            catalog,
            connection,
            "serving",
            bucket=os.environ["MINIO_LAKEHOUSE_BUCKET"],
            run_id=run_id,
            as_of=observed.isoformat(),
        )
    result = {
        "reconciliation_checks": reconciliation["passed_checks"],
        "gold_checks": quality["quality"]["passed_checks"],
    }
elif mode == "corrupt":
    table = catalog.load_table(("gold", "mart_order_daily"))
    frame = read_snapshot(table, capture(table)).with_columns(
        (pl.col("net_revenue_vnd") + 1000000).alias("net_revenue_vnd")
    )
    table.overwrite(align_arrow(frame.to_arrow(), table.schema()))
    result = {"unpublished_mart_mutation": True}
else:
    raise ValueError(mode)
print("STACK_RESULT " + json.dumps(result), flush=True)
