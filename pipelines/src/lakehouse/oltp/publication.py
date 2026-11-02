"""Publish one validated set of OLTP snapshots for batch and CDC readers."""

import json

from lakehouse.oltp.gold_ddl import GOLD_TABLE_DDL
from lakehouse.sql import literal

SCHEMA_VERSION = 3
PUBLICATIONS = "lakehouse.system.analytics_publications"


def ensure_publications(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.system")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {PUBLICATIONS} (
        run_id STRING, published_at TIMESTAMP, source_event_at TIMESTAMP,
        snapshots_json STRING, offsets_json STRING, changed_tables_json STRING,
        schema_version INT, source_mode STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')""")
    existing = set(spark.table(PUBLICATIONS).columns)
    for name, kind in (("schema_version", "INT"), ("source_mode", "STRING")):
        if name not in existing:
            spark.sql(f"ALTER TABLE {PUBLICATIONS} ADD COLUMN {name} {kind}")


def publish(spark, cfg, run_id, *, mode, source_at=None, offsets=None, changed=()):
    ensure_publications(spark)
    # A controlled batch handoff must retain the last acknowledged CDC position.
    # Airflow does not advance transport offsets or erase the source checkpoint.
    if mode == "batch" and offsets is None:
        previous = spark.sql(
            f"SELECT offsets_json,source_event_at FROM {PUBLICATIONS} ORDER BY published_at DESC LIMIT 1"
        ).collect()
        if previous:
            offsets = json.loads(previous[0].offsets_json)
            if source_at is None:
                source_at = previous[0].source_event_at
    targets = {f"lakehouse.silver.{table.silver_table}" for table in cfg.tables}
    targets.update(f"lakehouse.gold.{name}" for name in GOLD_TABLE_DDL)
    snapshots = {}
    for target in sorted(targets):
        rows = spark.sql(f"SELECT snapshot_id FROM {target}.refs WHERE name='main'").collect()
        snapshots[target] = rows[0].snapshot_id if rows else None
    source = f"TIMESTAMP {literal(str(source_at))}" if source_at else "CAST(NULL AS TIMESTAMP)"
    spark.sql(f"""INSERT INTO {PUBLICATIONS}
        (run_id,published_at,source_event_at,snapshots_json,offsets_json,changed_tables_json,schema_version,source_mode)
        SELECT {literal(run_id)}, current_timestamp(), {source},
        {literal(json.dumps(snapshots))}, {literal(json.dumps(offsets or {}))},
        {literal(json.dumps(sorted(changed)))}, {SCHEMA_VERSION}, {literal(mode)}""")
