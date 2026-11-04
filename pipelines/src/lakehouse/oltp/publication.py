"""Publish one validated set of OLTP snapshots for batch analytics readers."""

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
    targets = {f"lakehouse.silver.{table.silver_table}" for table in cfg.tables}
    targets.update(f"lakehouse.gold.{name}" for name in GOLD_TABLE_DDL)
    snapshots = {}
    for target in sorted(targets):
        rows = spark.sql(f"SELECT snapshot_id FROM {target}.refs WHERE name='main'").collect()
        snapshots[target] = rows[0].snapshot_id if rows else None
    source = f"TIMESTAMP {literal(str(source_at))}" if source_at else "CAST(NULL AS TIMESTAMP)"
    spark.sql(f"""MERGE INTO {PUBLICATIONS} t USING (
        SELECT {literal(run_id)} AS run_id, current_timestamp() AS published_at, {source} AS source_event_at,
        {literal(json.dumps(snapshots))} AS snapshots_json, {literal(json.dumps(offsets or {}))} AS offsets_json,
        {literal(json.dumps(sorted(changed)))} AS changed_tables_json,
        {SCHEMA_VERSION} AS schema_version, {literal(mode)} AS source_mode
        ) s ON t.run_id = s.run_id AND t.source_mode = s.source_mode
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
