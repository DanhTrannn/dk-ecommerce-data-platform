"""Publish log marts independently of the OLTP/CDC checkpoint."""

import json

from lakehouse.utils.sql import literal

PUBLICATIONS = "lakehouse.system.logs_analytics_publications"
SCHEMA_VERSION = 1


def publish_logs(spark, run_id, targets, *, observed_at, source_at, source_snapshots):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.system")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {PUBLICATIONS} (
        run_id STRING, published_at TIMESTAMP, source_event_at TIMESTAMP,
        snapshots_json STRING, schema_version INT, source_mode STRING,
        observed_at TIMESTAMP, source_snapshots_json STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')""")
    snapshots = {}
    for target in targets:
        refs = spark.sql(f"SELECT snapshot_id FROM {target}.refs WHERE name='main'").collect()
        snapshots[target] = refs[0].snapshot_id if refs else None
    source = f"TIMESTAMP {literal(str(source_at))}" if source_at else "CAST(NULL AS TIMESTAMP)"
    spark.sql(f"""MERGE INTO {PUBLICATIONS} t USING (
        SELECT {literal(run_id)} AS run_id, current_timestamp() AS published_at,
        {source} AS source_event_at, {literal(json.dumps(snapshots))} AS snapshots_json,
        {SCHEMA_VERSION} AS schema_version, 'logs_rollup' AS source_mode,
        TIMESTAMP {literal(str(observed_at))} AS observed_at,
        {literal(json.dumps(source_snapshots))} AS source_snapshots_json
        ) s ON t.run_id=s.run_id AND t.source_mode=s.source_mode
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
