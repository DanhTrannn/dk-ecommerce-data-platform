CREATE TABLE IF NOT EXISTS lakehouse.system.oltp_hard_deletes (
    table_name STRING,
    primary_key STRING,
    event_id STRING,
    source_updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    requested_run_id STRING,
    recorded_at TIMESTAMP,
    applied_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version' = '2')
