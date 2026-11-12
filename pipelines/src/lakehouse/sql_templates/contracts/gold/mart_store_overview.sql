CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_store_overview (
    store_key BIGINT,
    store_id BIGINT,
    store_code STRING,
    store_name STRING,
    is_active BOOLEAN,
    total_orders BIGINT,
    completed_orders BIGINT,
    total_revenue_vnd BIGINT,
    active_staff BIGINT,
    low_stock_count BIGINT,
    stockout_count BIGINT,
    inventory_observed_at TIMESTAMP,
    _gold_ingested_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
