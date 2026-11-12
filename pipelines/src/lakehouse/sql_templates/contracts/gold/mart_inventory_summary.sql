CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_inventory_summary (
    summary_key STRING,
    inventory_rows BIGINT,
    total_inventory_value_vnd BIGINT,
    warehouse_stock_units BIGINT,
    store_stock_units BIGINT,
    stockout_count BIGINT,
    inventory_observed_at TIMESTAMP,
    inbound_batches_count BIGINT,
    _gold_ingested_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
