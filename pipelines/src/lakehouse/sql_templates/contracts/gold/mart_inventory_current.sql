CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_inventory_current (
    location_type STRING,
    location_id BIGINT,
    variant_id BIGINT,
    product_key BIGINT,
    sku STRING,
    product_name STRING,
    category_id BIGINT,
    category_name STRING,
    snapshot_date DATE,
    on_hand BIGINT,
    reserved_quantity BIGINT,
    unit_cost_vnd BIGINT,
    inventory_value_cost_vnd BIGINT,
    is_low_stock BOOLEAN,
    is_out_of_stock BOOLEAN,
    observed_at TIMESTAMP,
    _gold_ingested_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
