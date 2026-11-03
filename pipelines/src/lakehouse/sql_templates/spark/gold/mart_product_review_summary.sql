CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_product_review_summary (
    product_id BIGINT,
    product_name STRING,
    review_count BIGINT,
    recent_negative_count BIGINT,
    marketing_observed_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
