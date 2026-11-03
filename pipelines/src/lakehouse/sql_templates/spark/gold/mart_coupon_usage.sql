CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_coupon_usage (
    coupon_id BIGINT,
    coupon_code STRING,
    used_count INT,
    usage_limit INT,
    is_available BOOLEAN,
    recent_redemptions_count BIGINT,
    marketing_observed_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
