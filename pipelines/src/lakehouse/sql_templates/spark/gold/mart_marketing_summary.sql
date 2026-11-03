CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_marketing_summary (
    summary_key STRING,
    coupon_rows BIGINT,
    review_rows BIGINT,
    marketing_observed_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
