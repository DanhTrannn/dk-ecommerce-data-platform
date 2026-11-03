CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_product_sales_daily (
    order_date DATE,
    product_id BIGINT,
    product_name STRING,
    units_sold BIGINT,
    gross_revenue_vnd BIGINT,
    refunded_amount_vnd BIGINT,
    net_revenue_vnd BIGINT,
    cogs_vnd BIGINT,
    gross_profit_vnd BIGINT,
    _gold_ingested_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
