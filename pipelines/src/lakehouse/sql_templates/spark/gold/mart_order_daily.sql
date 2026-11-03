CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_order_daily (
    order_date DATE,
    channel STRING,
    store_key BIGINT,
    store_name STRING,
    total_orders BIGINT,
    recognized_orders BIGINT,
    revenue_orders BIGINT,
    boom_orders BIGINT,
    return_requests_count BIGINT,
    order_total_vnd BIGINT,
    gross_revenue_vnd BIGINT,
    refunded_amount_vnd BIGINT,
    net_revenue_vnd BIGINT,
    cogs_vnd BIGINT,
    gross_profit_vnd BIGINT,
    cod_in_transit_vnd BIGINT,
    cod_shipments_count BIGINT,
    _gold_ingested_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
