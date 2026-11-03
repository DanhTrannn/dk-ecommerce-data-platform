CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_operations_summary (
    summary_key STRING,
    order_rows BIGINT,
    pending_orders BIGINT,
    over_two_hours_orders BIGINT,
    average_waiting_hours DOUBLE,
    shipment_rows BIGINT,
    problematic_shipments BIGINT,
    failed_shipments BIGINT,
    return_rows BIGINT,
    return_requests_count BIGINT,
    refund_rows BIGINT,
    successful_refunds_count BIGINT,
    successful_refund_amount_vnd BIGINT,
    operations_observed_at TIMESTAMP,
    _gold_ingested_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
