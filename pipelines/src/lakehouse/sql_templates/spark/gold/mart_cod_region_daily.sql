CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_cod_region_daily (
    shipment_date DATE,
    region STRING,
    total_cod_shipments BIGINT,
    failed_cod_shipments BIGINT,
    _gold_ingested_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
PARTITIONED BY (shipment_date)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
