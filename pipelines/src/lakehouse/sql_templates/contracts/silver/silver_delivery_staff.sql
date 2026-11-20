CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_delivery_staff (
    staff_id                        BIGINT,
    public_id                       BINARY,
    full_name                       STRING,
    vehicle_plate                   STRING,
    is_active                       BOOLEAN,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _silver_ingested_at             TIMESTAMP,
    _source_bronze_run_id           STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
