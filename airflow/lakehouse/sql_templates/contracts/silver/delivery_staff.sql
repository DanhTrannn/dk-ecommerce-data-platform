CREATE TABLE IF NOT EXISTS lakehouse.silver.delivery_staff (
    staff_id                        BIGINT,
    public_id                       BINARY,
    full_name                       STRING,
    vehicle_plate                   STRING,
    is_active                       BOOLEAN,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP,
    _run_id                         STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
