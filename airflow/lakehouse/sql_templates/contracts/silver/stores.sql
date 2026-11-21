CREATE TABLE IF NOT EXISTS lakehouse.silver.stores (
    store_id                        BIGINT,
    city_id                         BIGINT,
    code                            STRING,
    name                            STRING,
    address                         STRING,
    phone                           STRING,
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
