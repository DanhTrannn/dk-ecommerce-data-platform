CREATE TABLE IF NOT EXISTS lakehouse.silver.customers (
    customer_id                     BIGINT,
    public_id                       BINARY,
    role                            STRING,
    display_name                    STRING,
    status                          STRING,
    city_id                         BIGINT,
    store_id                        BIGINT,
    is_cod_blocked                  BOOLEAN,
    boom_count                      INT,
    data_origin                     STRING,
    generation_run_id               STRING,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
