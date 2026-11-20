CREATE TABLE IF NOT EXISTS lakehouse.silver.silver_customers (
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
    _silver_ingested_at             TIMESTAMP,
    _source_bronze_run_id           STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
