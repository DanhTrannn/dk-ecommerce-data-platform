CREATE TABLE IF NOT EXISTS lakehouse.silver.categories (
    category_id                     BIGINT,
    public_id                       BINARY,
    parent_category_id              BIGINT,
    code                            STRING,
    name                            STRING,
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
