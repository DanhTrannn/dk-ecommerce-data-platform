CREATE TABLE IF NOT EXISTS lakehouse.silver.product_variants (
    variant_id                      BIGINT,
    public_id                       BINARY,
    product_id                      BIGINT,
    sku                             STRING,
    size_code                       STRING,
    color_code                      STRING,
    price_vnd                       BIGINT,
    cost_price_vnd                  BIGINT,
    is_active                       BOOLEAN,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
