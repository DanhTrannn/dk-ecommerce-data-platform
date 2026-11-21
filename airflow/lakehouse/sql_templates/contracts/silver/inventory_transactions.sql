CREATE TABLE IF NOT EXISTS lakehouse.silver.inventory_transactions (
    transaction_id                  BIGINT,
    public_id                       BINARY,
    variant_id                      BIGINT,
    location_type                   STRING,
    store_id                        BIGINT,
    movement_type                   STRING,
    quantity_delta                  INT,
    order_id                        BIGINT,
    receipt_id                      BIGINT,
    return_id                       BIGINT,
    reference_code                  STRING,
    notes                           STRING,
    created_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
