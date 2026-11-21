CREATE TABLE IF NOT EXISTS lakehouse.silver.return_items (
    return_item_id                  BIGINT,
    public_id                       BINARY,
    return_id                       BIGINT,
    order_item_id                   BIGINT,
    variant_id                      BIGINT,
    quantity                        INT,
    exchange_variant_id             BIGINT,
    refund_amount_vnd               BIGINT,
    inspection_status               STRING,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
