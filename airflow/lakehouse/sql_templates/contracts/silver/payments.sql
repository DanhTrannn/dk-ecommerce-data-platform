CREATE TABLE IF NOT EXISTS lakehouse.silver.payments (
    payment_id                      BIGINT,
    payment_reference               STRING,
    order_id                        BIGINT,
    payment_idempotency_key         STRING,
    status                          STRING,
    amount_vnd                      BIGINT,
    failure_code                    STRING,
    attempted_at                    TIMESTAMP,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
