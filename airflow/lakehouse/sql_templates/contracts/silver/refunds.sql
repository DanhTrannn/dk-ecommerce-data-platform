CREATE TABLE IF NOT EXISTS lakehouse.silver.refunds (
    refund_id                       BIGINT,
    public_id                       BINARY,
    payment_id                      BIGINT,
    return_id                       BIGINT,
    refund_idempotency_key          STRING,
    status                          STRING,
    amount_vnd                      BIGINT,
    reason                          STRING,
    created_at                      TIMESTAMP,
    completed_at                    TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP,
    _run_id                         STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
