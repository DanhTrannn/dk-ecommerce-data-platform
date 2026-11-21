CREATE TABLE IF NOT EXISTS lakehouse.silver.shipments (
    shipment_id                     BIGINT,
    public_id                       BINARY,
    shipment_code                   STRING,
    order_id                        BIGINT,
    delivery_staff_id               BIGINT,
    status                          STRING,
    attempt_count                   INT,
    cod_amount_vnd                  BIGINT,
    cod_collected_vnd               BIGINT,
    dispatched_at                   TIMESTAMP,
    delivered_at                    TIMESTAMP,
    failed_at                       TIMESTAMP,
    failure_reason                  STRING,
    notes                           STRING,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
