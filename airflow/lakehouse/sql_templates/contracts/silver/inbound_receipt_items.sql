CREATE TABLE IF NOT EXISTS lakehouse.silver.inbound_receipt_items (
    item_id                         BIGINT,
    public_id                       BINARY,
    receipt_id                      BIGINT,
    variant_id                      BIGINT,
    quantity                        INT,
    unit_cost_vnd                   BIGINT,
    total_cost_vnd                  BIGINT,
    previous_cost_price_vnd         BIGINT,
    new_cost_price_vnd              BIGINT,
    created_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP,
    _run_id                         STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
