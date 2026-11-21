CREATE TABLE IF NOT EXISTS lakehouse.silver.coupons (
    coupon_id                       BIGINT,
    public_id                       BINARY,
    code_normalized                 STRING,
    discount_type                   STRING,
    discount_value                  BIGINT,
    minimum_subtotal_vnd            BIGINT,
    starts_at                       TIMESTAMP,
    ends_at                         TIMESTAMP,
    is_active                       BOOLEAN,
    total_usage_limit               INT,
    per_customer_usage_limit        INT,
    used_count                      INT,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
