CREATE TABLE IF NOT EXISTS lakehouse.silver.product_reviews (
    review_id                       BIGINT,
    public_id                       BINARY,
    order_item_id                   BIGINT,
    customer_id                     BIGINT,
    product_id                      BIGINT,
    rating                          INT,
    content                         STRING,
    status                          STRING,
    moderation_reason               STRING,
    moderated_at                    TIMESTAMP,
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
