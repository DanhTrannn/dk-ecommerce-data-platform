CREATE TABLE IF NOT EXISTS lakehouse.silver.order_items (
    order_item_id                   BIGINT,
    public_id                       BINARY,
    order_id                        BIGINT,
    variant_id                      BIGINT,
    product_public_id_snapshot      BINARY,
    category_code_snapshot          STRING,
    category_name_snapshot          STRING,
    product_name_snapshot           STRING,
    sku_snapshot                    STRING,
    size_code_snapshot              STRING,
    color_code_snapshot             STRING,
    unit_price_vnd                  BIGINT,
    cost_price_vnd                  BIGINT,
    quantity                        INT,
    line_total_vnd                  BIGINT,
    created_at                      TIMESTAMP,
    _ingested_at                    TIMESTAMP,
    _run_id                         STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
