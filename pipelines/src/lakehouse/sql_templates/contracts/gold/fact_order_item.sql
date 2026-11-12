CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_order_item (
    order_item_id                   BIGINT                          COMMENT 'Order item ID',
    order_id                        BIGINT                          COMMENT 'Foreign key to order',
    order_date_key                  INT                             COMMENT 'Date key YYYYMMDD',
    order_date                      DATE                            COMMENT 'Order placement date',
    customer_key                    BIGINT                          COMMENT 'Foreign key to dim_customer',
    product_key                     BIGINT                          COMMENT 'Foreign key to dim_product',
    variant_key                     BIGINT                          COMMENT 'Foreign key to dim_variant',
    store_key                       BIGINT                          COMMENT 'Foreign key to dim_store (0 if online)',
    channel                         STRING                          COMMENT 'Sales channel (online or pos)',
    order_status                    STRING                          COMMENT 'Order status at snapshot',
    quantity                        INT                             COMMENT 'Quantity sold',
    unit_price_vnd                  BIGINT                          COMMENT 'Sale unit price in VND',
    unit_cost_vnd                   BIGINT                          COMMENT 'Unit cost price (COGS) in VND',
    item_total_vnd                  BIGINT                          COMMENT 'Original line value before discount and recognition',
    item_cost_vnd                   BIGINT                          COMMENT 'Recognized cost after completed accepted returns',
    item_profit_vnd                 BIGINT                          COMMENT 'Net merchandise revenue less recognized cost',
    category_code_snapshot          STRING,
    category_name_snapshot          STRING,
    allocated_discount_vnd          BIGINT,
    returned_quantity               INT,
    recognized_quantity             INT,
    gross_revenue_vnd               BIGINT,
    refunded_amount_vnd             BIGINT,
    net_revenue_vnd                 BIGINT,
    _gold_ingested_at               TIMESTAMP                       COMMENT 'UTC timestamp written to Gold',
    _source_run_id                  STRING                          COMMENT 'Batch run ID'
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'history.expire.max-snapshot-age-ms' = '259200000',
    'history.expire.min-snapshots-to-keep' = '10'
)
