-- Initialize and Seed Lakehouse Gold Marts in Apache Iceberg via Polaris

CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

-- 1. mart_sales_daily
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_sales_daily (
    order_date                      DATE                            COMMENT 'Sales observation date',
    channel                         STRING                          COMMENT 'Channel (online or pos)',
    store_key                       BIGINT                          COMMENT 'Store key (0 if online)',
    category_id                     BIGINT                          COMMENT 'Product category ID',
    category_name                   STRING                          COMMENT 'Product category name',
    total_orders                    BIGINT                          COMMENT 'Total distinct orders placed',
    successful_orders               BIGINT                          COMMENT 'Distinct delivered/completed orders',
    boom_orders                     BIGINT                          COMMENT 'Distinct failed delivery (boom) orders',
    items_sold                      BIGINT                          COMMENT 'Total units delivered to customers',
    gross_revenue_vnd               BIGINT                          COMMENT 'Recognized merchandise after discounts and before refunds',
    cogs_vnd                        BIGINT                          COMMENT 'Cost of goods sold of delivered units in VND',
    gross_profit_vnd                BIGINT                          COMMENT 'Gross profit in VND (gross_revenue - cogs)',
    gross_profit_margin_pct         DOUBLE                          COMMENT 'Gross profit percentage',
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
);

-- 2. mart_logistics_performance
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_logistics_performance (
    shipment_date                   DATE,
    staff_key                       BIGINT,
    staff_code                      STRING,
    assigned_city_id                BIGINT,
    assigned_city_name              STRING,
    total_shipments                 BIGINT,
    delivered_count                 BIGINT,
    boom_count                      BIGINT,
    boom_rate_pct                   DOUBLE,
    on_time_count                   BIGINT,
    on_time_rate_pct                DOUBLE,
    avg_duration_minutes            DOUBLE,
    total_cod_collected_vnd         BIGINT,
    _gold_ingested_at               TIMESTAMP,
    _source_run_id                  STRING
)
USING iceberg
PARTITIONED BY (shipment_date);

-- 3. mart_inventory_health
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_inventory_health (
    snapshot_date                   DATE,
    location_type                   STRING,
    location_id                     BIGINT,
    category_id                     BIGINT,
    category_name                   STRING,
    total_variants                  BIGINT,
    out_of_stock_count              BIGINT,
    low_stock_count                 BIGINT,
    total_on_hand_units             BIGINT,
    total_inventory_value_vnd       BIGINT,
    _gold_ingested_at               TIMESTAMP,
    _source_run_id                  STRING
)
USING iceberg
PARTITIONED BY (snapshot_date);

-- 4. mart_product_returns
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_product_returns (
    return_date                     DATE,
    returned_variant_key            BIGINT,
    product_id                      BIGINT,
    product_name                    STRING,
    sku                             STRING,
    size                            STRING,
    color                           STRING,
    action_type                     STRING,
    reason                          STRING,
    total_return_requests           BIGINT,
    total_returned_quantity         BIGINT,
    total_refund_amount_vnd         BIGINT,
    _gold_ingested_at               TIMESTAMP,
    _source_run_id                  STRING
)
USING iceberg
PARTITIONED BY (return_date);

-- 5. dim_product
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_product (
    product_key                     BIGINT                          COMMENT 'Product key (product_id)',
    product_id                      BIGINT                          COMMENT 'Original OLTP product id',
    product_name                    STRING                          COMMENT 'Product display name',
    slug                            STRING                          COMMENT 'URL friendly slug',
    category_id                     BIGINT                          COMMENT 'Category id',
    category_code                   STRING,
    category_name                   STRING                          COMMENT 'Category display name',
    base_price_vnd                  BIGINT                          COMMENT 'Base listed price in VND',
    is_active                       BOOLEAN                         COMMENT 'Product active status',
    created_at                      TIMESTAMP                       COMMENT 'Product creation timestamp',
    _gold_ingested_at               TIMESTAMP                       COMMENT 'UTC timestamp written to Gold',
    _source_run_id                  STRING                          COMMENT 'Batch run ID'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- 6. fact_order_item
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
    item_total_vnd                  BIGINT                          COMMENT 'Line revenue (quantity * unit_price) in VND',
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
);

-- 7. mart_marketing_funnel_daily
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_marketing_funnel_daily (
    metric_date                     DATE,
    product_views                   BIGINT,
    cart_additions                  BIGINT,
    checkout_initiations            BIGINT,
    orders_completed                BIGINT,
    view_to_cart_rate_pct           DOUBLE,
    cart_to_checkout_rate_pct       DOUBLE,
    checkout_to_order_rate_pct      DOUBLE,
    overall_conversion_rate_pct     DOUBLE,
    unique_visitors                 BIGINT,
    _gold_ingested_at               TIMESTAMP,
    _source_run_id                  STRING
)
USING iceberg
PARTITIONED BY (metric_date);


-- Current inventory state, separate from daily history.
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_inventory_current (
    snapshot_date_key               INT                             COMMENT 'Date key YYYYMMDD',
    snapshot_date                   DATE                            COMMENT 'Snapshot observation date',
    location_type                   STRING                          COMMENT 'Location classification (warehouse or store)',
    location_id                     BIGINT                          COMMENT 'Warehouse (0) or store_id',
    variant_key                     BIGINT                          COMMENT 'Foreign key to dim_variant',
    product_key                     BIGINT                          COMMENT 'Foreign key to dim_product',
    on_hand_quantity                INT                             COMMENT 'Units physically in stock',
    reserved_quantity               INT                             COMMENT 'Units reserved for pending orders',
    unit_cost_vnd                   BIGINT                          COMMENT 'COGS unit price in VND',
    inventory_value_cost_vnd        BIGINT                          COMMENT 'Stock valuation at cost (on_hand * unit_cost)',
    is_low_stock                    BOOLEAN                         COMMENT 'True if on_hand <= 5 units',
    is_out_of_stock                 BOOLEAN                         COMMENT 'True if on_hand == 0',
    _gold_ingested_at               TIMESTAMP                       COMMENT 'UTC timestamp written to Gold',
    _source_run_id                  STRING                          COMMENT 'Batch run ID'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'history.expire.max-snapshot-age-ms' = '259200000',
    'history.expire.min-snapshots-to-keep' = '10'
);
