CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_admin_overview (
    summary_key STRING,
    product_rows BIGINT,
    variant_rows BIGINT,
    inventory_rows BIGINT,
    customer_rows BIGINT,
    review_rows BIGINT,
    coupon_rows BIGINT,
    order_rows BIGINT,
    active_products BIGINT,
    active_variants BIGINT,
    low_stock_variants BIGINT,
    customers BIGINT,
    total_reviews BIGINT,
    active_coupons BIGINT,
    pending_payment_orders BIGINT,
    paid_orders BIGINT,
    confirmed_orders BIGINT,
    shipping_orders BIGINT,
    delivered_orders BIGINT,
    completed_orders BIGINT,
    cancelled_orders BIGINT,
    failed_delivery_orders BIGINT,
    returned_orders BIGINT,
    admin_observed_at TIMESTAMP,
    _source_run_id STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
