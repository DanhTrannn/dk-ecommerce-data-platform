CREATE TABLE IF NOT EXISTS lakehouse.silver.orders (
    order_id                        BIGINT,
    order_number                    STRING,
    cart_id                         BIGINT,
    customer_id                     BIGINT,
    checkout_idempotency_key        STRING,
    coupon_id                       BIGINT,
    status                          STRING,
    payment_method                  STRING,
    subtotal_vnd                    BIGINT,
    coupon_code_snapshot            STRING,
    coupon_type_snapshot            STRING,
    coupon_value_snapshot           BIGINT,
    discount_amount_vnd             BIGINT,
    shipping_fee_vnd                BIGINT,
    total_vnd                       BIGINT,
    shipping_city_id                BIGINT,
    data_origin                     STRING,
    generation_run_id               STRING,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP,
    paid_at                         TIMESTAMP,
    confirmed_at                    TIMESTAMP,
    completed_at                    TIMESTAMP,
    cancelled_at                    TIMESTAMP,
    store_id                        BIGINT,
    channel                         STRING,
    staff_id                        BIGINT,
    _ingested_at                    TIMESTAMP,
    _run_id                         STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
