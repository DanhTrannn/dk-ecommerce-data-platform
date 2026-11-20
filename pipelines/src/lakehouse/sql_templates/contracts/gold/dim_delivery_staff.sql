CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_delivery_staff (
    staff_key                       BIGINT                          COMMENT 'Surrogate key',
    staff_id                        BIGINT                          COMMENT 'Original staff id',
    staff_code                      STRING                          COMMENT 'Business code (DK-SHIP-...)',
    full_name                       STRING                          COMMENT 'Delivery staff full name',
    assigned_city_id                BIGINT                          COMMENT 'Delivery operational zone (city id)',
    assigned_city_name              STRING                          COMMENT 'Delivery operational zone name',
    vehicle_type                    STRING                          COMMENT 'Vehicle type/plate info',
    is_active                       BOOLEAN                         COMMENT 'Active employment status',
    _gold_ingested_at               TIMESTAMP                       COMMENT 'UTC timestamp written to Gold',
    _source_run_id                  STRING                          COMMENT 'Batch run ID'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
