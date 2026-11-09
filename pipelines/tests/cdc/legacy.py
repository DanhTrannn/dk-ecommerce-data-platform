"""Create the pre-PostgreSQL audit contract in the disposable catalog only."""

from lakehouse.utils.spark import spark_session

spark = spark_session("cdc_legacy_fixture")
try:
    spark.sql("""CREATE TABLE lakehouse.bronze.oltp_cdc_events (
        event_id STRING, topic STRING, database_name STRING, table_name STRING,
        operation STRING, event_ts TIMESTAMP, source_ts TIMESTAMP, transaction_id STRING,
        mysql_server_id STRING, binlog_file STRING, binlog_position BIGINT, binlog_row INT,
        kafka_partition INT, kafka_offset BIGINT, record_key STRING,
        before_payload STRING, after_payload STRING, raw_payload STRING,
        ingested_at TIMESTAMP, event_date DATE
    ) USING iceberg PARTITIONED BY(event_date) TBLPROPERTIES ('format-version'='2')""")
    spark.sql("""INSERT INTO lakehouse.bronze.oltp_cdc_events SELECT
        'legacy:0:42', 'legacy.ecommerce.customers', 'ecommerce', 'customers', 'c',
        TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-01-01 00:00:00', CAST(NULL AS STRING),
        '17', 'mysql-bin.000001', CAST(42 AS BIGINT), CAST(0 AS INT), CAST(0 AS INT), CAST(42 AS BIGINT),
        '{"customer_id":1}', CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
        current_timestamp(), DATE '2025-01-01'""")
    print("LEGACY_CDC_FIXTURE_OK", flush=True)
finally:
    spark.stop()
