INSERT INTO lakehouse.bronze.oltp_cdc_events
    (event_id, topic, database_name, table_name, operation, event_ts, source_ts,
     transaction_id, mysql_server_id, binlog_file, binlog_position, binlog_row,
     kafka_partition, kafka_offset, record_key, before_payload, after_payload,
     raw_payload, ingested_at, event_date, source_connector, source_schema,
     postgres_lsn, postgres_tx_id)
SELECT
    CONCAT(topic, ':', CAST(kafka_partition AS STRING), ':', CAST(kafka_offset AS STRING)),
    topic,
    COALESCE(JSON_VALUE(payload, '$.source.db'), {{database_name}}),
    CASE WHEN topic LIKE '%.transaction' THEN '__transaction__' ELSE COALESCE(
        JSON_VALUE(payload, '$.source.table'),
        REGEXP_EXTRACT(topic, '[^.]+$', 0)
    ) END,
    CASE
        WHEN payload IS NULL THEN 't'
        ELSE COALESCE(JSON_VALUE(payload, '$.op'), JSON_VALUE(payload, '$.status'))
    END,
    COALESCE(
        TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT), 3),
        kafka_timestamp
    ),
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT), 3),
    COALESCE(JSON_VALUE(payload, '$.transaction.id'), JSON_VALUE(payload, '$.id')),
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    CAST(NULL AS BIGINT),
    CAST(NULL AS INT),
    kafka_partition,
    kafka_offset,
    record_key,
    JSON_QUERY(payload, '$.before'),
    JSON_QUERY(payload, '$.after'),
    payload,
    CURRENT_TIMESTAMP,
    CAST(
        COALESCE(
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT), 3),
            kafka_timestamp,
            CURRENT_TIMESTAMP
        ) AS DATE
    ),
    COALESCE(JSON_VALUE(payload, '$.source.connector'), 'postgresql'),
    COALESCE(JSON_VALUE(payload, '$.source.schema'),
        CASE WHEN topic LIKE '%.transaction' THEN CAST(NULL AS STRING) ELSE 'public' END),
    CAST(JSON_VALUE(payload, '$.source.lsn') AS BIGINT),
    CAST(JSON_VALUE(payload, '$.source.txId') AS BIGINT)
FROM postgres_cdc_kafka
