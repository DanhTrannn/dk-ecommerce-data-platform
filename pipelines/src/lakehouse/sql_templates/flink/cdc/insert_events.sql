INSERT INTO lakehouse.bronze.oltp_cdc_events
SELECT
    CONCAT(topic, ':', CAST(kafka_partition AS STRING), ':', CAST(kafka_offset AS STRING)),
    topic,
    JSON_VALUE(payload, '$.source.db'),
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
    JSON_VALUE(payload, '$.source.server_id'),
    JSON_VALUE(payload, '$.source.file'),
    CAST(JSON_VALUE(payload, '$.source.pos') AS BIGINT),
    CAST(JSON_VALUE(payload, '$.source.row') AS INT),
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
    )
FROM mysql_cdc_kafka
