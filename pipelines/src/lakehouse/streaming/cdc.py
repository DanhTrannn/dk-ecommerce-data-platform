"""PostgreSQL CDC transport contract; importing it does not start Flink."""

import re

from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.sql import literal, render_sql

POSTGRES_COLUMNS = {
    "source_connector": "STRING",
    "source_schema": "STRING",
    "postgres_lsn": "BIGINT",
    "postgres_tx_id": "BIGINT",
}


def topic_pattern(prefix):
    if not prefix or not prefix.strip():
        raise ValueError("CDC_TOPIC_PREFIX must be nonempty")
    tables = "|".join(re.escape(table.name) for table in SOURCE_TABLES)
    return rf"{re.escape(prefix)}\.(?:public\.(?:{tables})|transaction)"


def kafka_source_ddl(bootstrap_servers, prefix, group_id):
    return render_sql(
        "flink/cdc/kafka_source.sql",
        topic_pattern=literal(topic_pattern(prefix)),
        bootstrap_servers=literal(bootstrap_servers),
        group_id=literal(group_id),
    )


def insert_sql(database):
    return render_sql("flink/cdc/insert_events.sql", database_name=literal(database))
