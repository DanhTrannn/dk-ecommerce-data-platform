import pyarrow as pa

from lakehouse.oltp.bronze import DELETE_FIELDS
from lakehouse.oltp.staging import LINEAGE_FIELDS
from lakehouse.oltp.source import arrow_source_schema
from lakehouse.oltp.sources import SOURCE_TABLES
from lakehouse.utils.iceberg import PROPERTIES
from lakehouse.utils.postgres import source_query, source_read_transaction

LAYERS = ("staging", "bronze", "silver", "gold")
# Each Gold mart's progress: the latest Gold `_ingested_at` and the last day it has built. A mart's dbt post-hook
# advances its row only after the mart was written, so a failed run is picked up by the next one.
MART_STATE = ("gold", "_mart_state")
MART_STATE_SCHEMA = pa.schema([
    pa.field("mart", pa.string()),
    pa.field("watermark", pa.timestamp("us", tz="UTC")),
    pa.field("last_day", pa.date32()),
])


def source_schema(connection, table):
    with connection.cursor() as cursor:
        cursor.execute(source_query(table) + " LIMIT 0")
        return pa.schema([*arrow_source_schema(cursor.description), *LINEAGE_FIELDS])


def create_or_evolve(catalog, key, schema, properties):
    """Create the table, or add the columns the source gained; columns dropped at the source are kept."""
    if catalog.table_exists(key):
        table = catalog.load_table(key)
        if set(schema.names) - set(table.schema().column_names):
            with table.update_schema() as update:
                update.union_by_name(schema)
        return table
    return catalog.create_table(key, schema=schema, properties=properties)


def create_staging_and_bronze(catalog, table, schema):
    create_or_evolve(catalog, ("staging", table.name), schema, PROPERTIES)
    create_or_evolve(catalog, ("bronze", table.name), pa.schema([*schema, *DELETE_FIELDS]), PROPERTIES)


def create_oltp_schema(catalog, connection):
    for layer in LAYERS:
        catalog.create_namespace_if_not_exists(layer)
    with source_read_transaction(connection):
        schemas = {table.name: source_schema(connection, table) for table in SOURCE_TABLES}
    for table in SOURCE_TABLES:
        create_staging_and_bronze(catalog, table, schemas[table.name])
        print(f"INIT table={table.name}", flush=True)
    create_or_evolve(catalog, MART_STATE, MART_STATE_SCHEMA, PROPERTIES)
