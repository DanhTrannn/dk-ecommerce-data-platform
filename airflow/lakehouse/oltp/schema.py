"""Create the OLTP namespaces and the Bronze and Silver tables; the load jobs only write to what this creates.

Bronze copies the source, so its columns and types are read from PostgreSQL; Silver follows its reviewed contract.
Gold tables are left to dbt, which rebuilds them from Silver on every run.
"""

import pyarrow as pa
from pyiceberg.transforms import DayTransform

from lakehouse.oltp.bronze import BRONZE_PROPERTIES, LINEAGE_FIELDS
from lakehouse.oltp.silver import silver_schema
from lakehouse.oltp.source import arrow_source_schema
from lakehouse.oltp.sources import SOURCE_TABLES
from lakehouse.utils.iceberg import PROPERTIES, evolve_table_schema
from lakehouse.utils.postgres import source_query, source_read_transaction

LAYERS = ("bronze", "silver", "gold")


def bronze_schema(connection, table):
    """The source columns the batch reads, typed as the source reports them, plus the lineage columns."""
    with connection.cursor() as cursor:
        cursor.execute(source_query(table) + " LIMIT 0")
        return pa.schema([*arrow_source_schema(cursor.description), *LINEAGE_FIELDS])


def create_or_evolve(catalog, key, schema, properties):
    """Create the table, or add the columns its schema gained since it was created."""
    if catalog.table_exists(key):
        table = catalog.load_table(key)
        evolve_table_schema(table, schema)
        return table
    return catalog.create_table(key, schema=schema, properties=properties)


def create_bronze(catalog, table, schema):
    target = create_or_evolve(catalog, ("bronze", table.name), schema, BRONZE_PROPERTIES)
    if not target.spec().fields:
        with target.update_spec() as update:
            update.add_field("_ingested_at", DayTransform(), "_ingested_at_day")


def create_silver(catalog, table):
    create_or_evolve(catalog, ("silver", table.name), silver_schema(table), PROPERTIES)


def create_oltp_schema(catalog, connection):
    for layer in LAYERS:
        catalog.create_namespace_if_not_exists(layer)
    with source_read_transaction(connection):
        schemas = {table.name: bronze_schema(connection, table) for table in SOURCE_TABLES}
    for table in SOURCE_TABLES:
        create_bronze(catalog, table, schemas[table.name])
        create_silver(catalog, table)
        print(f"INIT table={table.name}", flush=True)
