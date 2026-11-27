import pyarrow as pa

from lakehouse.oltp.bronze import DELETE_FIELDS, RELOAD_PENDING, reload_pending
from lakehouse.oltp.drift import classify, refuse, source_columns
from lakehouse.oltp.staging import LINEAGE_FIELDS
from lakehouse.oltp.sources import SOURCE_TABLES
from lakehouse.utils.iceberg import PROPERTIES, current_snapshot_has_manifests
from lakehouse.utils.postgres import source_read_transaction

LAYERS = ("staging", "bronze", "silver", "gold")


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


def evolve_schema(catalog, table, source):
    """Add the approved columns and widen the widened ones in staging and Bronze; refuse a breaking change first."""
    drift = classify(table, source, catalog.load_table(("bronze", table.name)).schema().as_arrow())
    refuse(table, drift, ("dropped", "incompatible"))
    if drift.evolve:
        changes = pa.schema([pa.field(name, source[name]) for name in drift.evolve])
        with catalog.load_table(("staging", table.name)).update_schema() as update:
            update.union_by_name(changes)
        bronze = catalog.load_table(("bronze", table.name))
        # The new columns and the reload they need are committed together, so a failure cannot leave one alone.
        with bronze.transaction() as transaction:
            with transaction.update_schema() as update:
                update.union_by_name(changes)
            if drift.approved:
                pending = sorted({*reload_pending(bronze), *drift.approved})
                transaction.set_properties({RELOAD_PENDING: ",".join(pending)})
    rewrite_files_lacking_columns(catalog.load_table(("bronze", table.name)))
    return drift


def rewrite_files_lacking_columns(target):
    """Rewrite the table in one commit when a data file predates one of its columns.

    PyIceberg cannot upsert into a table whose older files lack a column. Deciding from the files, not from the
    change, lets a retry finish a rewrite that failed.
    """
    if not current_snapshot_has_manifests(target):
        return
    field_ids = {field.field_id for field in target.schema().fields}
    files = [file for file in target.inspect.files().to_pylist() if file["content"] == 0]
    if any(field_ids - set(dict(file["value_counts"] or [])) for file in files):
        target.overwrite(target.scan().to_arrow_batch_reader())


def create_oltp_schema(catalog, connection):
    for layer in LAYERS:
        catalog.create_namespace_if_not_exists(layer)
    with source_read_transaction(connection):
        sources = {table.name: source_columns(connection, table) for table in SOURCE_TABLES}
    for table in SOURCE_TABLES:
        source = sources[table.name]
        if catalog.table_exists(("bronze", table.name)):
            evolve_schema(catalog, table, source)
        else:
            refuse(table, classify(table, source, pa.schema([])), ("dropped",))
            columns = [pa.field(name, source[name]) for name in table.columns]
            create_staging_and_bronze(catalog, table, pa.schema([*columns, *LINEAGE_FIELDS]))
        print(f"INIT table={table.name}", flush=True)
