from lakehouse.oltp.gold.schema import schema as gold_schema
from lakehouse.oltp.polars.schema import silver_schema
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.iceberg import evolve_table_schema
from lakehouse.utils.namespaces import namespace as ns


LEGACY_COLUMNS = {
    "silver": {
        "silver_customers": {
            "display_name_pseudonymized", "email_pseudonymized", "phone_pseudonymized",
            "full_name_pseudonymized", "anonymized_at", "_pii_pseudonymized_at",
        },
        "silver_orders": {
            "receiver_name_pseudonymized", "receiver_phone_pseudonymized",
            "shipping_address_text_pseudonymized", "_pii_pseudonymized_at",
        },
        "silver_delivery_staff": {"full_name_pseudonymized", "phone_pseudonymized", "_pii_pseudonymized_at"},
    },
    "gold": {"dim_customer": {"email", "phone"}, "dim_delivery_staff": {"phone"}},
}


def prepare_schema(catalog, prefix, *, migrate):
    silver = {spec.silver_table: spec for spec in SOURCE_TABLES}
    changes = []
    for layer, tables in LEGACY_COLUMNS.items():
        namespace = ns(prefix, layer)
        for name, allowed_removals in tables.items():
            key = (namespace, name)
            if not catalog.table_exists(key):
                continue
            table = catalog.load_table(key)
            schema = silver_schema(silver[name]) if layer == "silver" else gold_schema(name)
            existing = set(table.schema().column_names)
            removed = existing - set(schema.names)
            if removed - allowed_removals:
                raise ValueError(f"Unrecognized columns in {namespace}.{name}; schema migration must be reviewed")
            snapshot = table.current_snapshot()
            stale = snapshot is not None and snapshot.schema_id != table.schema().schema_id
            if removed or set(schema.names) - existing or stale:
                changes.append((table, schema, removed))

    if changes and not migrate:
        raise ValueError("Legacy analytical schemas require a full run with --migrate-schema")
    for table, schema, removed in changes:
        evolve_table_schema(table, schema, drop=removed)
