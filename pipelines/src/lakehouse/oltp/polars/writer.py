"""Iceberg writes preserve source versions, newer current state and quarantine identity."""

from lakehouse.utils.namespaces import namespace as ns
import polars as pl
from pyiceberg.expressions import In, IsNull, NotEqualTo, Or
from pyiceberg.transforms import DayTransform

from lakehouse.oltp.polars.schema import align_arrow, quarantine_schema, silver_schema
from lakehouse.oltp.polars.transforms import filter_current, prepare_silver


PROPERTIES = {"format-version": "2", "write.parquet.compression-codec": "zstd"}


def guard_bronze(table):
    if "_source_system" not in table.schema().column_names:
        raise ValueError("Bronze contains legacy or unclassified source history")
    if table.current_snapshot() is None:
        return
    invalid = Or(IsNull("_source_system"), NotEqualTo("_source_system", "postgresql_ecommerce"))
    if table.scan(row_filter=invalid, selected_fields=("_source_system",), limit=1).to_arrow().num_rows:
        raise ValueError("Bronze contains legacy or unclassified source history")


def evolve_bronze(table, schema):
    if set(schema.names) - set(table.schema().column_names):
        with table.update_schema() as update:
            update.union_by_name(schema)


def quarantine_table(catalog, namespace):
    table = catalog.create_table_if_not_exists(
        (namespace, "silver_oltp_violations"),
        schema=quarantine_schema(),
        properties=PROPERTIES,
    )
    # Bind the partition to catalog-assigned field IDs rather than guessing Arrow IDs.
    if not table.spec().fields:
        with table.update_spec() as update:
            update.add_field("_quarantined_at", DayTransform(), "_quarantined_at_day")
    return table


def ingest_page(catalog, prefix, table, page, run_id, observed, salt):
    bronze = catalog.load_table((ns(prefix, "bronze"), table.name))
    guard_bronze(bronze)
    evolve_bronze(bronze, page.schema)
    if page.num_rows:
        bronze.upsert(align_arrow(page, bronze.schema()), join_cols=[table.pk, table.cursor_field])
    return merge_silver(catalog, prefix, table, page, run_id, observed, salt)


def merge_silver(catalog, prefix, table, page, run_id, observed, salt):
    schema = silver_schema(table)
    silver = catalog.create_table_if_not_exists(
        (ns(prefix, "silver"), table.silver_table), schema=schema, properties=PROPERTIES
    )
    rejected = quarantine_table(catalog, ns(prefix, "quarantine"))
    delta, violations = prepare_silver(page, table, schema, run_id, observed, salt)
    if violations.num_rows:
        violations = (
            pl.from_arrow(violations)
            .unique(subset=["source_table", "record_data", "violation_type"], maintain_order=True)
            .to_arrow()
        )
        rejected.upsert(
            align_arrow(violations, rejected.schema()),
            join_cols=["source_table", "record_data", "violation_type"],
            when_matched_update_all=False,
        )
    if delta.num_rows:
        keys = delta.column(table.pk).to_pylist()
        current = silver.scan(row_filter=In(table.pk, keys), selected_fields=(table.pk, table.cursor_field)).to_arrow()
        delta = filter_current(delta, current, table)
        if delta.num_rows:
            silver.upsert(align_arrow(delta, silver.schema()), join_cols=[table.pk])
    return {"valid": delta.num_rows, "quarantined": violations.num_rows}
