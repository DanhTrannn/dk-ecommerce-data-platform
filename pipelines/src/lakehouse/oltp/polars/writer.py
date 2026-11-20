"""Iceberg writes preserve source versions, newer current state and quarantine identity."""

from lakehouse.utils.namespaces import namespace as ns
import polars as pl
import pyarrow as pa
from pyiceberg.expressions import In
from pyiceberg.transforms import DayTransform

from lakehouse.oltp.polars.schema import align_arrow, quarantine_schema, silver_schema
from lakehouse.oltp.polars.transforms import filter_current, prepare_silver
from lakehouse.utils.iceberg import evolve_table_schema, load_or_create


PROPERTIES = {"format-version": "2", "write.parquet.compression-codec": "zstd"}


def quarantine_table(catalog, namespace):
    table = load_or_create(
        catalog,
        (namespace, "silver_oltp_violations"),
        schema=quarantine_schema(),
        properties=PROPERTIES,
    )
    # Bind the partition to catalog-assigned field IDs rather than guessing Arrow IDs.
    if not table.spec().fields:
        with table.update_spec() as update:
            update.add_field("_quarantined_at", DayTransform(), "_quarantined_at_day")
    return table


def ingest_page(catalog, prefix, table, page, run_id, observed):
    bronze = load_or_create(
        catalog, (ns(prefix, "bronze"), table.name), schema=page.schema, properties=PROPERTIES
    )
    evolve_table_schema(bronze, page.schema)
    if page.num_rows:
        keys = [table.pk, table.cursor_field]
        new_versions = page
        if bronze.current_snapshot() is not None:
            existing = bronze.scan(
                row_filter=In(table.pk, page.column(table.pk).to_pylist()), selected_fields=tuple(keys),
            ).to_arrow()
            new_versions = pl.from_arrow(page).join(pl.from_arrow(existing), on=keys, how="anti").to_arrow()
        if new_versions.num_rows:
            bronze.append(align_arrow(new_versions, bronze.schema()))
    return merge_silver(catalog, prefix, table, page, run_id, observed)


def merge_silver(catalog, prefix, table, page, run_id, observed):
    schema = silver_schema(table)
    silver = load_or_create(
        catalog, (ns(prefix, "silver"), table.silver_table), schema=schema, properties=PROPERTIES
    )
    rejected = quarantine_table(catalog, ns(prefix, "quarantine"))
    delta, violations = prepare_silver(page, table, schema, run_id, observed)
    accepted = delta.num_rows
    if violations.num_rows:
        violations = (
            pl.from_arrow(violations)
            .unique(subset=["source_table", "record_data", "violation_type"], maintain_order=True)
            .to_arrow()
        )
        # Bound the composite-key expression built by PyIceberg.
        for batch in violations.to_batches(max_chunksize=512):
            rejected.upsert(
                align_arrow(pa.Table.from_batches([batch]), rejected.schema()),
                join_cols=["source_table", "record_data", "violation_type"],
                when_matched_update_all=False,
            )
    if delta.num_rows:
        keys = delta.column(table.pk).to_pylist()
        fields = tuple(name for name in delta.column_names if not name.startswith("_"))
        current = silver.scan(row_filter=In(table.pk, keys), selected_fields=fields).to_arrow()
        delta = filter_current(delta, current, table)
        if delta.num_rows:
            silver.upsert(align_arrow(delta, silver.schema()), join_cols=[table.pk])
    return {"accepted": accepted, "valid": delta.num_rows, "quarantined": violations.num_rows}
