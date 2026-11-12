"""Immutable Iceberg reads, normalizing legacy UTC timestamps."""

import polars as pl
import pyarrow as pa


def utc_frame(arrow):
    frame = pl.from_arrow(arrow)
    naive = [
        pl.col(name).dt.replace_time_zone("UTC")
        for name, kind in frame.schema.items()
        if isinstance(kind, pl.Datetime) and kind.time_zone is None
    ]
    return frame.with_columns(naive) if naive else frame


def read_snapshot(table, snapshot_id):
    if snapshot_id is None:
        return utc_frame(pa.Table.from_pylist([], schema=table.schema().as_arrow()))
    # Trino's position-delete Parquet encoding is not dictionary-decodable by Arrow 18.
    # Analytical tables are read through Trino, rather than patching PyIceberg internals.
    if table.name()[-2] == "gold" or table.name()[-2].endswith("_gold"):
        from lakehouse.utils.trino import read_gold_snapshot

        return utc_frame(read_gold_snapshot(table, snapshot_id))
    return utc_frame(table.scan(snapshot_id=snapshot_id).to_arrow())


def capture(table):
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot else None
