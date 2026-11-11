"""Topological native Gold registry and existing column contracts."""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import polars as pl

from lakehouse.oltp.gold.schema import GOLD_TABLE_DDL
from lakehouse.oltp.polars.schema import align_arrow, ddl_schema
from .core import metadata
from .dimensions import SQL as DIMENSIONS, build_dimension
from .inventory import SQL as INVENTORY, build_inventory
from .marts import SQL as MARTS, build_mart
from .observations import SQL as OBSERVATIONS, build_observation
from .sales import fact_order, fact_order_item

TABLES = tuple(GOLD_TABLE_DDL)


def schema(name):
    if name not in TABLES:
        raise ValueError("Unknown Gold table")
    return ddl_schema(f"spark/gold/{name}.sql")


def build_all(silver, run_id, observed, *, inventory_history=None, ingested_at=None):
    if not isinstance(observed, datetime) or observed.tzinfo is None:
        raise ValueError("Gold observation time must include timezone")
    ingested_at = ingested_at or datetime.now(UTC)
    frames = dict(silver)
    frames["clock"] = pl.DataFrame({"observed_at": [observed]})
    built = {}
    day = observed.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
    for name in TABLES:
        if name == "dim_date" or name in DIMENSIONS:
            frame = build_dimension(name, frames)
        elif name == "fact_order_item":
            frame = fact_order_item(frames)
        elif name == "fact_order":
            frame = fact_order(frames)
        elif name in INVENTORY or name == "fact_inventory_current":
            frame = build_inventory(name, frames, day)
        elif name in MARTS:
            frame = build_mart(name, frames)
        elif name in OBSERVATIONS:
            frame = build_observation(name, frames)
        else:
            raise ValueError("Gold builder missing: " + name)
        # Align before downstream builders so types and nullable fields are stable even for empty data.
        arrow = align_arrow(metadata(frame, run_id, ingested_at).to_arrow(), schema(name))
        built[name] = pl.from_arrow(arrow)
        if name == "fact_inventory_daily_snapshot" and inventory_history is not None:
            previous = inventory_history.filter(pl.col("snapshot_date") != day)
            built[name] = pl.concat([previous, built[name]], how="vertical")
        frames[name] = built[name]
    return built
