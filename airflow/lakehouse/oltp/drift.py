from dataclasses import dataclass, fields

import pyarrow as pa

from lakehouse.oltp.source import arrow_source_schema
from lakehouse.utils.postgres import identifier

# The type changes Iceberg can apply in place: int to long, float to double, and a wider decimal of the same scale.
PROMOTIONS = {(pa.int32(), pa.int64()), (pa.float32(), pa.float64())}
ACTIONS = {
    "dropped": "remove it from sources.py and the Silver model, then clear the task",
    "incompatible": "update the Silver model, drop the Bronze table and run lakehouse_oltp_init",
    "unlisted": "add it to `columns` (and the Silver model) or `excluded` in sources.py, then clear the task",
}


@dataclass(frozen=True)
class Drift:
    """How the source's columns differ from sources.py and Bronze, by kind of change."""

    unlisted: tuple[str, ...] = ()
    approved: tuple[str, ...] = ()
    widened: tuple[str, ...] = ()
    narrowed: tuple[str, ...] = ()
    dropped: tuple[str, ...] = ()
    incompatible: tuple[str, ...] = ()

    @property
    def breaking(self):
        return self.dropped + self.incompatible

    @property
    def evolve(self):
        return self.approved + self.widened


class SchemaChanged(RuntimeError):
    """The source's columns changed in a way the pipeline must not apply on its own."""


def refuse(table, drift, kinds):
    """Raise SchemaChanged naming each column of the given kinds and what to do about it."""
    problems = [f"{kind} {list(getattr(drift, kind))}: {ACTIONS[kind]}" for kind in kinds if getattr(drift, kind)]
    if problems:
        raise SchemaChanged(f"Source table {table.name} changed: " + "; ".join(problems))


def source_columns(connection, table):
    """Return every source column with its Arrow type, or None for columns sources.py does not load."""
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT * FROM "public".{identifier(table.name)} LIMIT 0')
        description = cursor.description
    return {
        column.name: arrow_source_schema([column])[0].type if column.name in table.columns else None
        for column in description
    }


def normalized(kind):
    if pa.types.is_large_string(kind):
        return pa.string()
    if pa.types.is_large_binary(kind):
        return pa.binary()
    if pa.types.is_timestamp(kind) and kind.tz is not None:
        return pa.timestamp(kind.unit, tz="UTC")
    return kind


def promotes(old, new):
    if pa.types.is_decimal(old) and pa.types.is_decimal(new):
        return new.scale == old.scale and new.precision > old.precision
    return (old, new) in PROMOTIONS


def classify(table, source, bronze):
    """Compare the source columns with sources.py and the Bronze schema; lineage columns are ignored."""
    current = {field.name: normalized(field.type) for field in bronze if not field.name.startswith("_")}
    found = {field.name: [] for field in fields(Drift)}
    for name in source:
        if name not in table.columns and name not in table.excluded:
            found["unlisted"].append(name)
    for name in table.columns:
        if name not in source:
            found["dropped"].append(name)
        elif name not in current:
            found["approved"].append(name)
        elif normalized(source[name]) != current[name]:
            old, new = current[name], normalized(source[name])
            kind = "widened" if promotes(old, new) else "narrowed" if promotes(new, old) else "incompatible"
            found[kind].append(name)
    return Drift(**{kind: tuple(sorted(names)) for kind, names in found.items()})
