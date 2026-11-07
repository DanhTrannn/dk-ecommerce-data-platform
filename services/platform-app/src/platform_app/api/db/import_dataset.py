"""Import trusted generator SQL with rollback, schema checks and a JSON receipt."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from psycopg2 import Error as DriverError
from sqlalchemy import CheckConstraint, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Connection, Engine

from platform_app.api.db.base import Base
from platform_app.api.db.generated_sql import iter_statements, read_metadata
from platform_app.api.db.schema import render_schema


def _check_schema(connection: Connection) -> None:
    inspector = inspect(connection)
    if not set(Base.metadata.tables) <= set(inspector.get_table_names(schema="public")):
        raise ValueError("Source schema is missing; apply database/schema.sql to the source database first")
    triggers = set(
        connection.execute(
            text("""SELECT c.relname,t.tgname FROM pg_trigger t
        JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND NOT t.tgisinternal AND t.tgenabled='O'""")
        ).tuples()
    )
    for name, table in Base.metadata.tables.items():
        indexes = {item["name"]: item for item in inspector.get_indexes(name, schema="public")}
        if any(index.name not in indexes or indexes[index.name]["unique"] != index.unique for index in table.indexes):
            raise ValueError(f"Source indexes do not match: {name}")
        if "updated_at" in table.c and (name, f"trg_{name}_updated_at") not in triggers:
            raise ValueError(f"Source updated_at trigger is missing or disabled: {name}")
        columns = {col["name"]: col for col in inspector.get_columns(name, schema="public")}
        if set(columns) != set(table.c.keys()):
            raise ValueError(f"Source columns do not match the current schema: {name}")
        for col in table.c:
            if str(columns[col.name]["type"].compile(connection.dialect)) != str(col.type.compile(connection.dialect)):
                raise ValueError(f"Source column type does not match: {name}.{col.name}")
            if columns[col.name]["nullable"] != col.nullable:
                raise ValueError(f"Source nullability does not match: {name}.{col.name}")
            if col.identity is not None and not columns[col.name].get("identity"):
                raise ValueError(f"Source identity is missing: {name}.{col.name}")
        checks = [c for c in table.constraints if isinstance(c, CheckConstraint)]
        checks += [c for col in table.c for c in col.constraints if isinstance(c, CheckConstraint)]
        if len(inspector.get_foreign_keys(name, schema="public")) != len(table.foreign_key_constraints) or len(
            inspector.get_check_constraints(name, schema="public")
        ) != len(checks):
            raise ValueError(f"Source constraints do not match: {name}")


def import_dataset(engine: Engine, path: Path) -> dict[str, Any]:
    if engine.dialect.name != "postgresql":
        raise ValueError("Dataset import requires PostgreSQL")
    schema_checksum = hashlib.sha256(render_schema().encode()).hexdigest()
    with path.open("rb") as source:
        checksum = hashlib.file_digest(source, "sha256").hexdigest()
    with path.open(encoding="utf-8", newline="") as stream:
        metadata = read_metadata(stream)
        run_id = metadata["generation_run_id"]
        with engine.begin() as connection:
            _check_schema(connection)
            # Serialize imports, including duplicate checks, without creating a ledger table.
            connection.execute(text("SELECT pg_advisory_xact_lock(841671902)"))
            duplicate = connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM public.customers WHERE generation_run_id=:run)"), {"run": run_id}
            )
            if duplicate:
                raise ValueError("Dataset already imported; no rows were changed")
            cursor = connection.connection.driver_connection.cursor()
            try:
                for statement in iter_statements(stream):
                    # No DBAPI parameters: preserve SQL format('%I', ...) and literal percent signs.
                    cursor.execute(statement)
            finally:
                cursor.close()
            totals = (
                connection.execute(
                    text("""SELECT COUNT(*) AS orders,
                COALESCE(SUM(total_vnd),0) AS placed_order_value_vnd
                FROM public.orders WHERE generation_run_id=:run"""),
                    {"run": run_id},
                )
                .mappings()
                .one()
            )
            customers = connection.scalar(
                text("SELECT COUNT(*) FROM public.customers WHERE generation_run_id=:run"), {"run": run_id}
            )
            if totals["orders"] != metadata["counts"]["orders"] or customers != metadata["counts"]["customers"]:
                raise ValueError("Imported dataset does not match its expected counts")
    return {
        "sql_sha256": checksum,
        "expected_schema_sha256": schema_checksum,
        "sql_format": metadata["sql_format"],
        "generator_version": metadata["generator_version"],
        "generation_run_id": run_id,
        "logical_identity": metadata["logical_identity"],
        "customers": customers,
        "orders": totals["orders"],
        "placed_order_value_vnd": int(totals["placed_order_value_vnd"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sql_file", type=Path)
    arguments = parser.parse_args()
    try:
        from platform_app.api.db.session import engine

        receipt = import_dataset(engine, arguments.sql_file)
    except (ValueError, OSError, SQLAlchemyError, DriverError, KeyError) as error:
        # DB errors can include whole SQL batches, credentials or personal data.
        parser.exit(
            1,
            f"Import failed ({type(error).__name__}); transaction rolled back. Check artifact/schema and rerun validation.\n",
        )
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
