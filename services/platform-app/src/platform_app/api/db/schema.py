"""Render the fresh PostgreSQL bootstrap; never connect to or upgrade a database."""

import argparse
from pathlib import Path

from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import AddConstraint, CreateIndex, CreateTable
from sqlalchemy.util import OrderedSet

from platform_app.api import models  # noqa: F401 — register all source tables
from platform_app.api.db.base import Base


UPDATED_AT_FUNCTION = """CREATE FUNCTION public.touch_updated_at_utc() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    ignored_columns text[] := ARRAY['updated_at'] || TG_ARGV;
BEGIN
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at
       AND (to_jsonb(NEW) - ignored_columns) IS DISTINCT FROM
           (to_jsonb(OLD) - ignored_columns) THEN
        NEW.updated_at := timezone('UTC', statement_timestamp());
    END IF;
    RETURN NEW;
END;
$$;"""


def render_schema() -> str:
    dialect = postgresql.dialect()
    tables = sorted(Base.metadata.tables.values(), key=lambda table: table.name)
    statements = [
        "-- D&K PostgreSQL 16 source schema. EMPTY database only; no data migration.",
        "-- Generated from ORM: python -m platform_app.api.db.schema --output database/schema.sql",
        "-- UUID is native; timestamps without timezone contain UTC; VND is integer.",
        "BEGIN;",
        "SET LOCAL search_path TO public;",
        "SET LOCAL TIME ZONE 'UTC';",
    ]
    # Add FKs only after all tables exist, including customer/store/order cycles.
    for table in tables:
        for column in table.c:
            column.constraints = OrderedSet(sorted(column.constraints, key=lambda constraint: constraint.name))
        statements.append(
            str(CreateTable(table, include_foreign_key_constraints=[]).compile(dialect=dialect)).strip() + ";"
        )
    for table in tables:
        for constraint in sorted(table.foreign_key_constraints, key=lambda item: item.name):
            statements.append(str(AddConstraint(constraint).compile(dialect=dialect)).strip() + ";")
        for index in sorted(table.indexes, key=lambda item: item.name):
            statements.append(str(CreateIndex(index).compile(dialect=dialect)).strip() + ";")
    statements.append(UPDATED_AT_FUNCTION)
    for table in tables:
        if "updated_at" not in table.c:
            continue
        generated = ", ".join("'" + column.name + "'" for column in table.c if column.computed is not None)
        statements.append(
            f"CREATE TRIGGER trg_{table.name}_updated_at BEFORE UPDATE ON public.{table.name} "
            f"FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at_utc({generated});"
        )
    statements.append("COMMIT;")
    header = "\n".join(statements[:3])
    return (header + "\n\n" + "\n\n".join(statements[3:])).replace(" \n", "\n") + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", action="store_true", help="Fail if the SQL file differs from ORM metadata")
    arguments = parser.parse_args()
    sql = render_schema()
    if arguments.check:
        if not arguments.output.is_file() or arguments.output.read_text(encoding="utf-8") != sql:
            parser.error("Schema differs from ORM; regenerate and review the SQL diff")
    else:
        arguments.output.write_text(sql, encoding="utf-8")


if __name__ == "__main__":
    main()
