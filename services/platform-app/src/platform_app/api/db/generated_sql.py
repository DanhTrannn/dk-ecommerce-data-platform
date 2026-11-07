"""Read the line-oriented format emitted by the trusted generator CLI.

This is deliberately not a general SQL parser or a sandbox for uploaded SQL.
Generated E literals contain escaped newlines; INSERT rows stay on one line.
"""

import json
import re
import uuid
from typing import Any, Iterator, TextIO

from generator.postgres_sql import SQL_FORMAT


def read_metadata(stream: TextIO) -> dict[str, Any]:
    first = stream.readline()
    if first != "-- TLCN deterministic PostgreSQL dataset\n":
        raise ValueError("Expected a PostgreSQL generator artifact; regenerate legacy SQL")
    line = stream.readline()
    if not line.startswith("-- import_metadata: "):
        raise ValueError("Generated SQL has no import metadata")
    metadata = json.loads(line.removeprefix("-- import_metadata: "))
    if not isinstance(metadata, dict) or not isinstance(metadata.get("counts"), dict):
        raise ValueError("Invalid import metadata")
    if not isinstance(metadata.get("generator_version"), str) or not metadata["generator_version"]:
        raise ValueError("Missing generator version")
    if metadata["sql_format"] != SQL_FORMAT:
        raise ValueError("Unsupported generated SQL format")
    for name in ("generation_run_id", "logical_identity"):
        if not isinstance(metadata.get(name), str) or str(uuid.UUID(metadata[name])) != metadata[name]:
            raise ValueError("Invalid dataset identity")
    for name in ("customers", "products", "variants", "orders"):
        value = metadata["counts"][name]
        if type(value) is not int or value < (0 if name == "orders" else 1):
            raise ValueError("Invalid generated row counts")
    for line in stream:
        if line.strip() == "BEGIN;":
            return metadata
        if line.strip() and not line.startswith("--"):
            raise ValueError("Unexpected SQL before BEGIN")
    raise ValueError("Generated SQL is missing BEGIN")


def iter_statements(stream: TextIO) -> Iterator[str]:
    parts, dollar_tag = [], None
    committed = False
    for line in stream:
        stripped = line.strip()
        if not parts and (not stripped or stripped.startswith("--")):
            continue
        if committed:
            raise ValueError("Unexpected data after COMMIT")
        if not parts:
            if stripped == "COMMIT;":
                committed = True
                continue
            match = re.fullmatch(r"DO (\$[a-z_]*\$)", stripped)
            dollar_tag = match[1] if match else None
        parts.append(line)
        finished = stripped == dollar_tag + ";" if dollar_tag else stripped.endswith(";")
        if not finished:
            continue
        statement = "".join(parts)
        if statement.strip() in {"BEGIN;", "ROLLBACK;"}:
            raise ValueError("Unexpected transaction boundary")
        yield statement
        parts, dollar_tag = [], None
    if parts or not committed:
        raise ValueError("Generated SQL is truncated or missing COMMIT")
