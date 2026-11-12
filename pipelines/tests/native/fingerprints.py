"""Semantic hashes for engine parity; omit ingest metadata and never output raw data."""

import hashlib
import json
from datetime import UTC, datetime, date
from decimal import Decimal


def encode(value):
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=UTC).isoformat(timespec="microseconds")
            if value.tzinfo is None
            else value.astimezone(UTC).isoformat(timespec="microseconds")
        )
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def fingerprint(frame):
    names = sorted(name for name in frame.columns if not name.startswith("_"))
    rows = [
        {name: round(value, 6) if isinstance(value, float) else value for name, value in row.items()}
        for row in frame.select(names).to_dicts()
    ]

    def digest(items):
        content = "\n".join(
            sorted(
                json.dumps(item, ensure_ascii=False, sort_keys=True, default=encode, separators=(",", ":"))
                for item in items
            )
        )
        return hashlib.sha256(content.encode()).hexdigest()

    return {
        "count": len(rows),
        "digest": digest(rows),
        "columns": {name: digest([row[name] for row in rows]) for name in names},
    }
