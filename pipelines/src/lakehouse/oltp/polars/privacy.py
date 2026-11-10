"""Keep the existing salted SHA-256 and quarantine representation contracts."""

import base64
import hashlib
import json
import os
import re
from datetime import UTC, date, datetime
from decimal import Decimal

import polars as pl

PII_PATTERN = re.compile(r"(^|_)(email|phone|full_name|display_name|receiver_name|receiver_phone|address)(_|$)", re.I)


def required_salt():
    salt = os.environ.get("SILVER_PSEUDONYMIZE_SALT", "").strip()
    if not salt:
        raise ValueError("SILVER_PSEUDONYMIZE_SALT is required and must not be empty")
    return salt


def token(value, salt):
    if value is None:
        return None
    # Spark trim removes U+0020; using Python strip() would also remove tabs/newlines.
    normalized = str(value).strip(" ").lower()
    return hashlib.sha256((salt + ":" + normalized).encode()).hexdigest()


def token_expr(name, salt):
    return pl.col(name).map_elements(lambda value: token(value, salt), return_dtype=pl.String)


def token_name(name):
    return "email_pseudonymized" if name == "email_normalized" else name + "_pseudonymized"


def record_json(row):
    def scalar(value):
        if isinstance(value, datetime):
            value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            value = value.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        elif isinstance(value, date):
            value = value.isoformat()
        elif isinstance(value, (bytes, bytearray)):
            value = base64.b64encode(value).decode()
        elif isinstance(value, Decimal):
            return format(value, "f")
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    return (
        "{"
        + ",".join(
            json.dumps(name, ensure_ascii=False) + ":" + scalar(row[name])
            for name in sorted(row)
            if not name.startswith("_") and row[name] is not None
        )
        + "}"
    )
