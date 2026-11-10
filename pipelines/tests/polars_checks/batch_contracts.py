"""Engine-neutral acceptance fingerprints; never return row contents or raw PII."""

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal

UTC = timezone.utc
PREFIX = "polars_batch_acceptance"
FAULTS = {
    "customers": {"role": None, "display_name": "  ĐẶNG TRẦN  "},
    "categories": {"category_id": None, "code": None},
    "products": {"category_id": None},
    "product_variants": {"price_vnd": -1},
    "orders": {
        "status": "invalid",
        "total_vnd": -1,
        "receiver_name": "  ĐẶNG TRẦN  ",
        "receiver_phone": "0900000000",
        "shipping_address_text": "Quận 7, Hồ Chí Minh",
    },
    "order_items": {"quantity": 0, "line_total_vnd": -1},
    "cart_items": {"quantity": 0},
    "payments": {"status": "invalid", "amount_vnd": -1},
    "refunds": {"status": "invalid", "amount_vnd": -1},
    "coupons": {"discount_type": "percentage", "discount_value": 101},
    "product_reviews": {"rating": 9},
    "inventory": {"on_hand": -1},
    "store_inventory": {"on_hand": -1},
    "shipments": {"status": "invalid"},
    "return_requests": {"action_type": "invalid"},
    "inventory_transactions": {"movement_type": "invalid"},
    "inbound_receipts": {"total_cost_vnd": -1, "total_items_count": -1},
    "inbound_receipt_items": {"quantity": 0, "total_cost_vnd": -1},
}


# Exercise generic missing-key quarantine for every remaining configured source.
from lakehouse.utils.config import SOURCE_TABLES

for table in SOURCE_TABLES:
    FAULTS.setdefault(table.name, {table.pk: None})
FAULTS["delivery_staff"].update(full_name="  ĐẶNG TRẦN  ", phone="0900000000")


def encode(value):
    if isinstance(value, datetime):
        value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        return value.isoformat(timespec="microseconds")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def fingerprint(rows, columns):
    columns = sorted(name for name in columns if not name.startswith("_"))
    safe = []
    for item in rows:
        row = item.asDict(recursive=True) if hasattr(item, "asDict") else item
        row = {name: row[name] for name in columns}
        if "record_data" in row:
            row["record_data"] = json.loads(row["record_data"])
        safe.append(row)

    def digest(values):
        content = "\n".join(
            sorted(
                json.dumps(value, sort_keys=True, ensure_ascii=False, default=encode, separators=(",", ":"))
                for value in values
            )
        )
        return hashlib.sha256(content.encode()).hexdigest()

    return {
        "count": len(safe),
        "digest": digest(safe),
        "columns": {name: digest([row[name] for row in safe]) for name in columns},
    }
