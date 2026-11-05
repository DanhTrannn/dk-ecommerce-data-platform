"""Auditable reference measurement; does not alter generator distributions."""

import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

from generator.reference_input import COLUMNS, read_reference_rows
from generator.reference_metrics import summarize_invoices


def _sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _load_spec(path):
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    if spec["cohort"]["duplicate_policy"] != "retain":
        raise ValueError("Only explicit duplicate_policy=retain is supported")
    if not re.fullmatch(r"[0-9a-f]{64}", spec["source"]["sha256"]):
        raise ValueError("source.sha256 must pin the exact input artifact")
    windows = {}
    for name in ("fit", "evaluation"):
        window = spec["windows"][name]
        start, end = (datetime.fromisoformat(window[key]) for key in ("start", "end"))
        if start.tzinfo is not None or end.tzinfo is not None or start >= end:
            raise ValueError("Windows require increasing, naive source-local ISO dates")
        windows[name] = (start, end)
    if windows["fit"][1] > windows["evaluation"][0]:
        raise ValueError("Fit and evaluation windows must not overlap or run backwards")
    return spec, windows


def _parse_line(row, stock_pattern):
    invoice = row["InvoiceNo"].strip()
    if invoice.upper().startswith("C"):
        return None, "credit_or_cancellation"
    if not re.fullmatch(r"[0-9]+", invoice):
        return None, "invalid_invoice_id"
    try:
        date = datetime.fromisoformat(row["InvoiceDate"].strip())
        if date.tzinfo is not None:
            raise ValueError("Source-local naive timestamp required")
    except ValueError:
        return None, "invalid_invoice_date"
    try:
        quantity = Decimal(row["Quantity"])
        if not quantity.is_finite() or quantity != quantity.to_integral_value():
            raise InvalidOperation
    except InvalidOperation:
        return None, "invalid_quantity"
    if quantity <= 0:
        return None, "nonpositive_quantity"
    try:
        price = Decimal(row["UnitPrice"])
        if not price.is_finite():
            raise InvalidOperation
    except InvalidOperation:
        return None, "invalid_price"
    if price <= 0:
        return None, "nonpositive_price"
    stock = row["StockCode"].strip()
    if not stock_pattern.fullmatch(stock):
        return None, "nonmerchandise_or_nonstandard_code"
    customer = row["CustomerID"].strip()
    if customer:
        try:
            value = Decimal(customer)
            if not value.is_finite() or value <= 0 or value != value.to_integral_value():
                raise InvalidOperation
            customer = str(int(value))
        except InvalidOperation:
            return None, "invalid_customer_id"
    country = row["Country"].strip()
    if not country:
        return None, "missing_country"
    return (invoice, stock, int(quantity), date, customer or None, country), None


def build_reference_profile(input_path: Path, spec_path: Path) -> dict:
    spec, windows = _load_spec(spec_path)
    checksum = _sha256(input_path)
    if checksum != spec["source"]["sha256"]:
        raise ValueError("Input SHA-256 does not match the pinned source artifact")
    pattern = re.compile(spec["cohort"]["stock_code_pattern"])
    invoices, raw_hashes, raw_invoice_ids = {}, set(), set()
    missing, excluded = Counter({name: 0 for name in COLUMNS}), Counter()
    raw_rows = duplicate_rows = 0
    for row in read_reference_rows(input_path):
        raw_rows += 1
        raw_invoice_ids.add(row["InvoiceNo"])
        missing.update(name for name, value in row.items() if not value.strip())
        digest = hashlib.sha256(json.dumps(list(row.values()), ensure_ascii=False).encode()).digest()
        is_duplicate = digest in raw_hashes
        duplicate_rows += is_duplicate
        raw_hashes.add(digest)
        parsed, reason = _parse_line(row, pattern)
        if reason:
            excluded[reason] += 1
            continue
        invoice_id, stock, quantity, date, customer, country = parsed
        invoice = invoices.setdefault(
            invoice_id,
            {
                "date": date,
                "customer": customer,
                "country": country,
                "items": Counter(),
                "rows": 0,
                "duplicate_rows": 0,
                "conflict": False,
            },
        )
        invoice["conflict"] |= (invoice["date"], invoice["customer"], invoice["country"]) != (date, customer, country)
        invoice["items"][stock] += quantity
        invoice["rows"] += 1
        invoice["duplicate_rows"] += is_duplicate
    cohorts = {name: [] for name in windows}
    excluded_invoices = Counter()
    for invoice in invoices.values():
        if invoice["conflict"]:
            reason = "invoice_metadata_conflict"
        elif invoice["country"] != spec["cohort"]["country"]:
            reason = "other_country"
        else:
            period = next((name for name, (start, end) in windows.items() if start <= invoice["date"] < end), None)
            if period is not None:
                cohorts[period].append(invoice)
                continue
            reason = "outside_windows"
        excluded[reason] += invoice["rows"]
        excluded_invoices[reason] += 1
    metrics = {
        name: summarize_invoices(rows, start=windows[name][0], end=windows[name][1]) for name, rows in cohorts.items()
    }
    kept_rows = sum(metric["eligible_rows"] for metric in metrics.values())
    assert raw_rows == sum(excluded.values()) + kept_rows
    profile = {
        "schema_version": 1,
        "profile_version": spec["profile_version"],
        "status": "measured_reference_not_applied_to_generator",
        "source": spec["source"],
        "measurement": {
            "policy_version": "uci-merchandise-v1",
            "cohort": spec["cohort"],
            "windows": spec["windows"],
            "timestamp_basis": "naive source-local; timezone not asserted by source",
            "quantiles": "nearest rank: sorted[ceil(q*n)-1]",
            "basket_grain": "invoice/distinct StockCode; repeated SKU rows sum Quantity",
            "quantity_grain": "one aggregated invoice/StockCode pair",
            "repeat_grain": "identified customer within each separate window; observed gaps only",
            "popularity_basis": "positive sold units by SKU, not revenue or unique buyers",
            "duplicate_policy": "retain suspected identical eight-field rows; no receipt-line ID exists",
        },
        "audit": {
            "raw_rows": raw_rows,
            "raw_distinct_invoice_ids": len(raw_invoice_ids),
            "candidate_invoices_after_line_filters": len(invoices),
            "missing_by_column": dict(missing),
            "suspected_duplicate_rows_raw": duplicate_rows,
            "suspected_duplicate_rows_kept": sum(
                invoice["duplicate_rows"] for rows in cohorts.values() for invoice in rows
            ),
            "excluded_rows_by_primary_reason": dict(sorted(excluded.items())),
            "excluded_candidate_invoices": dict(sorted(excluded_invoices.items())),
            "kept_rows": kept_rows,
        },
        "periods": metrics,
        "parameter_provenance": {
            "period_metrics": "measured_from_pinned_source_under_declared_policy",
            "country_windows_stock_pattern": "analytical_cohort_choices",
            "duplicate_policy": "assumption_retain_ambiguous_rows",
            "generator_price_currency_seasonality_pos_cod_coupon_returns_reviews": "not_calibrated",
        },
    }
    payload = json.dumps(profile, sort_keys=True, ensure_ascii=False, allow_nan=False)
    profile["profile_id"] = hashlib.sha256(payload.encode()).hexdigest()
    return profile


def write_reference_profile(input_path: Path, spec_path: Path, output_path: Path) -> dict:
    if output_path.resolve() in {input_path.resolve(), spec_path.resolve()}:
        raise ValueError("Profile output must not overwrite input or specification")
    profile = build_reference_profile(input_path, spec_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(json.dumps(profile, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(output_path)
    return profile
