"""Explicit source/Silver diagnostic for a quiescent PostgreSQL source."""

import math
from dataclasses import asdict

import polars as pl

from lakehouse.utils.snapshots import capture, read_snapshot
from lakehouse.oltp.quality.storage import persist_quality
from lakehouse.oltp.quality.contracts import QualityCheckResult
from lakehouse.utils.config import SOURCE_TABLES
from lakehouse.utils.namespaces import namespace as ns
from lakehouse.utils.postgres import identifier, namespace_lock
from lakehouse.utils.sql import load_sql


class ReconciliationGateError(ValueError):
    pass


def _variance_percent(source_value, target_value):
    if source_value == target_value:
        return 0.0
    return 100.0 if source_value == 0 else round(abs(source_value - target_value) / abs(source_value) * 100, 4)


def _within_tolerance(source_value, target_value, tolerance_pct):
    return target_value == 0 if source_value == 0 else _variance_percent(source_value, target_value) <= tolerance_pct


def run_reconciliation_gate(catalog, connection, *, prefix="serving", tolerance_pct=0.05, run_id, batch_date):
    if not math.isfinite(tolerance_pct) or not 0 <= tolerance_pct <= 100:
        raise ValueError("Invalid tolerance")
    with namespace_lock(connection, prefix):
        results = []
        frames = {}

        def check(name, table, source, target):
            passed = _within_tolerance(source, target, tolerance_pct)
            results.append(
                QualityCheckResult(
                    name,
                    table,
                    str(target),
                    str(source),
                    passed,
                    "Quiescent PostgreSQL/Silver diagnostic",
                    variance_percent=_variance_percent(source, target),
                    failed_rows=abs(source - target),
                )
            )

        with connection.cursor() as cursor:
            for table in SOURCE_TABLES:
                cursor.execute('SELECT count(*) FROM "public".' + identifier(table.name))
                source = cursor.fetchone()[0]
                target = catalog.load_table((ns(prefix, "silver"), table.silver_table))
                frames[table.name] = read_snapshot(target, capture(target))
                check("row_count", table.name, source, frames[table.name].height)
            for name, table, column, sql, predicate in [
                ("gross_revenue_vnd", "orders", "total_vnd", "order_revenue", pl.col("status") != "cancelled"),
                (
                    "successful_payments_vnd",
                    "payments",
                    "amount_vnd",
                    "successful_payments",
                    pl.col("status") == "succeeded",
                ),
                (
                    "successful_refunds_vnd",
                    "refunds",
                    "amount_vnd",
                    "successful_refunds",
                    pl.col("status") == "succeeded",
                ),
            ]:
                cursor.execute(load_sql("postgres/" + sql + ".sql"))
                source = int(cursor.fetchone()[0] or 0)
                target = int(frames[table].filter(predicate)[column].sum() or 0)
                check(name, table, source, target)
        failed = sum(not item.passed for item in results)
        summary = dict(
            total_checks=len(results),
            passed_checks=len(results) - failed,
            failed_checks=failed,
            status="FAILED" if failed else "PASSED",
            results=[asdict(item) for item in results],
        )
        catalog.create_namespace_if_not_exists(ns(prefix, "system"))
        persist_quality(catalog, prefix, summary, run_id, batch_date, phase="pre_gold_reconciliation")
        if failed:
            raise ReconciliationGateError("Source/Silver reconciliation failed; inspect persisted quality results")
        return summary
