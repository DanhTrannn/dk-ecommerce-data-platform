"""Blocking post-Gold checks, independent from builder outputs and before publication."""

from dataclasses import asdict

import polars as pl

from lakehouse.oltp.quality.contracts import QualityCheckResult
from .domains import domain_checks
from lakehouse.utils.frames import query

FRESH = (
    "fact_order",
    "fact_order_item",
    "mart_sales_daily",
    "mart_order_daily",
    "mart_product_sales_daily",
    "mart_inventory_current",
    "mart_inventory_summary",
    "mart_store_overview",
    "mart_operations_summary",
    "mart_cod_region_daily",
    "mart_marketing_summary",
    "mart_coupon_usage",
    "mart_product_review_summary",
    "mart_admin_overview",
)


def amount(frame, name):
    return int(frame[name].sum() or 0)


def evaluate(frames, run_id):
    results = []

    def record(name, dataset, observed, expected, passed, details, failed_rows=0):
        variance = 0.0
        if name == "silver_to_gold_row_count" and observed != expected:
            variance = 100.0 if expected == 0 else round(abs(observed - expected) / abs(expected) * 100, 4)
        results.append(
            QualityCheckResult(
                name,
                dataset,
                str(observed),
                str(expected),
                bool(passed),
                details,
                failed_rows=int(failed_rows),
                variance_percent=variance,
            )
        )

    def bad_rows(name, table, sql):
        n = query(frames, sql).height
        record(name, table, n, 0, n == 0, "Independent row-level constraint", n)

    orders, items = frames["fact_order"], frames["fact_order_item"]
    for source, target in (("orders", "fact_order"), ("order_items", "fact_order_item")):
        observed, expected = frames[target].height, frames[source].height
        difference = abs(observed - expected)
        record(
            "silver_to_gold_row_count",
            "lakehouse.gold." + target,
            observed,
            expected,
            observed == expected,
            "Source and fact row counts",
            difference,
        )
    observed, expected = amount(orders, "order_total_vnd"), amount(frames["orders"], "total_vnd")
    record(
        "silver_to_gold_order_total_vnd",
        "lakehouse.gold.fact_order",
        observed,
        expected,
        observed == expected,
        "Placed value conserved",
        int(observed != expected),
    )
    current = frames["fact_inventory_current"]
    wrong = current.filter(
        pl.col("snapshot_date").is_null()
        | pl.col("_gold_ingested_at").is_null()
        | (pl.col("snapshot_date") != pl.col("_gold_ingested_at").dt.convert_time_zone("Asia/Ho_Chi_Minh").dt.date())
    ).height
    record(
        "inventory_observation_date",
        "fact_inventory_current",
        wrong,
        0,
        wrong == 0,
        "Actual Vietnam observation date",
        wrong,
    )
    bad_rows(
        "successful_refunds_match_source",
        "fact_order",
        """WITH r AS (
        SELECT p.order_id,sum(r.amount_vnd) amount FROM refunds r JOIN payments p ON r.payment_id=p.payment_id
        WHERE r.status='succeeded' GROUP BY p.order_id)
        SELECT o.order_id FROM fact_order o LEFT JOIN r ON o.order_id=r.order_id
        WHERE o.refunded_amount_vnd<>CASE WHEN o.is_revenue_recognized THEN LEAST(o.gross_revenue_vnd,coalesce(r.amount,0)) ELSE 0 END""",
    )
    for order_col, item_col, mart_col in (
        ("gross_revenue_vnd", "gross_revenue_vnd", "gross_revenue_vnd"),
        ("refunded_amount_vnd", "refunded_amount_vnd", "refunded_amount_vnd"),
        ("net_revenue_vnd", "net_revenue_vnd", "net_revenue_vnd"),
        ("total_cost_vnd", "item_cost_vnd", "cogs_vnd"),
        ("net_profit_vnd", "item_profit_vnd", "gross_profit_vnd"),
    ):
        values = [amount(orders, order_col), amount(items, item_col)] + [
            amount(frames[name], mart_col)
            for name in ("mart_sales_daily", "mart_order_daily", "mart_product_sales_daily")
        ]
        record(
            "financial_parity_" + order_col,
            "fact_order / fact_order_item / sales marts",
            tuple(values),
            "equal",
            len(set(values)) == 1,
            "Exact integer merchandise parity",
        )
    bad_rows(
        "valid_item_financials",
        "fact_order_item",
        """SELECT order_item_id FROM fact_order_item WHERE
        unit_cost_vnd IS NULL OR item_cost_vnd IS NULL OR net_revenue_vnd IS NULL OR net_revenue_vnd<0
        OR refunded_amount_vnd<0 OR returned_quantity>quantity OR gross_revenue_vnd-refunded_amount_vnd<>net_revenue_vnd
        OR net_revenue_vnd-item_cost_vnd<>item_profit_vnd""",
    )
    source = (orders.height, int(orders["is_revenue_recognized"].sum() or 0), amount(orders, "order_total_vnd"))
    target = tuple(
        amount(frames["mart_order_daily"], col) for col in ("total_orders", "recognized_orders", "order_total_vnd")
    )
    record(
        "additive_order_mart_counts",
        "mart_order_daily",
        target,
        source,
        source == target,
        "One-to-many joins cannot multiply order totals",
    )
    for table, keys in (
        ("mart_order_daily", ["order_date", "channel", "store_key"]),
        ("mart_product_sales_daily", ["order_date", "product_id"]),
    ):
        n = (
            frames[table]
            .group_by(keys)
            .len()
            .filter((pl.col("len") > 1) | pl.any_horizontal([pl.col(key).is_null() for key in keys]))
            .height
        )
        record("serving_mart_grain", table, n, 0, n == 0, "Unique non-null serving keys", n)
    domain_checks(frames, record)
    for table, key in (
        ("fact_order", "order_id"),
        ("fact_order_item", "order_item_id"),
        ("dim_customer", "customer_key"),
        ("dim_product", "product_key"),
        ("dim_variant", "variant_key"),
    ):
        n = frames[table].group_by(key).len().filter(pl.col(key).is_null() | (pl.col("len") > 1)).height
        record("primary_key_unique_not_null", "lakehouse.gold." + table, n, 0, n == 0, "Unique non-null primary key", n)
    for key, dimension in (("product_key", "dim_product"), ("variant_key", "dim_variant")):
        n = items.filter(pl.col(key) > 0).join(frames[dimension].select(key), on=key, how="anti").height
        record("referential_integrity", "lakehouse.gold.fact_order_item", n, 0, n == 0, "No orphan " + key, n)
    for name in FRESH:
        frame = frames[name]
        n = frame.filter(pl.col("_source_run_id") == run_id).height
        passed = n > 0 or frame.height == 0
        record(
            "current_run_freshness_marker",
            "lakehouse.gold." + name,
            n,
            ">0",
            passed,
            "Current logical run marker",
            int(not passed),
        )
    for table, columns in (
        ("dim_customer", ("email", "phone", "full_name")),
        ("dim_delivery_staff", ("phone", "full_name")),
    ):
        for name in columns:
            n = frames[table].filter(pl.col(name).is_not_null() & ~pl.col(name).str.contains(r"^[0-9a-f]{64}$")).height
            record(
                "pii_pseudonymization", f"lakehouse.gold.{table}.{name}", n, 0, n == 0, "Only SHA-256 tokens or NULL", n
            )
    failed = [r for r in results if not r.passed and r.severity == "ERROR"]
    return {
        "total_checks": len(results),
        "passed_checks": len(results) - len(failed),
        "failed_checks": len(failed),
        "status": "FAILED" if failed else "PASSED",
        "results": [asdict(r) for r in results],
    }
