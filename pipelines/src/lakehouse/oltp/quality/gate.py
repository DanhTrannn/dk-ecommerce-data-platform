"""Persistent data-quality audit and post-Gold publication gates."""

from __future__ import annotations

from lakehouse.utils.sql import identifier, render_sql


import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

try:
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover - unit tests without Spark
    F = None  # type: ignore


QUALITY_RESULTS_TABLE = "lakehouse.system.data_quality_results"

QUALITY_RESULTS_DDL = render_sql(
    "spark/system/quality_results.sql", quality_results_table=identifier(QUALITY_RESULTS_TABLE)
)


from lakehouse.oltp.quality.contracts import GoldQualityGateError, QualityCheckResult


def ensure_quality_results_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.system")
    spark.sql(QUALITY_RESULTS_DDL)


def _result_payload(result: Any) -> dict[str, Any]:
    if isinstance(result, QualityCheckResult):
        return asdict(result)
    return {
        "check_name": getattr(result, "metric_name", "unknown_check"),
        "dataset": getattr(result, "table_name", "unknown_dataset"),
        "observed_value": str(getattr(result, "silver_value", "")),
        "expected_value": str(getattr(result, "source_value", "")),
        "passed": bool(getattr(result, "passed", False)),
        "details": getattr(result, "details", ""),
        "severity": getattr(result, "severity", "ERROR"),
        "variance_percent": float(getattr(result, "variance_percent", 0.0)),
        "failed_rows": int(getattr(result, "failed_rows", 0)),
    }


def _sql_string(value: Any) -> str:
    """Quote a value as a Spark SQL string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def persist_quality_results(
    spark: SparkSession,
    results: Iterable[Any],
    *,
    run_id: str,
    batch_date: str,
    phase: str,
) -> None:
    """Append quality evidence before the gate succeeds or raises."""
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for result in results:
        payload = _result_payload(result)
        rows.append(
            render_sql(
                "spark/system/quality_result_row.sql",
                result_id=_sql_string(uuid.uuid4().hex),
                run_id=_sql_string(run_id),
                batch_date=_sql_string(batch_date),
                phase=_sql_string(phase),
                check_name=_sql_string(payload["check_name"]),
                dataset=_sql_string(payload["dataset"]),
                severity=_sql_string(payload["severity"]),
                observed=_sql_string(payload["observed_value"]),
                expected=_sql_string(payload["expected_value"]),
                variance=str(float(payload["variance_percent"])),
                failed_rows=str(int(payload["failed_rows"])),
                status=_sql_string("PASSED" if payload["passed"] else "FAILED"),
                details=_sql_string(payload["details"]),
                checked_at=_sql_string(checked_at.strftime("%Y-%m-%d %H:%M:%S.%f")),
            ).strip()
        )

    if not rows:
        return

    ensure_quality_results_table(spark)
    # SQL VALUES executes fully in the JVM. That avoids starting Python workers
    # merely to persist a few audit rows, and keeps the gate portable across a
    # Spark cluster whose worker image may use a different Python minor version.
    spark.sql(
        render_sql(
            "spark/system/insert_quality_results.sql",
            quality_results_table=identifier(QUALITY_RESULTS_TABLE),
            rows=", ".join(rows),
        )
    )


def _exact_count_check(
    check_name: str,
    source_dataset: str,
    source_count: int,
    target_dataset: str,
    target_count: int,
) -> QualityCheckResult:
    passed = source_count == target_count
    return QualityCheckResult(
        check_name=check_name,
        dataset=target_dataset,
        observed_value=str(target_count),
        expected_value=str(source_count),
        passed=passed,
        variance_percent=(
            0.0
            if source_count == target_count
            else (100.0 if source_count == 0 else round(abs(source_count - target_count) / source_count * 100, 4))
        ),
        failed_rows=abs(source_count - target_count),
        details=(
            f"{source_dataset} and {target_dataset} counts match"
            if passed
            else f"count mismatch: {source_dataset}={source_count}, {target_dataset}={target_count}"
        ),
    )


def _sum_as_int(df: DataFrame, column_name: str) -> int:
    value = df.select(F.coalesce(F.sum(F.col(column_name)), F.lit(0)).alias("value")).first()["value"]
    return int(value or 0)


def run_gold_quality_gate(
    spark: SparkSession,
    *,
    run_id: str,
    batch_date: str,
    persist_results: bool = True,
) -> dict[str, Any]:
    """Validate Gold publication completeness, keys, references, PII, and run freshness."""
    silver_orders = spark.table("lakehouse.silver.silver_orders")
    silver_order_items = spark.table("lakehouse.silver.silver_order_items")
    fact_order = spark.table("lakehouse.gold.fact_order")
    fact_order_item = spark.table("lakehouse.gold.fact_order_item")

    results: list[QualityCheckResult] = [
        _exact_count_check(
            "silver_to_gold_row_count",
            "lakehouse.silver.silver_orders",
            silver_orders.count(),
            "lakehouse.gold.fact_order",
            fact_order.count(),
        ),
        _exact_count_check(
            "silver_to_gold_row_count",
            "lakehouse.silver.silver_order_items",
            silver_order_items.count(),
            "lakehouse.gold.fact_order_item",
            fact_order_item.count(),
        ),
    ]

    silver_revenue = _sum_as_int(silver_orders, "total_vnd")
    gold_revenue = _sum_as_int(fact_order, "order_total_vnd")
    results.append(
        QualityCheckResult(
            check_name="silver_to_gold_order_total_vnd",
            dataset="lakehouse.gold.fact_order",
            observed_value=str(gold_revenue),
            expected_value=str(silver_revenue),
            passed=gold_revenue == silver_revenue,
            failed_rows=0 if gold_revenue == silver_revenue else 1,
            details=f"silver={silver_revenue} VND, gold={gold_revenue} VND",
        )
    )

    results.extend(financial_consistency_checks(spark))
    from lakehouse.oltp.quality.inventory import inventory_consistency_checks

    results.extend(inventory_consistency_checks(spark))
    from lakehouse.oltp.quality.operations import operations_consistency_checks

    results.extend(operations_consistency_checks(spark))
    from lakehouse.oltp.quality.marketing import marketing_consistency_checks

    results.extend(marketing_consistency_checks(spark))
    from lakehouse.oltp.quality.admin import admin_consistency_checks

    results.extend(admin_consistency_checks(spark))

    for table_name, primary_key in (
        ("lakehouse.gold.fact_order", "order_id"),
        ("lakehouse.gold.fact_order_item", "order_item_id"),
        ("lakehouse.gold.dim_customer", "customer_key"),
        ("lakehouse.gold.dim_product", "product_key"),
        ("lakehouse.gold.dim_variant", "variant_key"),
    ):
        df = spark.table(table_name)
        duplicate_rows = (
            df.groupBy(primary_key).count().filter(F.col(primary_key).isNull() | (F.col("count") > 1)).count()
        )
        results.append(
            QualityCheckResult(
                check_name="primary_key_unique_not_null",
                dataset=table_name,
                observed_value=str(duplicate_rows),
                expected_value="0",
                passed=duplicate_rows == 0,
                failed_rows=duplicate_rows,
                details=f"duplicate or null {primary_key} groups={duplicate_rows}",
            )
        )

    dim_product = spark.table("lakehouse.gold.dim_product").select("product_key")
    dim_variant = spark.table("lakehouse.gold.dim_variant").select("variant_key")
    orphan_products = (
        fact_order_item.filter(F.col("product_key") > 0).join(dim_product, "product_key", "left_anti").count()
    )
    orphan_variants = (
        fact_order_item.filter(F.col("variant_key") > 0).join(dim_variant, "variant_key", "left_anti").count()
    )
    for key_name, orphan_count in (
        ("product_key", orphan_products),
        ("variant_key", orphan_variants),
    ):
        results.append(
            QualityCheckResult(
                check_name="referential_integrity",
                dataset="lakehouse.gold.fact_order_item",
                observed_value=str(orphan_count),
                expected_value="0",
                passed=orphan_count == 0,
                failed_rows=orphan_count,
                details=f"orphan {key_name} rows={orphan_count}",
            )
        )

    for table_name in (
        "lakehouse.gold.fact_order",
        "lakehouse.gold.fact_order_item",
        "lakehouse.gold.mart_sales_daily",
        "lakehouse.gold.mart_order_daily",
        "lakehouse.gold.mart_product_sales_daily",
        "lakehouse.gold.mart_inventory_current",
        "lakehouse.gold.mart_inventory_summary",
        "lakehouse.gold.mart_store_overview",
        "lakehouse.gold.mart_operations_summary",
        "lakehouse.gold.mart_cod_region_daily",
        "lakehouse.gold.mart_marketing_summary",
        "lakehouse.gold.mart_coupon_usage",
        "lakehouse.gold.mart_product_review_summary",
        "lakehouse.gold.mart_admin_overview",
    ):
        current_run_rows = spark.table(table_name).filter(F.col("_source_run_id") == run_id).count()
        results.append(
            QualityCheckResult(
                check_name="current_run_freshness_marker",
                dataset=table_name,
                observed_value=str(current_run_rows),
                expected_value=">0",
                passed=current_run_rows > 0 or spark.table(table_name).limit(1).count() == 0,
                failed_rows=0 if current_run_rows > 0 or spark.table(table_name).limit(1).count() == 0 else 1,
                details=f"rows published by run {run_id}={current_run_rows}",
            )
        )

    hash_pattern = r"^[0-9a-f]{64}$"
    for table_name, columns in (
        ("lakehouse.gold.dim_customer", ("email", "phone", "full_name")),
        ("lakehouse.gold.dim_delivery_staff", ("phone", "full_name")),
    ):
        df = spark.table(table_name)
        for column_name in columns:
            invalid = df.filter(F.col(column_name).isNotNull() & ~F.col(column_name).rlike(hash_pattern)).count()
            results.append(
                QualityCheckResult(
                    check_name="pii_pseudonymization",
                    dataset=f"{table_name}.{column_name}",
                    observed_value=str(invalid),
                    expected_value="0",
                    passed=invalid == 0,
                    failed_rows=invalid,
                    details=f"non-null values not matching SHA-256 token format={invalid}",
                )
            )

    if persist_results:
        persist_quality_results(
            spark,
            results,
            run_id=run_id,
            batch_date=batch_date,
            phase="post_gold",
        )

    failed = [result for result in results if not result.passed and result.severity == "ERROR"]
    summary = {
        "total_checks": len(results),
        "passed_checks": len(results) - len(failed),
        "failed_checks": len(failed),
        "status": "PASSED" if not failed else "FAILED",
        "results": [asdict(result) for result in results],
    }
    if failed:
        messages = "; ".join(f"{item.dataset}.{item.check_name}: {item.details}" for item in failed)
        raise GoldQualityGateError(f"Gold quality gate failed with {len(failed)} violation(s): {messages}")
    return summary


def financial_consistency_checks(spark):
    """Conserve integer merchandise amounts across item, order and category grain."""
    checks = []
    wrong_inventory_date = (
        spark.sql("""SELECT COUNT(*) n FROM lakehouse.gold.fact_inventory_current
        WHERE snapshot_date IS NULL OR _gold_ingested_at IS NULL
        OR snapshot_date <> to_date(from_utc_timestamp(_gold_ingested_at,'Asia/Ho_Chi_Minh'))""")
        .first()
        .n
    )
    checks.append(
        QualityCheckResult(
            "inventory_observation_date",
            "fact_inventory_current",
            str(wrong_inventory_date),
            "0",
            wrong_inventory_date == 0,
            "Current balances must use the actual Vietnam observation date, not a backfill interval",
            failed_rows=wrong_inventory_date,
        )
    )
    missing_refunds = (
        spark.sql("""SELECT COUNT(*) n FROM lakehouse.gold.fact_order o
        LEFT JOIN (
            SELECT p.order_id,SUM(r.amount_vnd) amount
            FROM lakehouse.silver.silver_refunds r
            JOIN lakehouse.silver.silver_payments p ON r.payment_id=p.payment_id
            WHERE r.status='succeeded' GROUP BY p.order_id
        ) r ON o.order_id=r.order_id
        WHERE o.refunded_amount_vnd <> CASE WHEN o.is_revenue_recognized
            THEN LEAST(o.gross_revenue_vnd,COALESCE(r.amount,0)) ELSE 0 END""")
        .first()
        .n
    )
    checks.append(
        QualityCheckResult(
            "successful_refunds_match_source",
            "fact_order",
            str(missing_refunds),
            "0",
            missing_refunds == 0,
            "Successful merchandise refunds must not be lost because return-line metadata is incomplete",
            failed_rows=missing_refunds,
        )
    )
    for order_column, item_column, mart_column in (
        ("gross_revenue_vnd", "gross_revenue_vnd", "gross_revenue_vnd"),
        ("refunded_amount_vnd", "refunded_amount_vnd", "refunded_amount_vnd"),
        ("net_revenue_vnd", "net_revenue_vnd", "net_revenue_vnd"),
        ("total_cost_vnd", "item_cost_vnd", "cogs_vnd"),
        ("net_profit_vnd", "item_profit_vnd", "gross_profit_vnd"),
    ):
        row = spark.sql(f"""SELECT
            (SELECT COALESCE(SUM({order_column}),0) FROM lakehouse.gold.fact_order) orders,
            (SELECT COALESCE(SUM({item_column}),0) FROM lakehouse.gold.fact_order_item) items,
            (SELECT COALESCE(SUM({mart_column}),0) FROM lakehouse.gold.mart_sales_daily) marts,
            (SELECT COALESCE(SUM({mart_column}),0) FROM lakehouse.gold.mart_order_daily) order_mart,
            (SELECT COALESCE(SUM({mart_column}),0) FROM lakehouse.gold.mart_product_sales_daily) product_mart""").first()
        checks.append(
            QualityCheckResult(
                f"financial_parity_{order_column}",
                "fact_order / fact_order_item / sales marts",
                str(tuple(row)),
                "equal",
                len(set(row)) == 1,
                "Same recognition, discount allocation and successful-refund definition at every grain",
            )
        )
    invalid = (
        spark.sql("""SELECT count(*) n FROM lakehouse.gold.fact_order_item
        WHERE unit_cost_vnd IS NULL OR item_cost_vnd IS NULL OR net_revenue_vnd IS NULL
           OR net_revenue_vnd<0 OR refunded_amount_vnd<0 OR returned_quantity>quantity
           OR gross_revenue_vnd-refunded_amount_vnd<>net_revenue_vnd
           OR net_revenue_vnd-item_cost_vnd<>item_profit_vnd""")
        .first()
        .n
    )
    checks.append(
        QualityCheckResult(
            "valid_item_financials",
            "fact_order_item",
            str(invalid),
            "0",
            invalid == 0,
            "No missing historical cost, negative revenue, over-return or inconsistent profit",
            failed_rows=invalid,
        )
    )
    row = spark.sql("""SELECT
        (SELECT COUNT(*) FROM lakehouse.gold.fact_order) source_orders,
        (SELECT COALESCE(SUM(total_orders),0) FROM lakehouse.gold.mart_order_daily) mart_orders,
        (SELECT COUNT_IF(is_revenue_recognized) FROM lakehouse.gold.fact_order) source_recognized,
        (SELECT COALESCE(SUM(recognized_orders),0) FROM lakehouse.gold.mart_order_daily) mart_recognized,
        (SELECT COALESCE(SUM(order_total_vnd),0) FROM lakehouse.gold.fact_order) source_total,
        (SELECT COALESCE(SUM(order_total_vnd),0) FROM lakehouse.gold.mart_order_daily) mart_total
    """).first()
    checks.append(
        QualityCheckResult(
            "additive_order_mart_counts",
            "mart_order_daily",
            str(tuple(row)),
            "source = mart",
            row.source_orders == row.mart_orders
            and row.source_recognized == row.mart_recognized
            and row.source_total == row.mart_total,
            "Order counts and placed value must not multiply across categories, shipments or returns",
        )
    )
    for table, keys in (
        ("mart_order_daily", ["order_date", "channel", "store_key"]),
        ("mart_product_sales_daily", ["order_date", "product_id"]),
    ):
        grouped = spark.table(f"lakehouse.gold.{table}").groupBy(*keys).count()
        invalid_keys = grouped.filter("count > 1 OR " + " OR ".join(f"{key} IS NULL" for key in keys)).count()
        checks.append(
            QualityCheckResult(
                "serving_mart_grain",
                table,
                str(invalid_keys),
                "0",
                invalid_keys == 0,
                "Serving keys must be unique and non-null",
                failed_rows=invalid_keys,
            )
        )
    return checks
