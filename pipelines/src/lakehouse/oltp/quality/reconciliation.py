"""Post-Gold SQL reconciliation using Trino Iceberg time-travel against pinned snapshots."""

from typing import Any

from lakehouse.oltp.quality.contracts import QualityCheckResult
from lakehouse.utils.namespaces import namespace as ns

REQUIRED_SNAPSHOT_KEYS = (
    "silver.silver_orders",
    "gold.fact_order",
    "gold.mart_order_daily",
)


def validate_snapshots_present(prefix: str, snapshots: dict[str, Any]) -> dict[str, int | None]:
    """Ensure all required snapshot keys are present with integer snapshot IDs."""
    resolved = {}
    for key_suffix in REQUIRED_SNAPSHOT_KEYS:
        layer, table = key_suffix.split(".")
        full_key = f"lakehouse.{ns(prefix, layer)}.{table}"
        if full_key not in snapshots:
            raise KeyError(f"Missing required snapshot for reconciliation: {full_key}")
        snap_id = snapshots[full_key]
        if snap_id is not None and (type(snap_id) is not int or snap_id <= 0):
            raise ValueError(f"Snapshot ID for {full_key} must be a positive integer")
        resolved[key_suffix] = snap_id
    return resolved


def build_reconciliation_query(prefix: str, snapshots: dict[str, Any]) -> str:
    """Build single combined Trino SQL query pinning Iceberg snapshots."""
    pinned = validate_snapshots_present(prefix, snapshots)
    silver_ns = ns(prefix, "silver")
    gold_ns = ns(prefix, "gold")

    def relation(namespace, table, snapshot):
        name = f'"lakehouse_etl"."{namespace}"."{table}"'
        return f"{name} FOR VERSION AS OF {snapshot}" if snapshot is not None else f"(SELECT * FROM {name} WHERE false)"

    silver_orders = relation(silver_ns, "silver_orders", pinned["silver.silver_orders"])
    fact_orders = relation(gold_ns, "fact_order", pinned["gold.fact_order"])
    daily_orders = relation(gold_ns, "mart_order_daily", pinned["gold.mart_order_daily"])

    return f"""WITH silver_orders_snap AS (
    SELECT count(*) AS cnt, coalesce(sum(cast(total_vnd as bigint)), 0) AS total_vnd
    FROM {silver_orders}
),
fact_order_snap AS (
    SELECT count(*) AS cnt, coalesce(sum(cast(order_total_vnd as bigint)), 0) AS total_vnd
    FROM {fact_orders}
),
mart_grain_snap AS (
    SELECT count(*) AS bad_count FROM (
        SELECT order_date, channel, store_key
        FROM {daily_orders}
        GROUP BY order_date, channel, store_key
        HAVING count(*) > 1 OR order_date IS NULL OR channel IS NULL OR store_key IS NULL
    )
)
SELECT
    s.cnt AS silver_order_count,
    f.cnt AS gold_order_count,
    s.total_vnd AS silver_order_total,
    f.total_vnd AS gold_order_total,
    g.bad_count AS mart_grain_bad_count
FROM silver_orders_snap s
CROSS JOIN fact_order_snap f
CROSS JOIN mart_grain_snap g"""


def run_snapshot_reconciliation(cursor, prefix: str, snapshots: dict[str, Any]) -> list[QualityCheckResult]:
    """Execute Trino reconciliation query and return standardized QualityCheckResults."""
    sql = build_reconciliation_query(prefix, snapshots)
    cursor.execute(sql)
    rows = cursor.fetchall()
    if not rows:
        raise ValueError("Reconciliation query returned no rows")

    silver_cnt, gold_cnt, silver_total, gold_total, bad_grain = rows[0]
    silver_cnt = int(silver_cnt or 0)
    gold_cnt = int(gold_cnt or 0)
    silver_total = int(silver_total or 0)
    gold_total = int(gold_total or 0)
    bad_grain = int(bad_grain or 0)

    results = []

    # 1. Row count parity: silver_orders vs fact_order
    count_diff = abs(gold_cnt - silver_cnt)
    count_passed = (gold_cnt == silver_cnt)
    variance = 0.0
    if not count_passed:
        variance = 100.0 if silver_cnt == 0 else round(count_diff / abs(silver_cnt) * 100, 4)
    results.append(
        QualityCheckResult(
            check_name="silver_to_gold_row_count",
            dataset="lakehouse.gold.fact_order",
            observed_value=str(gold_cnt),
            expected_value=str(silver_cnt),
            passed=count_passed,
            details="Source and fact row counts",
            failed_rows=count_diff,
            variance_percent=variance,
        )
    )

    # 2. Financial conservation: total_vnd vs order_total_vnd (Exact 0 tolerance)
    money_passed = (gold_total == silver_total)
    results.append(
        QualityCheckResult(
            check_name="silver_to_gold_order_total_vnd",
            dataset="lakehouse.gold.fact_order",
            observed_value=str(gold_total),
            expected_value=str(silver_total),
            passed=money_passed,
            details="Placed value conserved",
            failed_rows=int(not money_passed),
        )
    )

    # 3. Serving mart grain: mart_order_daily (order_date, channel, store_key)
    grain_passed = (bad_grain == 0)
    results.append(
        QualityCheckResult(
            check_name="serving_mart_grain",
            dataset="mart_order_daily",
            observed_value=str(bad_grain),
            expected_value="0",
            passed=grain_passed,
            details="Unique non-null serving keys",
            failed_rows=bad_grain,
        )
    )

    return results
