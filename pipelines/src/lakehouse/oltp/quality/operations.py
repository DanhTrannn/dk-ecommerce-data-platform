"""Operations parity at each mart's declared observation time."""

from lakehouse.oltp.quality.gate import QualityCheckResult


def operations_consistency_checks(spark):
    summary = spark.sql("""WITH measured AS (
        SELECT operations_observed_at FROM lakehouse.gold.mart_operations_summary WHERE summary_key='current'
    ), orders AS (
        SELECT count(*) order_rows,
        sum(CASE WHEN status IN ('paid','confirmed') THEN 1 ELSE 0 END) pending,
        sum(CASE WHEN status IN ('paid','confirmed') AND created_at<=operations_observed_at-INTERVAL 2 HOURS THEN 1 ELSE 0 END) stale,
        avg(CASE WHEN status IN ('paid','confirmed') AND created_at<=operations_observed_at
            THEN (unix_timestamp(operations_observed_at)-unix_timestamp(created_at))/3600.0 END) waiting
        FROM lakehouse.gold.fact_order CROSS JOIN measured
    ), shipping AS (
        SELECT count(*) shipment_rows,sum(CASE WHEN attempt_count>1 OR status='failed' THEN 1 ELSE 0 END) problems,
        sum(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed FROM lakehouse.gold.fact_shipment
    ), returns AS (
        SELECT count(*) return_rows,sum(CASE WHEN status<>'cancelled' THEN 1 ELSE 0 END) requests
        FROM lakehouse.silver.silver_return_requests
    ), refunds AS (
        SELECT count(*) refund_rows,sum(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) refunds,
        sum(CASE WHEN status='succeeded' THEN amount_vnd ELSE 0 END) amount FROM lakehouse.silver.silver_refunds
    ) SELECT count(*) n,count_if(s.summary_key='current' AND s.order_rows=o.order_rows
        AND s.pending_orders <=> o.pending AND s.over_two_hours_orders <=> o.stale
        AND ((s.average_waiting_hours IS NULL AND o.waiting IS NULL) OR abs(s.average_waiting_hours-o.waiting)<0.000001)
        AND s.shipment_rows=h.shipment_rows AND s.problematic_shipments <=> h.problems AND s.failed_shipments <=> h.failed
        AND s.return_rows=r.return_rows AND s.return_requests_count <=> r.requests
        AND s.refund_rows=f.refund_rows AND s.successful_refunds_count <=> f.refunds
        AND s.successful_refund_amount_vnd <=> f.amount) valid
        FROM lakehouse.gold.mart_operations_summary s CROSS JOIN orders o CROSS JOIN shipping h
        CROSS JOIN returns r CROSS JOIN refunds f""").first()
    checks = [
        QualityCheckResult(
            "operations_summary_parity",
            "mart_operations_summary",
            str(tuple(summary)),
            "(1, 1)",
            summary.n == 1 and summary.valid == 1,
            "Independent source counts, successful refunds and frozen waiting time",
        )
    ]
    actual = spark.sql("""SELECT coalesce(sum(total_shipments),0) shipments,coalesce(sum(delivered_count),0) delivered,
        coalesce(sum(boom_count),0) failed,coalesce(sum(problematic_shipments),0) problems,
        coalesce(sum(on_time_eligible_count),0) eligible,coalesce(sum(on_time_count),0) on_time,
        coalesce(sum(duration_sample_count),0) samples FROM lakehouse.gold.mart_logistics_performance""").first()
    expected = spark.sql("""SELECT count(*) shipments,count_if(status='delivered') delivered,
        count_if(status='failed') failed,count_if(attempt_count>1 OR status='failed') problems,
        count_if(status='delivered' AND is_on_time IS NOT NULL) eligible,
        count_if(status='delivered' AND is_on_time=true) on_time,count(delivery_duration_minutes) samples
        FROM lakehouse.gold.fact_shipment""").first()
    checks.append(
        QualityCheckResult(
            "logistics_rollup_parity",
            "mart_logistics_performance",
            str(tuple(actual)),
            str(tuple(expected)),
            tuple(actual) == tuple(expected),
            "Unknown deadline comparisons never count as on-time observations",
        )
    )
    cod = spark.sql("""SELECT shipment_date,coalesce(shipping_region,'Khu vực khác') region,
        count(*) total_cod_shipments,sum(CASE WHEN s.status='failed' THEN 1 ELSE 0 END) failed_cod_shipments
        FROM lakehouse.gold.fact_shipment s JOIN lakehouse.gold.fact_order o ON s.order_id=o.order_id
        WHERE o.payment_method='cod' GROUP BY shipment_date,coalesce(shipping_region,'Khu vực khác')""")
    cod_target = spark.table("lakehouse.gold.mart_cod_region_daily").select(*cod.columns)
    mismatches = cod.exceptAll(cod_target).count() + cod_target.exceptAll(cod).count()
    checks.append(
        QualityCheckResult(
            "cod_region_parity",
            "mart_cod_region_daily",
            str(mismatches),
            "0",
            mismatches == 0,
            "Only COD shipments, each counted once at shipment date/region grain",
            failed_rows=mismatches,
        )
    )
    returns = spark.sql("""SELECT return_date,returned_variant_key,action_type,coalesce(reason,'Unknown') reason,
        count(DISTINCT return_id) total_return_requests,count(*) return_item_count,
        sum(returned_quantity) total_returned_quantity,
        sum(CASE WHEN return_status='completed' AND inspection_status='passed' THEN returned_quantity ELSE 0 END) accepted_return_quantity,
        sum(refund_amount_vnd) requested_refund_amount_vnd
        FROM lakehouse.gold.fact_return_exchange WHERE return_status<>'cancelled'
        GROUP BY return_date,returned_variant_key,action_type,coalesce(reason,'Unknown')""")
    return_target = spark.table("lakehouse.gold.mart_product_returns").select(*returns.columns)
    mismatches = returns.exceptAll(return_target).count() + return_target.exceptAll(returns).count()
    checks.append(
        QualityCheckResult(
            "product_return_grain_parity",
            "mart_product_returns",
            str(mismatches),
            "0",
            mismatches == 0,
            "Count requests distinctly per SKU; keep lines, requested/accepted units and requested money separate",
            failed_rows=mismatches,
        )
    )
    return checks
