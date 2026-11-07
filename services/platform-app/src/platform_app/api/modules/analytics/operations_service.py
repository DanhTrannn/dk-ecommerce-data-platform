"""Operations serving at the mart's observation time, never wall-clock aging."""

from datetime import UTC, datetime

from platform_app.api.core.errors import INTERNAL_ERROR, AppError
from platform_app.api.modules.analytics.reader import serve
from platform_app.api.modules.analytics.schemas import (
    CODRegionMetric,
    FulfillmentMetrics,
    LogisticsPerformanceMetric,
    OperationsMetricsResponse,
    ProductReturnMetric,
)


def get_operations_metrics(db=None, trino_client=None):
    def build(reader):
        summary = reader.one("SELECT * FROM lakehouse.gold.mart_operations_summary WHERE summary_key='current'")
        if not summary:
            raise AppError(INTERNAL_ERROR, "Mart vận hành chưa được xây dựng.", status_code=503)
        regions = reader.query("""SELECT region,SUM(total_cod_shipments) total_cod_shipments,
            SUM(failed_cod_shipments) failed_cod_shipments,
            100.0*SUM(failed_cod_shipments)/NULLIF(SUM(total_cod_shipments),0) failure_rate_percent
            FROM lakehouse.gold.mart_cod_region_daily GROUP BY region
            ORDER BY failed_cod_shipments DESC,region""")
        logistics = reader.query("""SELECT staff_key staff_id,staff_code,
            assigned_city_id city_id,assigned_city_name city_name,
            SUM(total_shipments) total_shipments,SUM(delivered_count) delivered_count,
            SUM(boom_count) failed_count,SUM(on_time_eligible_count) on_time_eligible_count,
            100.0*SUM(on_time_count)/NULLIF(SUM(on_time_eligible_count),0) on_time_rate_percent,
            SUM(duration_minutes_sum)/NULLIF(SUM(duration_sample_count),0) average_transit_minutes,
            SUM(total_cod_collected_vnd) cod_collected_vnd
            FROM lakehouse.gold.mart_logistics_performance GROUP BY 1,2,3,4
            ORDER BY total_shipments DESC,staff_id,city_id""")
        products = reader.query("""SELECT returned_variant_key variant_id,sku,product_name,
            SUM(total_return_requests) request_count,SUM(return_item_count) line_count,
            SUM(total_returned_quantity) requested_units,SUM(accepted_return_quantity) accepted_units,
            SUM(requested_refund_amount_vnd) requested_refund_vnd
            FROM lakehouse.gold.mart_product_returns GROUP BY 1,2,3
            ORDER BY requested_units DESC,variant_id LIMIT 10""")
        observed = datetime.fromisoformat(str(summary["operations_observed_at"]).replace(" UTC", "+00:00"))
        observed = observed.replace(tzinfo=UTC) if observed.tzinfo is None else observed.astimezone(UTC)
        return OperationsMetricsResponse(
            pending_fulfillment_count=summary["pending_orders"],
            problematic_shipments_count=summary["problematic_shipments"],
            failed_shipments_count=summary["failed_shipments"],
            return_requests_count=summary["return_requests_count"],
            successful_refunds_count=summary["successful_refunds_count"],
            successful_refund_amount_vnd=summary["successful_refund_amount_vnd"],
            operations_observed_at=observed.isoformat(),
            fulfillment=FulfillmentMetrics(
                pending_orders=summary["pending_orders"],
                over_two_hours_orders=summary["over_two_hours_orders"],
                average_waiting_hours=summary["average_waiting_hours"],
            )
            if summary["order_rows"]
            else None,
            cod_regions=[CODRegionMetric(**row) for row in regions],
            logistics=[LogisticsPerformanceMetric(**row) for row in logistics],
            product_returns=[ProductReturnMetric(**row) for row in products],
        )

    return serve(("operations",), build, trino_client)
