"""Compare published order-mart totals with a single current MySQL observation."""

import logging
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from app.models.order import Order
from app.modules.analytics.reader import serve
from app.modules.analytics.schemas import ReconciliationVariance, SystemMetricsResponse

log = logging.getLogger(__name__)


def get_system_metrics(db=None, trino_client=None):
    def build(reader):
        row = reader.one("""SELECT SUM(total_orders) orders,SUM(order_total_vnd) order_value
            FROM lakehouse.gold.mart_order_daily""")
        source, source_at = (None, None), None
        if db is not None:
            try:
                # One statement observes count and value together. The source
                # can legitimately be ahead of the last completed batch.
                count, value = db.execute(
                    select(func.count(Order.order_id), func.coalesce(func.sum(Order.total_vnd), 0))
                ).one()
                source, source_at = (int(count), int(value)), datetime.now(UTC).isoformat()
            except SQLAlchemyError:
                log.warning("Current MySQL reconciliation observation unavailable")
        comparisons = []
        for name, actual, published in [
            ("total_orders", source[0], row.get("orders")),
            ("order_total_vnd", source[1], row.get("order_value")),
        ]:
            difference = abs(actual - published) if actual is not None and published is not None else None
            comparisons.append(
                ReconciliationVariance(
                    metric_name=name,
                    oltp_value=actual,
                    lakehouse_value=published,
                    absolute_difference=difference,
                    variance_percent=difference * 100.0 / actual if difference is not None and actual > 0 else None,
                )
            )
        complete = all(c.absolute_difference is not None for c in comparisons)
        status = (
            ("mismatch" if any(c.absolute_difference for c in comparisons) else "matched")
            if complete
            else "unavailable"
        )
        return SystemMetricsResponse(
            source_mode=reader.publication.get("source_mode"),
            reconciliation_status=status,
            source_observed_at=source_at,
            reconciliation_variance=comparisons,
            is_degraded=not complete,
            degraded_reason="Chưa đủ hai phía để đối soát; giữ các giá trị đọc được." if not complete else None,
        )

    result = serve(("system",), build, trino_client)
    published = datetime.fromisoformat(result.data_updated_at.replace(" UTC", "+00:00"))
    age = max(0, int((datetime.now(UTC) - published).total_seconds() / 60))
    updates = {"publication_age_minutes": age}
    if result.served_from_cache:
        updates["reconciliation_status"] = "cached"
    return result.model_copy(update=updates)
