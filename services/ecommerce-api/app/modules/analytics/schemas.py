"""Pydantic schemas for published analytics and role-specific signals."""

from pydantic import BaseModel, ConfigDict, Field


# -----------------------------------------------------------------------------
# Signal schemas (serving sources are defined by the analytical services)
# -----------------------------------------------------------------------------

class InventoryStockAlert(BaseModel):
    """Published warehouse SKU balance; the query selects the alert threshold."""
    variant_id: int
    sku: str
    product_name: str
    on_hand: int


class InventoryHistoryPoint(BaseModel):
    date: str
    warehouse_stock_units: int | None
    store_stock_units: int | None
    total_inventory_value_vnd: int


class CODRegionMetric(BaseModel):
    region: str
    total_cod_shipments: int
    failed_cod_shipments: int
    failure_rate_percent: float | None


class FulfillmentMetrics(BaseModel):
    pending_orders: int
    over_two_hours_orders: int
    average_waiting_hours: float | None


class LogisticsPerformanceMetric(BaseModel):
    staff_id: int | None
    staff_code: str
    city_id: int
    city_name: str
    total_shipments: int
    delivered_count: int
    failed_count: int
    on_time_eligible_count: int
    on_time_rate_percent: float | None
    average_transit_minutes: float | None
    cod_collected_vnd: int


class ProductReturnMetric(BaseModel):
    variant_id: int
    sku: str
    product_name: str
    request_count: int
    line_count: int
    requested_units: int
    accepted_units: int
    requested_refund_vnd: int


class StoreStockoutSignal(BaseModel):
    """Published SKU balance with zero on-hand at a specific store."""
    variant_id: int
    sku: str
    product_name: str
    on_hand: int = 0


class HourlyRunRatePoint(BaseModel):
    """Hourly sales pace vs target point for POS stores."""
    hour: str
    hourly_revenue_vnd: int = 0
    cumulative_revenue_vnd: int = 0
    target_vnd: int = 0


class StoreRunRateSignal(BaseModel):
    """Store intra-day sales pacing vs target."""
    daily_target_vnd: int = 0
    current_revenue_vnd: int = 0
    achievement_percent: float = 0.0
    projected_revenue_vnd: int = 0
    pace_status: str = "on_track"
    hourly_points: list[HourlyRunRatePoint] = Field(default_factory=list)


class ViralProductSignal(BaseModel):
    """Real-time surge in order item sales velocity (unexpected best-seller)."""
    product_id: int
    product_name: str
    units_sold_recent: int = 0
    growth_velocity_multiple: float = 1.0
    viral_badge: bool = False


class ChannelPaceComparisonSignal(BaseModel):
    """Pace comparison between Online and physical Store channels."""
    online_growth_percent: float = 0.0
    store_growth_percent: float = 0.0
    dominant_channel: str = "balanced"
    pace_divergence_warning: bool = False


class HourlyMarginPoint(BaseModel):
    """Hourly gross profit margin point during the day."""
    hour: str
    revenue_vnd: int = 0
    cogs_vnd: int = 0
    margin_percent: float = 0.0


class MarginErosionSignal(BaseModel):
    """Intra-day gross margin erosion tracking to prevent selling at a loss."""
    baseline_margin_percent: float = 0.0
    current_margin_percent: float = 0.0
    erosion_drop_percent: float = 0.0
    erosion_warning: bool = False
    lowest_margin_hour: str = ""
    hourly_margins: list[HourlyMarginPoint] = Field(default_factory=list)


class CashInTransitSignal(BaseModel):
    """Published COD order value attached to active shipments, not collected cash."""
    total_cod_amount_vnd: int = 0
    dispatched_shipments_count: int = 0
    carrier_breakdown: dict[str, int] = Field(default_factory=dict)


# -----------------------------------------------------------------------------
# Role Metrics Responses (6 Business Roles + 1 System Governance Role)
# -----------------------------------------------------------------------------

class AnalyticsDataStatus(BaseModel):
    """Expose the validated Lakehouse publication and bounded cached-result age."""

    data_source: str = "lakehouse"
    is_degraded: bool = False
    degraded_reason: str | None = None
    source_run_id: str | None = None
    data_updated_at: str | None = None
    served_from_cache: bool = False
    cache_age_seconds: int = 0


class ExecutiveMetricsResponse(AnalyticsDataStatus):
    model_config = ConfigDict(from_attributes=True)

    role: str = "executive"
    gmv_vnd: int | None = None
    net_revenue_vnd: int | None = None
    cogs_vnd: int | None = None
    gross_profit_vnd: int | None = None
    gross_margin_percent: float | None = None
    total_orders: int | None = None
    aov_vnd: int | None = None
    boom_rate_percent: float | None = None
    return_rate_percent: float | None = None
    margin_erosion: MarginErosionSignal | None = None
    cash_in_transit: CashInTransitSignal | None = None


class StoreContribution(BaseModel):
    store_id: int | None = None
    store_name: str
    revenue_vnd: int = 0
    order_count: int = 0


class TopProductMetric(BaseModel):
    product_id: int
    product_name: str
    units_sold: int = 0
    revenue_vnd: int = 0


class CategoryShareMetric(BaseModel):
    category_id: int
    category_name: str
    revenue_vnd: int = 0
    share_percent: float | None = None


class SalesMetricsResponse(AnalyticsDataStatus):
    model_config = ConfigDict(from_attributes=True)

    role: str = "sales"
    store_contributions: list[StoreContribution] = Field(default_factory=list)
    top_selling_products: list[TopProductMetric] = Field(default_factory=list)
    category_shares: list[CategoryShareMetric] = Field(default_factory=list)
    viral_products: list[ViralProductSignal] = Field(default_factory=list)
    channel_pace: ChannelPaceComparisonSignal | None = None


class FunnelStep(BaseModel):
    step_name: str
    count: int
    conversion_rate_percent: float | None = None


class MarketingSourceStatus(AnalyticsDataStatus):
    available: bool = False
    observed_at: str | None = None


class CouponUsageMetric(BaseModel):
    coupon_id: int
    coupon_code: str
    used_count: int
    usage_limit: int | None
    usage_percent: float | None
    recent_redemptions_count: int
    recent_redemptions_per_minute: float


class NegativeReviewMetric(BaseModel):
    product_id: int
    product_name: str
    negative_count: int


class ProductDemandMetric(BaseModel):
    product_key: str
    product_id: int | None
    product_name: str | None
    detail_views: int
    cart_sets: int
    wishlist_adds: int
    cart_to_view_rate_percent: float | None


class MarketingMetricsResponse(AnalyticsDataStatus):
    role: str = "marketing"
    log_status: MarketingSourceStatus = Field(default_factory=MarketingSourceStatus)
    commerce_status: MarketingSourceStatus = Field(default_factory=MarketingSourceStatus)
    funnel_steps: list[FunnelStep] = Field(default_factory=list)
    view_events_count: int | None = None
    checkout_success_events_count: int | None = None
    checkout_to_view_rate_percent: float | None = None
    funnel_window_start: str | None = None
    funnel_window_end: str | None = None
    product_demand: list[ProductDemandMetric] = Field(default_factory=list)
    coupon_source_rows: int | None = None
    review_source_rows: int | None = None
    coupons: list[CouponUsageMetric] = Field(default_factory=list)
    negative_reviews: list[NegativeReviewMetric] = Field(default_factory=list)


class StoreMetricsResponse(AnalyticsDataStatus):
    model_config = ConfigDict(from_attributes=True)

    role: str = "store"
    store_id: int | None = None
    store_name: str | None = None
    store_revenue_today_vnd: int | None = None
    store_orders_count: int | None = None
    target_achievement_percent: float | None = None
    low_stock_at_store_count: int | None = None
    stockout_at_store_count: int | None = None
    inventory_observed_at: str | None = None
    store_stockouts: list[StoreStockoutSignal] = Field(default_factory=list)
    run_rate: StoreRunRateSignal | None = None


class StoreItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    store_id: int
    name: str
    code: str


class InventoryMetricsResponse(AnalyticsDataStatus):
    model_config = ConfigDict(from_attributes=True)

    role: str = "inventory"
    total_inventory_value_vnd: int | None = None
    warehouse_stock_units: int | None = None
    store_stock_units: int | None = None
    inbound_batches_count: int | None = None
    stockout_count: int | None = None
    inventory_observed_at: str | None = None
    inventory_history: list[InventoryHistoryPoint] = Field(default_factory=list)
    low_stock_alerts: list[InventoryStockAlert] = Field(default_factory=list)
    critical_stockout_alerts: list[InventoryStockAlert] = Field(default_factory=list)


class OperationsMetricsResponse(AnalyticsDataStatus):
    role: str = "operations"
    pending_fulfillment_count: int | None = None
    problematic_shipments_count: int | None = None
    failed_shipments_count: int | None = None
    return_requests_count: int | None = None
    successful_refunds_count: int | None = None
    successful_refund_amount_vnd: int | None = None
    operations_observed_at: str | None = None
    fulfillment: FulfillmentMetrics | None = None
    cod_regions: list[CODRegionMetric] = Field(default_factory=list)
    logistics: list[LogisticsPerformanceMetric] = Field(default_factory=list)
    product_returns: list[ProductReturnMetric] = Field(default_factory=list)


class ReconciliationVariance(BaseModel):
    metric_name: str
    oltp_value: int | None
    lakehouse_value: int | None
    absolute_difference: int | None
    variance_percent: float | None


class SystemMetricsResponse(AnalyticsDataStatus):
    role: str = "system"
    source_mode: str | None = None
    publication_age_minutes: int | None = None
    reconciliation_status: str = "unavailable"
    source_observed_at: str | None = None
    reconciliation_variance: list[ReconciliationVariance] = Field(default_factory=list)


class DailySalesTrendPoint(BaseModel):
    date: str
    revenue_vnd: int = 0
    cogs_vnd: int = 0
    profit_vnd: int = 0
    orders_count: int = 0


class SalesTrendResponse(AnalyticsDataStatus):
    days: int = 30
    points: list[DailySalesTrendPoint] = Field(default_factory=list)


RoleMetricsResponse = (
    ExecutiveMetricsResponse
    | SalesMetricsResponse
    | MarketingMetricsResponse
    | StoreMetricsResponse
    | InventoryMetricsResponse
    | OperationsMetricsResponse
    | SystemMetricsResponse
)
