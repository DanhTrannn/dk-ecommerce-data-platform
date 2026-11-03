"""Pydantic schemas for published analytics and role-specific signals."""

from pydantic import BaseModel, ConfigDict, Field


# -----------------------------------------------------------------------------
# Signal schemas (serving sources are defined by the analytical services)
# -----------------------------------------------------------------------------

class VoucherBurnRateSignal(BaseModel):
    """Real-time signal tracking flash sale coupon usage velocity and budget consumption."""
    coupon_code: str
    used_count: int = 0
    usage_limit: int = 0
    burn_rate_per_minute: float = 0.0
    budget_warning: bool = False
    budget_burn_percent: float = 0.0


class NegativeReviewSpikeSignal(BaseModel):
    """Real-time alert detecting sudden bursts of 1-2 star product reviews."""
    product_id: int
    product_name: str
    negative_count: int = 0
    window_minutes: int = 60
    warning_alert: str = ""


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


class RegionalBoomRateSignal(BaseModel):
    """Real-time alert on delivery failure (COD boom) spikes grouped by district/region."""
    region: str
    total_cod_shipments: int = 0
    failed_cod_shipments: int = 0
    boom_rate_percent: float = 0.0
    alert_level: str = "normal"


class FulfillmentBottleneckSignal(BaseModel):
    """Real-time operational bottleneck in packing and dispatching paid orders."""
    paid_unfulfilled_orders: int = 0
    stale_unfulfilled_orders: int = 0
    bottleneck_warning: bool = False
    average_waiting_hours: float = 0.0


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
    conversion_rate_percent: float = 0.0


class MarketingMetricsResponse(AnalyticsDataStatus):
    model_config = ConfigDict(from_attributes=True)

    role: str = "marketing"
    funnel_steps: list[FunnelStep] = Field(default_factory=list)
    conversion_rate_percent: float = 0.0
    total_visitors: int = 0
    total_purchases: int = 0
    funnel_window_days: int = 30
    voucher_burn_rate: VoucherBurnRateSignal | None = None
    negative_review_spikes: list[NegativeReviewSpikeSignal] = Field(default_factory=list)


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
    model_config = ConfigDict(from_attributes=True)

    role: str = "operations"
    pending_fulfillment_count: int = 0
    shipping_sla_violations_count: int = 0
    boom_orders_count: int = 0
    return_requests_count: int = 0
    regional_boom_rates: list[RegionalBoomRateSignal] = Field(default_factory=list)
    fulfillment_bottleneck: FulfillmentBottleneckSignal | None = None


class ReconciliationVariance(BaseModel):
    metric_name: str
    oltp_value: float
    lakehouse_value: float
    variance_percent: float


class SystemMetricsResponse(AnalyticsDataStatus):
    model_config = ConfigDict(from_attributes=True)

    role: str = "system"
    pipeline_status: str = "healthy"
    data_freshness_minutes: int | None = None
    data_freshness_sla_minutes: int = 15
    reconciliation_variance: list[ReconciliationVariance] = Field(default_factory=list)
    cdc_apply_status: str = "unknown"
    cdc_heartbeat_at: str | None = None
    cdc_pending_events: int = 0
    cdc_apply_lag_seconds: int = 0


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
