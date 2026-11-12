"""Typed, validated OLTP presets read from generator/oltp/configs/*.yml.

Field names match the YAML keys one to one, so a preset key is found in code by its name.
"""

from datetime import UTC, date, datetime
from functools import cached_property
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator

CONFIG_DIR = Path(__file__).resolve().parents[2] / "oltp" / "configs"
SHARE_TOLERANCE = 1e-6


def _ordered(value):
    if value[0] > value[1]:
        raise ValueError(f"range [{value[0]}, {value[1]}] is reversed")
    return value


IntRange = Annotated[tuple[int, int], AfterValidator(_ordered)]
FloatRange = Annotated[tuple[float, float], AfterValidator(_ordered)]
Rate = Annotated[float, Field(ge=0, le=1)]
Positive = Annotated[int, Field(gt=0)]
NonNegative = Annotated[int, Field(ge=0)]
Source = Literal["measured", "transfer", "assumption"]
FrequencyTable = Annotated[list[float], Field(min_length=3, max_length=3)]
StarTable = Annotated[list[float], Field(min_length=5, max_length=5)]


def _hour_profile(values):
    if len(values) != 24:
        raise ValueError(f"needs 24 hourly weights, got {len(values)}")
    if abs(sum(values) - 1) > 0.01:
        raise ValueError(f"hourly weights sum to {sum(values):.4f}, expected 1")
    return values


HourProfile = Annotated[list[float], AfterValidator(_hour_profile)]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Scale(Strict):
    buying_customers: Positive
    registered_without_order_ratio: Annotated[float, Field(ge=0)]
    products: Positive
    stores: Positive


class PurchaseFrequency(Strict):
    model: Literal["zero_truncated_log_series"]
    alpha_years: Annotated[float, Field(gt=0)]
    source: Source


class RepeatGap(Strict):
    model: Literal["exponential"]
    shrink_c: Annotated[float, Field(ge=0, lt=1)]
    source: Source


class CustomerRules(Strict):
    purchase_frequency: PurchaseFrequency
    repeat_gap: RepeatGap


class Popularity(Strict):
    model: Literal["lognormal"]
    sigma: Annotated[float, Field(gt=0)]
    source: Source


class CategoryShare(Strict):
    product_share: Rate
    demand_multiplier: Annotated[float, Field(gt=0)]


class PriceBand(Strict):
    min_vnd: Positive
    max_vnd: Positive
    weight: Positive


class PriceBands(Strict):
    source: Source
    bands: Annotated[list[PriceBand], Field(min_length=1)]


class ProductRules(Strict):
    popularity: Popularity
    categories: dict[str, CategoryShare]
    price_independent_of_popularity: bool
    variants_per_product: Positive
    price_bands: PriceBands

    @field_validator("categories", mode="before")
    @classmethod
    def _drop_source_label(cls, value):
        # The YAML block carries its provenance label next to the category codes.
        return {code: rule for code, rule in value.items() if code != "source"}

    @field_validator("categories")
    @classmethod
    def _shares_sum_to_one(cls, value):
        total = sum(rule.product_share for rule in value.values())
        if abs(total - 1) > 0.001:
            raise ValueError(f"product_share sums to {total:.4f}, expected 1")
        return value


class BasketProducts(Strict):
    model: Literal["geometric"]
    p: Annotated[float, Field(gt=0, le=1)]


class Baskets(Strict):
    source: Source
    products: BasketProducts
    variants_per_product: FrequencyTable
    quantity_per_variant: FrequencyTable


class DayOfWeek(Strict):
    source: Source
    weights: Annotated[list[float], Field(min_length=7, max_length=7)]


class HourOfDay(Strict):
    source: Source
    normal: HourProfile
    sale_day: HourProfile


class SaleDate(Strict):
    name: str
    month: Annotated[int, Field(ge=1, le=12)]
    day: Annotated[int, Field(ge=1, le=31)] | None = None
    weekday: Annotated[int, Field(ge=0, le=6)] | None = None
    week_index: Annotated[int, Field(ge=1, le=5)] | None = None
    offset_days: int = 0

    @model_validator(mode="after")
    def _day_or_weekday(self):
        if (self.day is None) == (self.weekday is None or self.week_index is None):
            raise ValueError(f"sale date {self.name} needs either day or weekday + week_index")
        return self


class SaleEvents(Strict):
    source: Source
    event_day_multiplier: Annotated[float, Field(gt=0)]
    after_days: NonNegative
    after_multiplier: Annotated[float, Field(gt=0)]
    dates: list[SaleDate]


class Tet(Strict):
    source: Source
    dates: list[date]
    before_days: NonNegative
    before_multiplier: Annotated[float, Field(gt=0)]
    holiday_days: NonNegative
    holiday_multiplier: Annotated[float, Field(gt=0)]


class Calendar(Strict):
    day_of_week: DayOfWeek
    hour_of_day: HourOfDay
    sale_events: SaleEvents
    tet: Tet


class LateBucket(Strict):
    days: IntRange
    share: Annotated[float, Field(ge=0)]


class Fulfilment(Strict):
    on_time_delivery_days: IntRange
    late_rate: Rate
    late_buckets: dict[Literal["late_1_3", "late_4_7", "late_8_plus"], LateBucket]


class RatingByLateness(Strict):
    source: Source
    on_time: StarTable
    late_1_3: StarTable
    late_4_7: StarTable
    late_8_plus: StarTable


class ReviewRules(Strict):
    rating_by_lateness: RatingByLateness
    write_rate: Rate
    status_weights: dict[Literal["approved", "rejected"], NonNegative]
    delay_days: IntRange


class Outcomes(Strict):
    source: Source
    cancellation_rate: Rate
    cod_rate: Rate
    cod_failure_rate: Rate
    return_rate: Rate
    exchange_rate: Rate
    cancellation_reasons: dict[str, Positive]


class CouponRules(Strict):
    source: Source
    usage_rate: Rate
    sale_day_usage_rate: Rate
    percentage: Annotated[int, Field(ge=1, le=100)]
    sale_day_percentage: Annotated[int, Field(ge=1, le=100)]
    minimum_subtotal_vnd: NonNegative


class City(Strict):
    code: str
    name: str
    share: Rate


class Locations(Strict):
    source: Source
    cities: Annotated[list[City], Field(min_length=1)]

    @field_validator("cities")
    @classmethod
    def _shares_sum_to_one(cls, value):
        total = sum(city.share for city in value)
        if abs(total - 1) > SHARE_TOLERANCE:
            raise ValueError(f"city shares sum to {total:.6f}, expected 1")
        return value


def _within_day(hours):
    if hours[0] < 0 or hours[1] > 24 or hours[0] == hours[1]:
        raise ValueError(f"opening hours {hours} must be a non-empty range inside [0, 24]")
    return hours


class Pos(Strict):
    source: Source
    orders_per_online_order: Annotated[float, Field(ge=0)]
    walk_in_rate: Rate
    payment_methods: dict[Literal["cash", "card", "vietqr"], NonNegative]
    opening_hours: Annotated[IntRange, AfterValidator(_within_day)]
    staff_per_store: Positive


class HistoryPolicy(Strict):
    master_data_lead_days: NonNegative


class CustomerPolicy(Strict):
    house_number_range: IntRange
    female_name_rate: Rate
    inactive_rate: Rate
    created_before_history_days: IntRange
    inactive_before_anchor_days: IntRange


class CatalogPolicy(Strict):
    archive_rate: Rate
    price_step_vnd: Positive
    cost_ratio_range: FloatRange


class ShippingPolicy(Strict):
    fee_vnd: NonNegative
    free_from_subtotal_vnd: NonNegative
    failed_attempts: Annotated[int, Field(ge=1, le=3)]
    attempt_interval_hours: NonNegative
    failure_after_days: NonNegative
    cod_block_after_failures: Positive
    delivery_staff: Positive


class TimingPolicy(Strict):
    confirmation_delay_hours: IntRange
    cancellation_delay_minutes: IntRange
    minute_of_hour: IntRange
    payment_delay_minutes: IntRange
    dispatch_delay_hours: IntRange
    completion_delay_hours: IntRange


class ReturnPolicy(Strict):
    item_selection: Literal["uniform"]
    replacement_selection: Literal["uniform"]
    quantity: Literal[1]
    request_delay_days: NonNegative
    processing_delay_days: NonNegative


class InventoryPolicy(Strict):
    buffer_units: IntRange
    store_buffer_units: IntRange


class CouponPolicy(Strict):
    start_before_history_days: NonNegative
    valid_after_anchor_days: Positive
    minimum_total_usage_limit: Positive
    extra_total_usage_limit: NonNegative
    ordinary_customer_usage_limit: Positive
    sale_day_customer_usage_limit: Positive


class ReviewPolicy(Strict):
    extra_delay_hours: IntRange
    moderation_delay_hours: IntRange


class WishlistPolicy(Strict):
    creation_rate: Rate
    item_count: IntRange
    purchased_share: Rate
    minimum_purchased_items: NonNegative
    added_before_purchase_days: IntRange
    added_before_purchase_hours: IntRange
    readded_after_days: IntRange
    unpurchased_readded_after_days: IntRange
    removal_rate: Rate
    removed_after_days: IntRange


class CartPolicy(Strict):
    active_cart_rate: Rate
    checkout_lead_minutes: IntRange
    stale_rate: Rate
    stale_before_anchor_days: IntRange
    recent_before_anchor_hours: IntRange
    created_before_update_hours: IntRange
    active_item_count: IntRange
    active_quantity: IntRange


class Policies(Strict):
    history: HistoryPolicy
    customers: CustomerPolicy
    catalog: CatalogPolicy
    shipping: ShippingPolicy
    timing: TimingPolicy
    returns: ReturnPolicy
    inventory: InventoryPolicy
    coupons: CouponPolicy
    reviews: ReviewPolicy
    wishlist: WishlistPolicy
    carts: CartPolicy


class OltpPreset(Strict):
    schema_version: Literal[3]
    scenario_id: str
    seed: NonNegative
    anchor_time: datetime
    history_months: Annotated[int, Field(ge=1, le=120)]
    business_timezone: str
    scale: Scale
    customers: CustomerRules
    products: ProductRules
    baskets: Baskets
    calendar: Calendar
    fulfilment: Fulfilment
    reviews: ReviewRules
    outcomes: Outcomes
    coupons: CouponRules
    locations: Locations
    pos: Pos
    policies: Policies

    @field_validator("anchor_time")
    @classmethod
    def _anchor_has_timezone(cls, value):
        if value.tzinfo is None:
            raise ValueError("anchor_time must include a timezone")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _tet_inside_history(self):
        for day in self.calendar.tet.dates:
            if not self.history_start.date() <= day < self.anchor_time.date():
                raise ValueError(f"calendar.tet.dates: {day} is outside the history window")
        return self

    @cached_property
    def history_start(self) -> datetime:
        months = self.anchor_time.year * 12 + self.anchor_time.month - 1 - self.history_months
        return self.anchor_time.replace(year=months // 12, month=months % 12 + 1)

    @property
    def history_years(self) -> float:
        return self.history_months / 12

    @property
    def run_id(self) -> str:
        return f"{self.scenario_id}:{self.seed}"


def preset_path(name: str) -> Path:
    return CONFIG_DIR / f"{name}.yml"


def load_oltp_preset(path: Path) -> OltpPreset:
    return OltpPreset.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
