"""History window, daily order intensity, customer batches and id allocation."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from datagen.presets import OltpPreset

CUSTOMERS_PER_BATCH = 50_000  # fixed: changing it changes which stream each customer draws from


class IdAllocator:
    """Hands out contiguous primary keys per table, in the order batches are generated."""

    def __init__(self, next_ids: dict[str, int]):
        self._next = dict(next_ids)

    def take(self, table: str, count: int) -> np.ndarray:
        start = self._next[table]
        self._next[table] = start + count
        return np.arange(start, start + count, dtype=np.int64)


@dataclass(frozen=True)
class Timeline:
    history_start: datetime
    anchor: datetime
    local_date: np.ndarray  # datetime64[D], business-timezone calendar day
    day_start: np.ndarray  # datetime64[us], that day's local midnight expressed in UTC
    day_weight: np.ndarray  # share of orders placed on each day, sums to 1
    is_sale_day: np.ndarray
    yearmonth: np.ndarray  # e.g. 202507, of the local day


def _nth_weekday(year: int, month: int, weekday: int, index: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (index - 1))


def sale_event_dates(preset: OltpPreset) -> dict[date, str]:
    zone = ZoneInfo(preset.business_timezone)
    first = preset.history_start.astimezone(zone).date()
    last = preset.anchor_time.astimezone(zone).date()
    events = {}
    for year in range(first.year, last.year + 1):
        for event in preset.calendar.sale_events.dates:
            if event.day is not None:
                day = date(year, event.month, event.day)
            else:
                day = _nth_weekday(year, event.month, event.weekday, event.week_index)
            events[day + timedelta(days=event.offset_days)] = event.name
    return events


def _day_multiplier(day: date, sales: set[date], preset: OltpPreset) -> float:
    calendar = preset.calendar
    if day in sales:
        return calendar.sale_events.event_day_multiplier
    if any(0 < (day - sale).days <= calendar.sale_events.after_days for sale in sales):
        return calendar.sale_events.after_multiplier
    for tet in calendar.tet.dates:
        if 0 <= (day - tet).days < calendar.tet.holiday_days:
            return calendar.tet.holiday_multiplier
        if 0 < (tet - day).days <= calendar.tet.before_days:
            return calendar.tet.before_multiplier
    return 1.0


def build_timeline(preset: OltpPreset) -> Timeline:
    """Local days lying wholly inside [history_start, anchor), each weighted by its order intensity."""
    zone = ZoneInfo(preset.business_timezone)
    first_local = preset.history_start.astimezone(zone)
    day = first_local.date() if first_local.time() == datetime.min.time() else first_local.date() + timedelta(days=1)
    days = []
    while datetime.combine(day + timedelta(days=1), datetime.min.time(), zone) <= preset.anchor_time:
        days.append(day)
        day += timedelta(days=1)
    sales = set(sale_event_dates(preset))
    weights = np.array(
        [preset.calendar.day_of_week.weights[d.weekday()] * _day_multiplier(d, sales, preset) for d in days]
    )
    starts = [
        datetime.combine(d, datetime.min.time(), zone).astimezone(ZoneInfo("UTC")).replace(tzinfo=None) for d in days
    ]
    return Timeline(
        history_start=preset.history_start,
        anchor=preset.anchor_time,
        local_date=np.array(days, dtype="datetime64[D]"),
        day_start=np.array(starts, dtype="datetime64[us]"),
        day_weight=weights / weights.sum(),
        is_sale_day=np.array([d in sales for d in days]),
        yearmonth=np.array([d.year * 100 + d.month for d in days]),
    )


def yearmonth_of(preset: OltpPreset, timestamps: pl.Series) -> pl.Series:
    local = timestamps.dt.convert_time_zone(preset.business_timezone)
    return (local.dt.year() * 100 + local.dt.month()).cast(pl.Int32)


@dataclass(frozen=True)
class CustomerBatch:
    index: int
    customer_count: int
    buyer_count: int


def customer_batches(preset: OltpPreset) -> list[CustomerBatch]:
    buyers = preset.scale.buying_customers
    total = buyers + round(buyers * preset.scale.registered_without_order_ratio)
    batches, done, buyers_done = [], 0, 0
    while done < total:
        count = min(CUSTOMERS_PER_BATCH, total - done)
        done += count
        # Cumulative rounding keeps the batch buyer counts summing exactly to buying_customers.
        buyers_through = round(done * buyers / total)
        batches.append(CustomerBatch(len(batches), count, buyers_through - buyers_done))
        buyers_done = buyers_through
    return batches


MICROS_PER_SECOND = 1_000_000
MICROS_PER_MINUTE = 60 * MICROS_PER_SECOND
MICROS_PER_HOUR = 60 * MICROS_PER_MINUTE
MICROS_PER_DAY = 24 * MICROS_PER_HOUR


def utc_micros(moment: datetime) -> np.int64:
    """A timezone-aware datetime as microseconds since the Unix epoch."""
    return np.datetime64(moment.astimezone(ZoneInfo("UTC")).replace(tzinfo=None), "us").astype(np.int64)


def utc_series(name: str, micros: np.ndarray) -> pl.Series:
    """Epoch microseconds as a UTC datetime column; NaN-free int64 input, use mask_null for missing values."""
    return pl.Series(name, np.asarray(micros, dtype=np.int64)).cast(pl.Datetime("us", "UTC"))
