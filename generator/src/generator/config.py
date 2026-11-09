"""Read the self-contained YAML scenario used by CLI and Streamlit."""

import copy
import hashlib
import json
import math
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from generator import __version__
from generator.locations import LocationConfig, parse_locations
from generator.validation import validate_scale

CATEGORY_NAMES = {
    "ao": "Áo nữ",
    "dam": "Đầm",
    "vay": "Váy",
    "quan": "Quần nữ",
    "khoac": "Áo khoác",
    "phu-kien": "Phụ kiện",
    "giay": "Giày dép",
    "tui-xach": "Túi xách",
}


@dataclass(frozen=True)
class PriceBand:
    min_vnd: int
    max_vnd: int
    weight: float


@dataclass(frozen=True)
class DistributionConfig:
    business_timezone: str
    day_of_week: tuple[float, ...]
    hour_of_day: tuple[float, ...]
    campaign_hour_of_day: tuple[float, ...]
    tet: dict[str, Any]
    sales: tuple[dict[str, Any], ...]
    categories: dict[str, float]
    price_bands: tuple[PriceBand, ...]
    order_size: tuple[float, ...]
    quantity_per_item: tuple[float, ...]
    customers: dict[str, dict[str, Any]]
    coupons: dict[str, Any]
    reviews: dict[str, Any]
    outcomes: dict[str, Any]
    product_popularity: tuple[float, ...]


@dataclass(frozen=True)
class GeneratorConfig:
    scenario_id: str
    dataset_size: str
    seed: int
    anchor_time: datetime
    history_months: int
    modes: tuple[str, ...]
    scale: dict[str, int]
    distributions: DistributionConfig
    policies: dict[str, dict[str, Any]]
    locations: LocationConfig = LocationConfig()

    @property
    def logical_identity(self) -> str:
        document = asdict(self)
        document["anchor_time"] = self.anchor_time.isoformat()
        document["generator_version"] = __version__
        payload = json.dumps(document, ensure_ascii=False, sort_keys=True)
        # A rewrite has different semantics even for an older YAML scenario.
        return str(uuid.uuid5(uuid.NAMESPACE_URL, "dk-generator-v2:" + hashlib.sha256(payload.encode()).hexdigest()))

    @property
    def generation_run_id(self) -> str:
        return str(uuid.uuid5(uuid.UUID(self.logical_identity), "generation-run"))


def _weights(values, label: str, length: int | None = None) -> tuple[float, ...]:
    weights = tuple(float(value) for value in values)
    if (length is not None and len(weights) != length) or not weights:
        raise ValueError(f"{label} has invalid length")
    if any(not math.isfinite(value) or value < 0 for value in weights) or sum(weights) <= 0:
        raise ValueError(f"{label} needs finite non-negative weights with a positive sum")
    return weights


def _bounds(values, label: str, minimum: int = 0) -> tuple[int, int]:
    if not isinstance(values, (list, tuple)) or len(values) != 2:
        raise ValueError(f"{label} must contain [minimum, maximum]")
    if any(isinstance(value, bool) or not math.isfinite(value) or int(value) != value for value in values):
        raise ValueError(f"{label} needs finite integers")
    lower, upper = map(int, values)
    if lower < minimum or lower > upper:
        raise ValueError(f"{label} must satisfy {minimum} <= minimum <= maximum")
    return lower, upper


def config_from_dict(raw: dict[str, Any]) -> GeneratorConfig:
    try:
        anchor = datetime.fromisoformat(str(raw["anchor_time"]).replace("Z", "+00:00"))
        scale = {key: int(value) for key, value in raw["scale"].items()}
        validate_scale(scale)
        if anchor.tzinfo is None:
            raise ValueError("anchor_time must include timezone")
        if int(raw["history_months"]) < 1:
            raise ValueError("history_months must be positive")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", str(raw["scenario_id"])):
            raise ValueError("scenario_id must be a safe name of at most 64 characters")
        source = copy.deepcopy(raw["distributions"])
        policies = copy.deepcopy(raw["policies"])
        integer_fields = {
            "history": {"days_per_month": 1, "master_data_lead_days": 0},
            "catalog": {"price_step_vnd": 1, "variant_price_step_vnd": 0, "cost_ratio_levels": 1},
            "shipping": {
                "fee_vnd": 0,
                "free_from_subtotal_vnd": 0,
                "failed_attempts": 1,
                "attempt_interval_hours": 1,
                "failure_after_days": 1,
                "cod_block_after_failures": 1,
            },
            "timing": {
                "cancellation_min_age_hours": 1,
                "completion_min_age_days": 1,
                "confirmation_min_age_hours": 1,
                "failure_min_age_days": 1,
                "campaign_pull_days": 0,
            },
            "returns": {"quantity": 1, "request_delay_days": 0, "processing_delay_days": 0},
            "wishlist": {"minimum_purchased_items": 0},
            "coupons": {
                "start_before_history_days": 0,
                "valid_after_anchor_days": 1,
                "minimum_total_usage_limit": 1,
                "extra_total_usage_limit": 0,
                "ordinary_customer_usage_limit": 1,
                "campaign_customer_usage_limit": 1,
            },
        }
        for group, fields in integer_fields.items():
            for key, minimum in fields.items():
                value = policies[group][key]
                if isinstance(value, bool) or not math.isfinite(value) or int(value) != value or value < minimum:
                    raise ValueError(f"policies.{group}.{key} must be an integer >= {minimum}")
                policies[group][key] = int(value)
        range_fields = {
            "customers": {"created_before_history_days": 1, "inactive_before_anchor_days": 1, "house_number_range": 1},
            "timing": {
                "confirmation_delay_hours": 0,
                "completion_delay_hours": 0,
                "cancellation_delay_minutes": 1,
                "minute_of_hour": 0,
            },
            "inventory": {"buffer_units": 0},
            "reviews": {"extra_delay_hours": 0, "moderation_delay_hours": 0},
            "wishlist": {
                "item_count": 1,
                "added_before_purchase_days": 0,
                "added_before_purchase_hours": 0,
                "readded_after_days": 0,
                "unpurchased_readded_after_days": 0,
                "removed_after_days": 0,
            },
            "carts": {
                "checkout_lead_minutes": 1,
                "stale_before_anchor_days": 0,
                "recent_before_anchor_hours": 0,
                "created_before_update_hours": 0,
                "active_item_count": 1,
                "active_quantity": 1,
            },
        }
        for group, fields in range_fields.items():
            for key, minimum in fields.items():
                policies[group][key] = _bounds(policies[group][key], f"policies.{group}.{key}", minimum)
        for group, keys in {
            "customers": ("female_name_rate", "inactive_rate"),
            "catalog": ("archive_rate",),
            "wishlist": ("creation_rate", "purchased_share", "removal_rate"),
            "carts": ("stale_rate",),
        }.items():
            for key in keys:
                value = policies[group][key]
                if not math.isfinite(value) or not 0 <= value <= 1:
                    raise ValueError(f"policies.{group}.{key} must be a finite rate in 0..1")
        for key in ("cost_ratio_base", "cost_ratio_step"):
            value = policies["catalog"][key]
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"policies.catalog.{key} must be finite and non-negative")
        for key in ("item_selection", "replacement_selection"):
            if policies["returns"][key] not in {"first", "random"}:
                raise ValueError(f"policies.returns.{key} must be first or random")
        shipping, timing = policies["shipping"], policies["timing"]
        if timing["minute_of_hour"][1] > 59:
            raise ValueError("minute_of_hour must be within 0..59")
        if shipping["failed_attempts"] > 3:
            raise ValueError("failed_attempts must be <= 3, as required by the source schema")
        if shipping["failed_attempts"] * shipping["attempt_interval_hours"] > shipping["failure_after_days"] * 24:
            raise ValueError("delivery attempts must fit before failure_after_days")
        if timing["failure_min_age_days"] < shipping["failure_after_days"]:
            raise ValueError("failure_min_age_days must cover failure_after_days")
        if timing["completion_min_age_days"] * 24 < sum(
            (timing["confirmation_delay_hours"][1], timing["completion_delay_hours"][1])
        ):
            raise ValueError("completion_min_age_days must cover confirmation and completion delays")
        if policies["history"]["master_data_lead_days"] < policies["coupons"]["start_before_history_days"]:
            raise ValueError("coupon start must not precede master-data creation")
        # Preserve dev's integer normalization for money, counts and calendar fields.
        for key in ("month_start", "day_start", "month_end", "day_end"):
            source["seasonality"]["tet"][key] = int(source["seasonality"]["tet"][key])
        for event in source["seasonality"]["sales"]:
            for key in ("month", "day", "weekday", "week_index", "after_days"):
                if event.get(key) is not None:
                    event[key] = int(event[key])
        for group in source["customers"].values():
            for key in ("orders_per_year", "interval_days"):
                if key in group:
                    group[key] = [int(value) for value in group[key]]
        for band in source["price_bands"]:
            for key in ("min_vnd", "max_vnd"):
                band[key] = int(band[key])
        for key in ("percentage", "campaign_percentage", "minimum_subtotal_vnd"):
            source["coupons"][key] = int(source["coupons"][key])
        source["reviews"]["delay_days"] = [int(value) for value in source["reviews"]["delay_days"]]
        ZoneInfo(source["business_timezone"])
        tet = source["seasonality"]["tet"]
        date(2000, tet["month_start"], tet["day_start"])
        date(2000, tet["month_end"], tet["day_end"])
        if not math.isfinite(tet["peak"]) or tet["peak"] <= 1:
            raise ValueError("tet peak must be finite and greater than 1")
        for event in source["seasonality"]["sales"]:
            if event.get("day") is not None:
                date(2000, event["month"], event["day"])
            elif not (1 <= event["month"] <= 12 and 0 <= event["weekday"] <= 6 and 1 <= event["week_index"] <= 4):
                raise ValueError("sale must define a valid day or weekday/week_index")
            if not math.isfinite(event["boost"]) or event["boost"] <= 1 or event["after_days"] < 0:
                raise ValueError("sale boost must be finite and >1; after_days must be non-negative")
        classes = source["customers"]
        shares = _weights([group["share"] for group in classes.values()], "customer shares")
        if not math.isclose(sum(shares), 1.0):
            raise ValueError("customer shares must sum to 1")
        for name, group in classes.items():
            if not 0 <= group["share"] <= 1:
                raise ValueError(f"invalid share for {name}")
            low, high = group["orders_per_year"]
            if not 1 <= low <= high:
                raise ValueError(f"invalid orders_per_year for {name}")
            group["interval_days"] = _bounds(group["interval_days"], f"{name}.interval_days", 1)
            if not 0 <= group["campaign_affinity"] <= 1:
                raise ValueError(f"invalid campaign_affinity for {name}")
        bands = tuple(PriceBand(**band) for band in source["price_bands"])
        if not bands or any(not 0 <= band.min_vnd <= band.max_vnd or band.weight <= 0 for band in bands):
            raise ValueError("price_bands need valid ranges and positive weights")
        _weights([band.weight for band in bands], "price_bands")
        categories = {name: float(weight) for name, weight in source["categories"].items()}
        if set(categories) != set(CATEGORY_NAMES):
            raise ValueError("categories must match the supported retail catalog")
        _weights(categories.values(), "categories")
        coupon, reviews, outcomes = (source[key] for key in ("coupons", "reviews", "outcomes"))
        rates = [coupon["usage_rate"], coupon["campaign_usage_rate"], reviews["write_rate"]]
        rates += [
            outcomes[key]
            for key in ("cancellation_rate", "cod_rate", "cod_failure_rate", "return_rate", "exchange_rate")
        ]
        if any(not math.isfinite(value) or not 0 <= value <= 1 for value in rates):
            raise ValueError("behavior rates must be finite and in 0..1")
        if any(not 1 <= coupon[key] <= 100 for key in ("percentage", "campaign_percentage")):
            raise ValueError("coupon discount must be in 1..100")
        if coupon["minimum_subtotal_vnd"] < 0:
            raise ValueError("coupon minimum subtotal must be non-negative")
        _weights(reviews["rating_weights"], "rating_weights", 5)
        _weights(reviews["status_weights"].values(), "review status_weights")
        _weights(outcomes["cancellation_reasons"].values(), "cancellation reasons")
        if set(reviews["status_weights"]) != {"approved", "rejected"}:
            raise ValueError("review status_weights must contain approved/rejected")
        if not 0 <= reviews["delay_days"][0] <= reviews["delay_days"][1]:
            raise ValueError("invalid review delay_days")
        distribution = DistributionConfig(
            business_timezone=source["business_timezone"],
            day_of_week=_weights(source["day_of_week"], "day_of_week", 7),
            hour_of_day=_weights(source["hour_of_day"], "hour_of_day", 24),
            campaign_hour_of_day=_weights(source["campaign_hour_of_day"], "campaign_hour_of_day", 24),
            tet=dict(source["seasonality"]["tet"]),
            sales=tuple(source["seasonality"]["sales"]),
            categories=dict(categories),
            price_bands=bands,
            order_size=_weights(source["order_size"], "order_size"),
            quantity_per_item=_weights(source["quantity_per_item"], "quantity_per_item"),
            customers=classes,
            coupons=coupon,
            reviews=reviews,
            outcomes=outcomes,
            product_popularity=_weights(source["product_popularity"], "product_popularity", 10),
        )
        modes = tuple(raw["modes"])
        if set(modes) - {"seed_master", "historical_transactions", "repurchase_history", "failure_fixtures"}:
            raise ValueError("unsupported generator modes")
        return GeneratorConfig(
            scenario_id=str(raw["scenario_id"]),
            dataset_size=str(raw["dataset_size"]),
            seed=int(raw["seed"]),
            anchor_time=anchor,
            history_months=int(raw["history_months"]),
            modes=modes,
            scale=scale,
            distributions=distribution,
            policies=policies,
            locations=parse_locations(raw.get("locations", {})),
        )
    except KeyError as error:
        raise ValueError(f"missing configuration field: {error.args[0]}") from error
    except (TypeError, OverflowError) as error:
        raise ValueError("configuration fields must use their expected YAML types") from error


def load_config(path: Path) -> GeneratorConfig:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("scenario YAML must contain a mapping")
    return config_from_dict(document)
