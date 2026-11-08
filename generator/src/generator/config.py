"""Read the self-contained YAML scenario used by CLI and Streamlit."""

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
    cancellations: dict[str, Any]
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
        source = raw["distributions"]
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
            if high > 1:
                start, end = group["interval_days"]
                if not 1 <= start <= end:
                    raise ValueError(f"invalid interval_days for {name}")
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
        coupon, reviews, cancellations = (source[key] for key in ("coupons", "reviews", "cancellations"))
        for label, mapping in (
            ("coupon multipliers", coupon["customer_multipliers"]),
            ("review rates", reviews["completed_order_rates"]),
            ("cancellation addons", cancellations["customer_addons"]),
        ):
            if set(mapping) != set(classes):
                raise ValueError(f"{label} must match customer classes")
        rates = [
            coupon[key]
            for key in ("base_usage_rate", "campaign_usage_rate", "midnight_usage_rate", "first_order_usage_rate")
        ]
        rates += list(reviews["completed_order_rates"].values())
        rates += [cancellations["base_rate"], cancellations["campaign_rate"]]
        if any(not math.isfinite(value) or not 0 <= value <= 1 for value in rates):
            raise ValueError("behavior rates must be in 0..1")
        _weights(coupon["customer_multipliers"].values(), "coupon multipliers")
        for key in ("campaign_percentage_values", "midnight_fixed_values_vnd"):
            values = _weights(coupon[key], key)
            if min(values) <= 0 or (key == "campaign_percentage_values" and max(values) > 100):
                raise ValueError(f"invalid {key}")
        if not 1 <= coupon["everyday_percentage"] <= 100 or coupon["welcome_fixed_vnd"] <= 0:
            raise ValueError("invalid everyday/welcome coupon discount")
        if min(coupon["everyday_minimum_subtotal_vnd"], coupon["campaign_minimum_subtotal_vnd"]) < 0:
            raise ValueError("coupon minimum subtotal must be non-negative")
        if (
            not math.isfinite(cancellations["coupon_addon"])
            or cancellations["coupon_addon"] < 0
            or any(not math.isfinite(value) for value in cancellations["customer_addons"].values())
        ):
            raise ValueError("invalid cancellation addons")
        _weights(reviews["rating_weights"], "rating_weights", 5)
        _weights(reviews["status_weights"].values(), "review status_weights")
        _weights(cancellations["reasons"].values(), "cancellation reasons")
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
            cancellations=cancellations,
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
        )
    except KeyError as error:
        raise ValueError(f"missing configuration field: {error.args[0]}") from error
    except TypeError as error:
        raise ValueError("configuration fields must use their expected YAML types") from error


def load_config(path: Path) -> GeneratorConfig:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError("scenario YAML must contain a mapping")
    return config_from_dict(document)
