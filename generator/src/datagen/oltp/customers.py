"""Customers of one batch, with the number of orders each buyer will place."""

import numpy as np
import polars as pl

from datagen.distributions import log_series_theta
from datagen.oltp.plan import MICROS_PER_DAY, CustomerBatch, IdAllocator, utc_micros, utc_series
from datagen.oltp.reference import Reference
from datagen.presets import OltpPreset
from datagen.randomness import uuid4_strings

FAMILY_NAMES = ("Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô")
FEMALE_MIDDLE = ("Thị", "Ngọc", "Thu", "Minh", "Thanh", "Phương", "Bảo")
MALE_MIDDLE = ("Văn", "Hữu", "Đức", "Minh", "Quốc", "Thành", "Gia")
FEMALE_GIVEN = ("Anh", "Trang", "Linh", "Hương", "Lan", "Mai", "Ngân", "Vy", "Hà", "Thảo", "Nhi", "Yến", "Chi", "Hạnh")
MALE_GIVEN = ("Huy", "Nam", "Khoa", "Long", "Tuấn", "Phúc", "Dũng", "Hùng", "Quân", "Bảo", "Hiếu", "Sơn", "Tài")


def _names(rng: np.random.Generator, count: int, female_rate: float) -> pl.Series:
    female = rng.random(count) < female_rate
    parts = pl.DataFrame(
        {
            "family": rng.choice(FAMILY_NAMES, count),
            "middle": np.where(female, rng.choice(FEMALE_MIDDLE, count), rng.choice(MALE_MIDDLE, count)),
            "given": np.where(female, rng.choice(FEMALE_GIVEN, count), rng.choice(MALE_GIVEN, count)),
        }
    )
    return parts.select(pl.concat_str("family", "middle", "given", separator=" ")).to_series()


def close_after_activity(customers: pl.DataFrame, activity: pl.DataFrame) -> pl.DataFrame:
    """Move an inactive customer's closing time (updated_at) past their last activity when that came later.

    activity has one row per event (customer_id, at): orders and what follows them, carts, wishlist items.
    """
    last = activity.group_by("customer_id").agg(pl.col("at").max().alias("last_activity"))
    return (
        customers.join(last, on="customer_id", how="left", maintain_order="left")
        .with_columns(
            updated_at=pl.when(pl.col("status") == "inactive")
            .then(pl.max_horizontal("updated_at", "last_activity"))
            .otherwise(pl.col("updated_at"))
        )
        .drop("last_activity")
    )


def generate_customers(
    preset: OltpPreset, reference: Reference, rng: np.random.Generator, ids: IdAllocator, batch: CustomerBatch
) -> pl.DataFrame:
    """Customer rows plus is_buyer and order_count (not conformed; COD counters are filled after orders)."""
    count = batch.customer_count
    policy = preset.policies.customers
    is_buyer = np.zeros(count, dtype=bool)
    is_buyer[rng.choice(count, size=batch.buyer_count, replace=False)] = True
    theta = log_series_theta(preset.customers.purchase_frequency.alpha_years, preset.history_years)
    order_count = np.where(is_buyer, rng.logseries(theta, count), 0)

    cities = reference.cities
    city_id = rng.choice(cities["city_id"].to_numpy(), size=count, p=cities["share"].to_numpy())
    created_low, created_high = policy.created_before_history_days
    created_at = (
        utc_micros(preset.history_start)
        - rng.integers(created_low, created_high + 1, count) * MICROS_PER_DAY
        - rng.integers(0, MICROS_PER_DAY, count)
    )
    inactive = rng.random(count) < policy.inactive_rate
    inactive_low, inactive_high = policy.inactive_before_anchor_days
    inactive_at = (
        utc_micros(preset.anchor_time)
        - rng.integers(inactive_low, inactive_high + 1, count) * MICROS_PER_DAY
        - rng.integers(0, MICROS_PER_DAY, count)
    )

    return pl.DataFrame(
        {
            "customer_id": ids.take("customers", count),
            "public_id": uuid4_strings(rng, count),
            "role": "customer",
            "display_name": _names(rng, count, policy.female_name_rate),
            "status": np.where(inactive, "inactive", "active"),
            "city_id": city_id,
            "is_cod_blocked": False,
            "boom_count": 0,
            "data_origin": "synthetic",
            "generation_run_id": preset.run_id,
            "created_at": utc_series("created_at", created_at),
            "updated_at": utc_series("updated_at", np.where(inactive, inactive_at, created_at)),
            "is_buyer": is_buyer,
            "order_count": order_count.astype(np.int64),
        }
    ).with_columns(store_id=pl.lit(None, dtype=pl.Int64))
