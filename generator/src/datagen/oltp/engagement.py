"""Shopping carts and wishlists around the generated orders."""

import numpy as np
import polars as pl

from datagen.oltp.plan import MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE, IdAllocator, utc_micros, utc_series
from datagen.oltp.reference import Reference
from datagen.oltp.tables import conform
from datagen.presets import OltpPreset
from datagen.randomness import uuid4_strings


def _uniform(rng: np.random.Generator, bounds: tuple[int, int], size: int) -> np.ndarray:
    return rng.integers(bounds[0], bounds[1] + 1, size)


def _micros(column: str) -> pl.Expr:
    return pl.col(column).dt.epoch("us")


def _random_products(reference: Reference, rng: np.random.Generator, owners: np.ndarray, counts: np.ndarray):
    """Up to counts[i] distinct products per owner, drawn by popularity (duplicates are dropped, not redrawn)."""
    products = reference.products
    drawn = rng.choice(products["product_id"].to_numpy(), size=int(counts.sum()), p=products["weight"].to_numpy())
    return pl.DataFrame({"owner": np.repeat(owners, counts), "product_id": drawn}).unique(maintain_order=True)


def cart_tables(
    preset: OltpPreset,
    lifecycle: pl.DataFrame,
    items: pl.DataFrame,
    customers: pl.DataFrame,
    reference: Reference,
    rng: np.random.Generator,
    ids: IdAllocator,
) -> tuple[dict[str, pl.DataFrame], pl.DataFrame]:
    """A checked-out cart behind every online order, plus open carts for some customers."""
    policy = preset.policies.carts
    online = lifecycle.filter(pl.col("channel") == "online").sort("order_id")
    joined = customers.select("customer_id", pl.col("created_at").alias("joined_at"))
    checked = online.select("order_id", "customer_id", "created_at").join(
        joined, on="customer_id", maintain_order="left"
    )
    lead = _uniform(rng, policy.checkout_lead_minutes, checked.height) * MICROS_PER_MINUTE
    checked = checked.with_columns(
        cart_id=pl.Series(ids.take("carts", checked.height)),
        public_id=uuid4_strings(rng, checked.height),
        opened_at=pl.max_horizontal(_micros("created_at") - pl.Series(lead), _micros("joined_at")),
    ).with_columns(opened_at=pl.col("opened_at").cast(pl.Datetime("us", "UTC")))
    checked_carts = checked.select(
        "cart_id",
        "public_id",
        "customer_id",
        pl.lit("checked_out").alias("status"),
        pl.col("opened_at").alias("created_at"),
        pl.col("created_at").alias("updated_at"),
        pl.col("created_at").alias("checked_out_at"),
    )
    checked_items = items.join(
        checked.select("order_id", "cart_id", "opened_at", "created_at"), on="order_id", maintain_order="left"
    ).select(
        "cart_id",
        "variant_id",
        "quantity",
        pl.col("opened_at").alias("first_added_at"),
        pl.col("created_at").alias("updated_at"),
    )

    owners = customers.filter(pl.Series(rng.random(customers.height) < policy.active_cart_rate))
    count = owners.height
    anchor = utc_micros(preset.anchor_time)
    stale = rng.random(count) < policy.stale_rate
    updated = np.where(
        stale,
        anchor - _uniform(rng, policy.stale_before_anchor_days, count) * MICROS_PER_DAY,
        anchor - _uniform(rng, policy.recent_before_anchor_hours, count) * MICROS_PER_HOUR,
    )
    opened = updated - _uniform(rng, policy.created_before_update_hours, count) * MICROS_PER_HOUR
    cart_ids = ids.take("carts", count)
    active_carts = (
        pl.DataFrame(
            {
                "cart_id": cart_ids,
                "public_id": uuid4_strings(rng, count),
                "customer_id": owners["customer_id"],
                "created_at": utc_series("created_at", opened),
                "updated_at": utc_series("updated_at", updated),
            }
        )
        # Constants are added as literals: a scalar in the constructor loses every column type when no cart exists.
        .with_columns(status=pl.lit("active"), checked_out_at=pl.lit(None, dtype=pl.Datetime("us", "UTC")))
        .select(checked_carts.columns)
    )
    picks = _random_products(reference, rng, cart_ids, _uniform(rng, policy.active_item_count, count))
    variant = picks.join(reference.variants.select("product_id", "variant_id"), on="product_id", maintain_order="left")
    variant = variant.with_columns(key=pl.Series(rng.random(variant.height)))
    variant = variant.sort("owner", "product_id", "key").group_by("owner", "product_id", maintain_order=True).first()
    active_items = variant.join(
        active_carts.select("cart_id", "created_at", "updated_at"),
        left_on="owner",
        right_on="cart_id",
        maintain_order="left",
    ).select(
        pl.col("owner").alias("cart_id"),
        "variant_id",
        pl.Series("quantity", _uniform(rng, policy.active_quantity, variant.height)),
        pl.col("created_at").alias("first_added_at"),
        "updated_at",
    )

    carts = pl.concat([checked_carts, active_carts])
    cart_items = pl.concat([checked_items, active_items])
    cart_items = cart_items.with_columns(
        cart_item_id=pl.Series(ids.take("cart_items", cart_items.height)), is_present=pl.lit(True)
    )
    tables = {
        "carts": conform("carts", carts),
        "cart_items": conform("cart_items", cart_items, null_columns=("removed_at",)),
    }
    return tables, checked.select("order_id", "cart_id")


def wishlist_table(
    preset: OltpPreset,
    lifecycle: pl.DataFrame,
    items: pl.DataFrame,
    customers: pl.DataFrame,
    reference: Reference,
    rng: np.random.Generator,
    ids: IdAllocator,
) -> pl.DataFrame:
    """Wishlists mixing products the customer later bought with products they never bought."""
    policy = preset.policies.wishlist
    owners = customers.filter(pl.Series(rng.random(customers.height) < policy.creation_rate)).select("customer_id")
    owners = owners.with_columns(size=pl.Series(_uniform(rng, policy.item_count, owners.height)))
    owners = owners.with_columns(
        wanted_bought=pl.max_horizontal(
            (pl.col("size") * policy.purchased_share).round().cast(pl.Int64), pl.lit(policy.minimum_purchased_items)
        )
    )

    first_bought = (
        items.join(lifecycle.select("order_id", "customer_id", "created_at"), on="order_id", maintain_order="left")
        .group_by("customer_id", "product_id")
        .agg(pl.col("created_at").min().alias("bought_at"))
        .join(owners, on="customer_id", maintain_order="left")
        .sort("customer_id", "product_id")
    )
    first_bought = first_bought.with_columns(key=pl.Series(rng.random(first_bought.height)))
    bought = first_bought.filter(
        pl.int_range(pl.len()).over("customer_id", order_by="key") < pl.min_horizontal("wanted_bought", "size")
    )
    taken = bought.group_by("customer_id").len().rename({"len": "taken"})
    rest = owners.join(taken, on="customer_id", how="left", maintain_order="left").with_columns(
        rest=pl.col("size") - pl.col("taken").fill_null(0)
    )
    rest = rest.filter(pl.col("rest") > 0)
    others = _random_products(reference, rng, rest["customer_id"].to_numpy(), rest["rest"].to_numpy()).rename(
        {"owner": "customer_id"}
    )
    others = others.join(
        bought.select("customer_id", "product_id"), on=["customer_id", "product_id"], how="anti", maintain_order="left"
    )

    anchor = utc_micros(preset.anchor_time)
    start = utc_micros(preset.history_start)
    bought_first = (
        bought["bought_at"].dt.epoch("us").to_numpy()
        - _uniform(rng, policy.added_before_purchase_days, bought.height) * MICROS_PER_DAY
        - _uniform(rng, policy.added_before_purchase_hours, bought.height) * MICROS_PER_HOUR
    )
    bought_last = bought_first + _uniform(rng, policy.readded_after_days, bought.height) * MICROS_PER_DAY
    other_first = rng.integers(start, anchor, others.height)
    other_last = other_first + _uniform(rng, policy.unpurchased_readded_after_days, others.height) * MICROS_PER_DAY

    rows = pl.concat(
        [
            bought.select("customer_id", "product_id").with_columns(
                first=pl.Series(bought_first), last=pl.Series(bought_last)
            ),
            others.with_columns(first=pl.Series(other_first), last=pl.Series(other_last)),
        ]
    ).sort("customer_id", "product_id")
    count = rows.height
    first = rows["first"].to_numpy()
    last = np.minimum(rows["last"].to_numpy(), anchor)
    removed = rng.random(count) < policy.removal_rate
    removed_at = np.minimum(last + _uniform(rng, policy.removed_after_days, count) * MICROS_PER_DAY, anchor)
    removed_series = utc_series("removed_at", removed_at)
    return conform(
        "wishlist_items",
        rows.select("customer_id", "product_id").with_columns(
            wishlist_item_id=pl.Series(ids.take("wishlist_items", count)),
            is_present=pl.Series(~removed),
            first_added_at=utc_series("first_added_at", first),
            last_added_at=utc_series("last_added_at", last),
            removed_at=pl.when(pl.Series(removed)).then(removed_series),
            updated_at=utc_series("updated_at", np.where(removed, removed_at, last)),
        ),
    )
