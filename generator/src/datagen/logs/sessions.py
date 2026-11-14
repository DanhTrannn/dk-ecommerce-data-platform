"""Requests of one day's sessions: one buying session per online order, browsing sessions around them.

Pure functions: rows in, rows out, randomness only from the given stream. Each request is a row of
REQUESTS_SCHEMA; events.py turns rows into contract JSON.
"""

import numpy as np
import polars as pl

from datagen.presets import LogPreset
from datagen.randomness import uuid4_strings

MICROS_PER_SECOND = 1_000_000
MAX_GAP_MICROS = 1800 * MICROS_PER_SECOND  # a longer pause would start a new session (30 min inactivity)
MICROS_PER_HOUR = 3600 * MICROS_PER_SECOND

REQUESTS_SCHEMA = pl.Schema(
    {
        "session_id": pl.String,
        "request_id": pl.String,
        "ts": pl.Datetime("us", "UTC"),
        "action": pl.String,
        "actor_type": pl.String,
        "actor_key": pl.String,
        "product_key": pl.String,
        "variant_key": pl.String,
        "order_number": pl.String,
    }
)

# Position of each request kind inside a session; requests of the same stage keep their own order key.
SEARCH, VIEW, CART, CART_READ, QUOTE, SUBMIT = range(6)


def _gaps(preset: LogPreset, rng: np.random.Generator, size: int) -> np.ndarray:
    """L3: microseconds between two consecutive requests, log-normal, at least 1 µs, at most 30 min."""
    gap = preset.timing.event_gap_seconds
    seconds = rng.lognormal(np.log(gap.median), gap.sigma, size)
    return np.clip(np.round(seconds * MICROS_PER_SECOND), 1, MAX_GAP_MICROS).astype(np.int64)


def _step(rate: float, rng: np.random.Generator, size: int) -> np.ndarray:
    return rng.random(size) < rate


def _viewed_products(products: pl.DataFrame, rng: np.random.Generator, size: int) -> np.ndarray:
    """Row indexes into `products`, drawn by sales weight (views proportional to purchases)."""
    return rng.choice(products.height, size=size, p=products["weight"].to_numpy())


def _rows(session: np.ndarray, stage: int, order_key: np.ndarray, action: str, **columns) -> pl.DataFrame:
    return pl.DataFrame(
        {"session": session, "stage": np.full(len(session), stage), "order_key": order_key, "action": action, **columns}
    )


def _finish(rows: pl.DataFrame, sessions: pl.DataFrame, preset: LogPreset, rng: np.random.Generator, *, anchor: str):
    """Order requests inside each session, lay them on the clock from the anchor, attach ids and actors.

    anchor="end": the last request happens at sessions.anchor_ts (buying sessions end at checkout).
    anchor="start": the first request happens at sessions.anchor_ts (browsing sessions).
    """
    rows = rows.sort("session", "stage", "order_key")
    gap = _gaps(preset, rng, rows.height)
    rows = rows.with_columns(gap=pl.Series(gap)).with_columns(
        is_last=pl.col("session") != pl.col("session").shift(-1, fill_value=-1),
        is_first=pl.col("session") != pl.col("session").shift(1, fill_value=-1),
    )
    if anchor == "end":
        # offset before the end = sum of the gaps from this request to the last one, the last gap being 0
        rows = rows.with_columns(gap=pl.when(pl.col("is_last")).then(0).otherwise(pl.col("gap")))
        offset = -pl.col("gap").cum_sum(reverse=True).over("session")
    else:
        rows = rows.with_columns(gap=pl.when(pl.col("is_first")).then(0).otherwise(pl.col("gap")))
        offset = pl.col("gap").cum_sum().over("session")
    rows = rows.with_columns(
        pl.lit(None, dtype=pl.String).alias(name)
        for name in ("product_key", "variant_key", "order_number")
        if name not in rows.columns
    )
    sessions = sessions.with_columns(session_id=uuid4_strings(rng, sessions.height))
    return (
        rows.join(sessions, on="session", how="left", maintain_order="left")
        .with_columns(
            ts=pl.col("anchor_ts") + pl.duration(microseconds=offset), request_id=uuid4_strings(rng, rows.height)
        )
        .select(pl.col(name).cast(dtype) for name, dtype in REQUESTS_SCHEMA.items())
        .sort("session_id", "ts")
    )


def buying_requests(
    orders: pl.DataFrame, lines: pl.DataFrame, products: pl.DataFrame, preset: LogPreset, rng: np.random.Generator
) -> pl.DataFrame:
    """One logged-in session per order, ending with checkout_submit at the order's created_at (spec 6.1)."""
    orders = orders.with_row_index("session").with_columns(pl.col("session").cast(pl.Int64))
    lines = lines.join(orders.select("order_number", "session"), on="order_number", how="inner", maintain_order="left")
    bought = lines.unique(subset=["session", "product_id"], keep="first", maintain_order=True)
    distinct = bought.group_by("session").len().sort("session")["len"].to_numpy()
    sessions = orders.height
    views = np.maximum(rng.geometric(preset.sessions.views_per_session.buying.p, sessions), distinct)
    extra = views - distinct
    extra_session = np.repeat(np.arange(sessions), extra)
    extra_product = _viewed_products(products, rng, int(extra.sum()))
    steps = preset.assumptions.request_steps
    searched = np.flatnonzero(_step(steps.category_list_or_search_before_first_view, rng, sessions))
    read = np.flatnonzero(_step(steps.cart_read_before_checkout, rng, sessions))
    quoted = np.flatnonzero(_step(steps.checkout_quote_before_submit, rng, sessions))
    all_sessions = np.arange(sessions)
    rows = pl.concat(
        [
            _rows(searched, SEARCH, np.zeros(len(searched)), "catalog_search"),
            _rows(
                bought["session"].to_numpy(),
                VIEW,
                rng.random(bought.height),
                "product_detail",
                product_key=bought["product_slug"].to_numpy(),
            ),
            _rows(
                extra_session,
                VIEW,
                rng.random(len(extra_session)),
                "product_detail",
                product_key=products["product_slug"].to_numpy()[extra_product],
            ),
            _rows(
                lines["session"].to_numpy(),
                CART,
                np.arange(lines.height, dtype=np.float64),
                "cart_item_set",
                variant_key=lines["variant_key"].to_numpy(),
            ),
            _rows(read, CART_READ, np.zeros(len(read)), "cart_read"),
            _rows(quoted, QUOTE, np.zeros(len(quoted)), "checkout_quote"),
            _rows(
                all_sessions,
                SUBMIT,
                np.zeros(sessions),
                "checkout_submit",
                order_number=orders["order_number"].to_numpy(),
            ),
        ],
        how="diagonal_relaxed",
    )
    actors = orders.select(
        "session",
        pl.col("created_at").alias("anchor_ts"),
        pl.lit("customer").alias("actor_type"),
        pl.col("customer_key").alias("actor_key"),
    )
    return _finish(rows, actors, preset, rng, anchor="end")


def non_buying_count(buying: int, multiplier: float, preset: LogPreset, rng: np.random.Generator) -> int:
    """L1 (× L4 on sale days): browsing sessions of a day, integer part plus a Bernoulli draw for the fraction."""
    expected = buying * preset.sessions.non_buying_per_buying.value * multiplier
    whole = int(expected)
    return whole + int(rng.random() < expected - whole)


def non_buying_requests(
    count: int,
    orders: pl.DataFrame,
    zone: str,
    products: pl.DataFrame,
    variants: pl.DataFrame,
    customers: pl.Series,
    preset: LogPreset,
    rng: np.random.Generator,
) -> pl.DataFrame:
    """Browsing sessions starting in the hour of a random order of the day (spec 6.2).

    A share `non_buying_cart_rate` adds one variant of its last viewed product to the cart; the API requires a login for
    the cart, so exactly those sessions belong to a randomly chosen active customer. All other sessions are anonymous.
    """
    if count == 0:
        return pl.DataFrame(schema=REQUESTS_SCHEMA)
    picked = orders["created_at"].gather(rng.integers(0, orders.height, count))
    hour_start = picked.dt.convert_time_zone(zone).dt.truncate("1h").dt.convert_time_zone("UTC")
    start = hour_start + pl.Series(rng.integers(0, MICROS_PER_HOUR, count)).cast(pl.Duration("us"))
    views = rng.logseries(preset.sessions.views_per_session.non_buying.theta, count)
    view_session = np.repeat(np.arange(count), views)
    view_product = _viewed_products(products, rng, int(views.sum()))
    carting = _step(preset.funnel.non_buying_cart_rate, rng, count) & (customers.len() > 0)
    cart_session = np.flatnonzero(carting)
    last_view = np.cumsum(views) - 1
    cart_product = products["product_id"].to_numpy()[view_product[last_view[cart_session]]]
    variant_product = variants["product_id"].to_numpy()
    first = np.searchsorted(variant_product, cart_product, side="left")
    choices = np.searchsorted(variant_product, cart_product, side="right") - first
    cart_variant = variants["variant_key"].to_numpy()[
        first + (rng.random(len(cart_session)) * choices).astype(np.int64)
    ]
    searched = np.flatnonzero(
        _step(preset.assumptions.request_steps.category_list_or_search_before_first_view, rng, count)
    )
    rows = pl.concat(
        [
            _rows(searched, SEARCH, np.zeros(len(searched)), "catalog_search"),
            _rows(
                view_session,
                VIEW,
                np.arange(len(view_session), dtype=np.float64),
                "product_detail",
                product_key=products["product_slug"].to_numpy()[view_product],
            ),
            _rows(cart_session, CART, np.zeros(len(cart_session)), "cart_item_set", variant_key=cart_variant),
        ],
        how="diagonal_relaxed",
    )
    actor_key = pl.Series("actor_key", [None] * count, dtype=pl.String)
    if len(cart_session):
        actor_key = actor_key.scatter(
            cart_session, customers.gather(rng.integers(0, customers.len(), len(cart_session)))
        )
    actors = pl.DataFrame(
        {
            "session": np.arange(count),
            "anchor_ts": start,
            "actor_type": np.where(carting, "customer", "anonymous"),
            "actor_key": actor_key,
        }
    )
    return _finish(rows, actors, preset, rng, anchor="start")
