"""Write the access log of the preset window, one Parquet file per session day."""

from datetime import timedelta
from pathlib import Path

import polars as pl

from datagen.logs.events import to_payloads
from datagen.logs.inputs import LogInputs
from datagen.logs.sessions import buying_requests, non_buying_count, non_buying_requests
from datagen.oltp.plan import sale_event_dates
from datagen.presets import LogPreset, OltpPreset
from datagen.randomness import LOG_SESSIONS, stream


def generate_logs(preset: LogPreset, oltp: OltpPreset, inputs: LogInputs, out: Path) -> dict[str, dict]:
    """Each window day uses its own random stream, so a day never depends on the days before it.

    Days without online orders get no file. Returns per day: buying_sessions, non_buying_sessions, requests.
    """
    out.mkdir(parents=True, exist_ok=True)
    sale_days = set(sale_event_dates(oltp))
    result = {}
    days = (preset.window.end - preset.window.start).days
    for index in range(days):
        day = preset.window.start + timedelta(days=index)
        orders = inputs.orders.filter(pl.col("local_date") == day)
        if orders.is_empty():
            continue
        rng = stream(preset.seed, LOG_SESSIONS, index)
        lines = inputs.lines.join(orders.select("order_number"), on="order_number", how="semi", maintain_order="left")
        buying = buying_requests(orders, lines, inputs.products, preset, rng)
        multiplier = preset.sessions.sale_day_ratio_multiplier if day in sale_days else 1.0
        count = non_buying_count(orders.height, multiplier, preset, rng)
        browsing = non_buying_requests(
            count, orders, oltp.business_timezone, inputs.products, inputs.variants, inputs.customers, preset, rng
        )
        requests = pl.concat([buying, browsing])
        to_payloads(requests).write_parquet(out / f"session_date={day}.parquet")
        result[str(day)] = {"buying_sessions": orders.height, "non_buying_sessions": count, "requests": requests.height}
    return result
