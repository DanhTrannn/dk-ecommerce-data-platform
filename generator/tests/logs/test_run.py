import json
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from datagen.logs.inputs import LogInputs
from datagen.logs.run import generate_logs
from datagen.presets import load_log_preset, load_oltp_preset, log_preset_path, preset_path

PRESET = load_log_preset(log_preset_path("1m"))
OLTP = load_oltp_preset(preset_path("1m"))
UTC_MICROS = pl.Datetime("us", "UTC")
LOCAL = timedelta(hours=7)


def inputs(order_times: list[datetime]) -> LogInputs:
    orders = pl.DataFrame(
        {
            "order_number": [f"ORD-{i + 1}" for i in range(len(order_times))],
            "created_at": order_times,
            "customer_key": [f"cust-{i % 5}" for i in range(len(order_times))],
        },
        schema={"order_number": pl.String, "created_at": UTC_MICROS, "customer_key": pl.String},
    ).with_columns(local_date=pl.col("created_at").dt.convert_time_zone("Asia/Ho_Chi_Minh").dt.date())
    lines = orders.select(
        "order_number",
        pl.lit(1, dtype=pl.Int64).alias("product_id"),
        pl.lit("ao-thun").alias("product_slug"),
        pl.lit("v-a").alias("variant_key"),
    )
    return LogInputs(
        orders=orders.select("order_number", "created_at", "local_date", "customer_key"),
        lines=lines,
        products=pl.DataFrame(
            {"product_id": [1, 2], "product_slug": ["ao-thun", "quan-jean"], "weight": [0.6, 0.4]},
            schema={"product_id": pl.Int64, "product_slug": pl.String, "weight": pl.Float64},
        ),
        variants=pl.DataFrame(
            {"product_id": [1, 2], "variant_key": ["v-a", "v-c"]},
            schema={"product_id": pl.Int64, "variant_key": pl.String},
        ),
        customers=pl.Series("customer_key", ["cust-0", "cust-1"]),
    )


def local(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC) - LOCAL


def actions(out: Path) -> pl.DataFrame:
    frame = pl.concat([pl.read_parquet(path) for path in sorted(out.glob("*.parquet"))])
    return frame.with_columns(
        action=pl.col("raw_payload").str.json_path_match("$.ecommerce.action"),
        order_number=pl.col("raw_payload").str.json_path_match("$.ecommerce.order_number"),
        session_id=pl.col("raw_payload").str.json_path_match("$.session.id"),
    )


class GenerateLogsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.tmp = Path(temporary.name)

    def test_one_buying_session_per_order_and_deterministic(self):
        data = inputs([local(date(2025, 11, 3), 10), local(date(2025, 11, 3), 20), local(date(2025, 11, 5), 9)])
        result = generate_logs(PRESET, OLTP, data, self.tmp / "a")
        generate_logs(PRESET, OLTP, data, self.tmp / "b")
        files = sorted(path.name for path in (self.tmp / "a").glob("*.parquet"))
        self.assertEqual(files, ["session_date=2025-11-03.parquet", "session_date=2025-11-05.parquet"])
        for name in files:
            self.assertTrue(pl.read_parquet(self.tmp / "a" / name).equals(pl.read_parquet(self.tmp / "b" / name)))
        submits = actions(self.tmp / "a").filter(pl.col("action") == "checkout_submit")
        self.assertEqual(sorted(submits["order_number"]), ["ORD-1", "ORD-2", "ORD-3"])
        self.assertEqual(result["2025-11-03"]["buying_sessions"], 2)
        self.assertEqual(sum(day["requests"] for day in result.values()), actions(self.tmp / "a").height)
        json.loads(actions(self.tmp / "a")["raw_payload"][0])

    def test_midnight_session_keeps_event_time(self):
        generate_logs(PRESET, OLTP, inputs([local(date(2025, 11, 1), 0, 3)]), self.tmp / "c")
        frame = actions(self.tmp / "c")
        buying = frame.filter(
            pl.col("session_id") == frame.filter(pl.col("action") == "checkout_submit")["session_id"][0]
        )
        self.assertTrue((self.tmp / "c" / "session_date=2025-11-01.parquet").exists())
        self.assertEqual(buying["event_at"].max(), local(date(2025, 11, 1), 0, 3))

    def test_day_without_orders_writes_nothing(self):
        generate_logs(PRESET, OLTP, inputs([local(date(2025, 11, 1), 12)]), self.tmp / "d")
        self.assertEqual([p.name for p in (self.tmp / "d").glob("*.parquet")], ["session_date=2025-11-01.parquet"])

    def test_sale_day_has_fewer_browsing_sessions_per_order(self):
        ordinary, sale = date(2025, 11, 20), date(2025, 11, 28)  # 28/11/2025 is Black Friday in the OLTP calendar
        times = [local(day, 8 + i % 12, i % 60) for day in (ordinary, sale) for i in range(40)]
        result = generate_logs(PRESET, OLTP, inputs(times), self.tmp / "e")
        ratio = {
            day: result[str(day)]["non_buying_sessions"] / result[str(day)]["buying_sessions"]
            for day in (ordinary, sale)
        }
        self.assertAlmostEqual(ratio[ordinary], PRESET.sessions.non_buying_per_buying.value, delta=0.1)
        expected_sale = PRESET.sessions.non_buying_per_buying.value * PRESET.sessions.sale_day_ratio_multiplier
        self.assertAlmostEqual(ratio[sale], expected_sale, delta=0.1)


if __name__ == "__main__":
    unittest.main()
