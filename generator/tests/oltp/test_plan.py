import unittest
from datetime import UTC, date, datetime

import numpy as np
import polars as pl

from datagen.oltp.plan import IdAllocator, build_timeline, customer_batches, sale_event_dates, yearmonth_of
from datagen.presets import load_oltp_preset, preset_path


def local_index(timeline, day):
    return int(np.flatnonzero(timeline.local_date == np.datetime64(day))[0])


class TimelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.preset = load_oltp_preset(preset_path("1m"))
        cls.timeline = build_timeline(cls.preset)

    def test_timeline_covers_whole_local_days_only(self):
        t = self.timeline
        self.assertEqual(len(t.day_start), 364)
        self.assertEqual(t.day_start[0], np.datetime64("2025-07-01T17:00:00"))
        self.assertEqual(t.local_date[0], np.datetime64("2025-07-02"))
        self.assertEqual(t.local_date[-1], np.datetime64("2026-06-30"))
        self.assertAlmostEqual(float(t.day_weight.sum()), 1.0)
        self.assertEqual(int(t.yearmonth[0]), 202507)
        self.assertEqual(int(t.yearmonth[-1]), 202606)

    def test_sale_and_tet_multipliers(self):
        t = self.timeline
        base = t.day_weight[local_index(t, date(2025, 9, 10))]
        ratio = lambda day: float(t.day_weight[local_index(t, day)] / base)  # noqa: E731
        self.assertAlmostEqual(ratio(date(2025, 11, 11)), 6.39)
        self.assertAlmostEqual(ratio(date(2025, 11, 12)), 2.6)
        self.assertAlmostEqual(ratio(date(2025, 11, 15)), 1.0)
        self.assertAlmostEqual(ratio(date(2026, 2, 10)), 1.5)
        self.assertAlmostEqual(ratio(date(2026, 2, 18)), 0.3)
        self.assertAlmostEqual(ratio(date(2026, 2, 22)), 1.0)
        self.assertEqual(int(t.is_sale_day.sum()), 3)

    def test_sale_event_dates_resolve_black_friday(self):
        events = sale_event_dates(self.preset)
        self.assertEqual(events[date(2025, 11, 28)], "black_friday")
        self.assertEqual(events[date(2025, 11, 11)], "11-11")

    def test_yearmonth_follows_business_timezone(self):
        stamps = pl.Series([datetime(2025, 7, 31, 18, tzinfo=UTC), datetime(2025, 7, 31, 16, tzinfo=UTC)])
        self.assertEqual(yearmonth_of(self.preset, stamps).to_list(), [202508, 202507])


class BatchTests(unittest.TestCase):
    def test_customer_batches_cover_everyone_exactly(self):
        batches = customer_batches(load_oltp_preset(preset_path("1m")))
        self.assertEqual(sum(b.customer_count for b in batches), 286000)
        self.assertEqual(sum(b.buyer_count for b in batches), 260000)
        self.assertEqual([b.index for b in batches], list(range(6)))
        self.assertTrue(all(b.customer_count <= 50_000 for b in batches))
        self.assertTrue(all(0 < b.buyer_count <= b.customer_count for b in batches))

    def test_id_allocator_hands_out_contiguous_ranges(self):
        ids = IdAllocator({"orders": 5})
        self.assertEqual(ids.take("orders", 3).tolist(), [5, 6, 7])
        self.assertEqual(ids.take("orders", 2).tolist(), [8, 9])
        self.assertEqual(len(ids.take("orders", 0)), 0)


if __name__ == "__main__":
    unittest.main()
