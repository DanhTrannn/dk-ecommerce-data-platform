import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock


from lakehouse.utils.maintenance import expire


class ExpiryTests(unittest.TestCase):
    def test_keeps_refs_the_latest_snapshots_and_recent_ones(self):
        old = int((datetime.now(UTC) - timedelta(days=10)).timestamp() * 1000)
        recent = int(datetime.now(UTC).timestamp() * 1000)
        snapshots = [SimpleNamespace(snapshot_id=i, timestamp_ms=old + i) for i in range(1, 26)]
        snapshots.append(SimpleNamespace(snapshot_id=99, timestamp_ms=recent))
        commit = Mock()
        table = SimpleNamespace(
            metadata=SimpleNamespace(snapshots=snapshots, refs={"tag": SimpleNamespace(snapshot_id=2)}),
            maintenance=SimpleNamespace(expire_snapshots=lambda: SimpleNamespace(by_ids=commit)),
        )
        # The 20 newest are 99 and 25..7; tag keeps 2; the rest are older than three days.
        self.assertEqual(expire(table), {"expired_snapshots": 5})
        self.assertEqual(commit.call_args.args[0], [6, 5, 4, 3, 1])


if __name__ == "__main__":
    unittest.main()
