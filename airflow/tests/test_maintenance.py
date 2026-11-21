import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock


import pyarrow as pa

from lakehouse.utils.maintenance import MIN_SMALL_FILES, SMALL_FILE_BYTES, compact, expire


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


class CompactionTests(unittest.TestCase):
    def table(self, sizes, content=0):
        files = pa.table({"file_size_in_bytes": sizes, "content": [content] * len(sizes)})
        return SimpleNamespace(inspect=SimpleNamespace(files=lambda: files))

    def test_waits_until_enough_small_files(self):
        sizes = [SMALL_FILE_BYTES - 1] * (MIN_SMALL_FILES - 1) + [SMALL_FILE_BYTES * 2]
        self.assertEqual(compact(self.table(sizes)), {"rewritten": False, "small_files": MIN_SMALL_FILES - 1})

    def test_rewrites_when_small_files_pile_up_or_deletes_exist(self):
        for table in (self.table([1] * MIN_SMALL_FILES), self.table([SMALL_FILE_BYTES * 2], content=1)):
            data = pa.table({"id": [1]})
            transaction = Mock()
            table.transaction = Mock(return_value=Mock(__enter__=Mock(return_value=transaction), __exit__=Mock(return_value=False)))
            table.scan = lambda: SimpleNamespace(to_arrow=lambda: data)
            self.assertEqual(compact(table), {"rewritten": True, "rows": 1})
            transaction.overwrite.assert_called_once_with(data)


if __name__ == "__main__":
    unittest.main()
