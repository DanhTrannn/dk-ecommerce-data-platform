import gzip
import json
import tempfile
import unittest
from pathlib import Path

from generator.config import load_config
from generator.log_export import export_logs


class LogExportTests(unittest.TestCase):
    def test_historical_logs_are_labelled_synthetic(self):
        config = load_config(Path(__file__).resolve().parents[1] / "configs" / "small.yml")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            summary = export_logs(config, root, expected_requests=30)
            events = []
            for path in root.rglob("*.jsonl.gz"):
                with gzip.open(path, "rt") as stream:
                    events.extend(json.loads(line) for line in stream)
            self.assertEqual(len(events), summary.emitted_requests)
            self.assertTrue(events)
            self.assertEqual({event["data_origin"] for event in events}, {"synthetic"})


if __name__ == "__main__":
    unittest.main()
