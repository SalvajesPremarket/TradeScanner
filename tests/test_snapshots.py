import tempfile
import time
import unittest
from pathlib import Path

from tradescanner.snapshots import SnapshotStore, collect_snapshots


class SnapshotStoreTests(unittest.TestCase):
    def test_saves_and_reads_real_normalized_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "snapshots.sqlite3")
            rows = [{
                "symbol": "AAPL",
                "price": 100.0,
                "data_source": "schwab",
                "sector": None,
            }]
            self.assertEqual(store.save(rows), 1)
            self.assertEqual(store.stats()["snapshot_count"], 1)
            self.assertEqual(store.stats()["symbols_seen"], 1)
            self.assertEqual(store.latest()[0]["price"], 100.0)

    def test_collection_batches_deduplicates_and_does_not_invent_missing_quotes(self):
        calls = []
        def fetch(batch):
            calls.append(batch)
            return {
                symbol: {"quote": {"lastPrice": 12.5}, "reference": {}}
                for symbol in batch[:1]
            }

        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "snapshots.sqlite3")
            result = collect_snapshots(["aapl", "MSFT", "AAPL"], store, fetch)

        self.assertEqual(result["requested_count"], 2)
        self.assertEqual(result["received_count"], 1)
        self.assertEqual(result["snapshots_saved"], 1)
        self.assertEqual(calls, [["AAPL", "MSFT"]])

    def test_quote_freshness_is_recomputed_when_snapshots_are_read(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "snapshots.sqlite3")
            now_ms = int(time.time() * 1000)
            store.save([
                {"symbol": "FRESH", "price": 10.0, "quote_time_ms": now_ms, "quote_is_fresh": True},
                {"symbol": "STALE", "price": 11.0, "quote_time_ms": now_ms - 10 * 60 * 1000, "quote_is_fresh": True},
                {"symbol": "UNKNOWN", "price": 12.0, "quote_is_fresh": True},
            ])
            rows = {row["symbol"]: row for row in store.latest()}

        self.assertTrue(rows["FRESH"]["quote_is_fresh"])
        self.assertFalse(rows["STALE"]["quote_is_fresh"])
        self.assertGreater(rows["STALE"]["quote_age_seconds"], 300)
        self.assertFalse(rows["UNKNOWN"]["quote_is_fresh"])
        self.assertIsNone(rows["UNKNOWN"]["quote_age_seconds"])

    def test_collection_requires_symbols(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "snapshots.sqlite3")
            with self.assertRaisesRegex(ValueError, "at least one"):
                collect_snapshots([], store, lambda batch: {})


if __name__ == "__main__":
    unittest.main()
