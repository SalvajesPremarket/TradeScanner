import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tradescanner.engine import run_scan_cycle
from tradescanner.snapshots import SnapshotStore


class AutomaticEngineTests(unittest.TestCase):
    def test_scan_cycles_advance_persisted_cursor_in_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            symbols = [f"SYM{i:03d}" for i in range(230)]
            requested_batches = []

            def fetch(batch):
                requested_batches.append(list(batch))
                return {
                    symbol: {
                        "quote": {"lastPrice": 10.0, "closePrice": 9.0},
                        "reference": {"description": symbol},
                    }
                    for symbol in batch
                }

            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }):
                first = run_scan_cycle(
                    store, universe_loader=lambda: {"symbols": symbols}, quote_fetcher=fetch
                )
                second = run_scan_cycle(
                    store, universe_loader=lambda: {"symbols": symbols}, quote_fetcher=fetch
                )

            self.assertEqual(first["status"], "running")
            self.assertEqual(first["cursor"], 100)
            self.assertEqual(second["cursor"], 200)
            self.assertEqual(requested_batches[0], symbols[:100])
            self.assertEqual(requested_batches[1], symbols[100:200])
            self.assertEqual(store.stats()["snapshot_count"], 200)
            self.assertEqual(store.get_state("cycles_completed"), 2)

    def test_missing_credentials_waits_without_loading_universe(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            with patch.dict(os.environ, {}, clear=True):
                result = run_scan_cycle(
                    store,
                    universe_loader=lambda: self.fail("universe should not load without credentials"),
                )
            self.assertEqual(result["status"], "waiting_for_credentials")
            self.assertEqual(store.stats()["snapshot_count"], 0)


if __name__ == "__main__":
    unittest.main()
