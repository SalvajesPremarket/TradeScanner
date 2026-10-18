import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from unittest.mock import patch

from tradescanner.engine import run_scan_cycle
from tradescanner.schwab import SchwabAPIError
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

    def test_empty_quote_batch_does_not_advance_cursor_or_complete_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            symbols = [f"SYM{i:03d}" for i in range(150)]

            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }):
                with self.assertRaises(SchwabAPIError):
                    run_scan_cycle(
                        store,
                        universe_loader=lambda: {"symbols": symbols},
                        quote_fetcher=lambda batch: {},
                    )

            self.assertEqual(store.get_state("universe_cursor", 0), 0)
            self.assertEqual(store.get_state("cycles_completed", 0), 0)
            self.assertEqual(store.stats()["snapshot_count"], 0)
            self.assertEqual(store.get_state("engine_status", {}).get("status"), "error")

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


    def test_scan_cycle_saves_candidate_candles_and_computes_rvol(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            symbols = ["AAPL", "MSFT"]

            def fetch(batch):
                return {
                    symbol: {
                        "quote": {"lastPrice": 11.0, "closePrice": 10.0, "totalVolume": 1000},
                        "reference": {"description": symbol},
                    }
                    for symbol in batch
                }

            ny = ZoneInfo("America/New_York")
            def timestamp(day):
                return int(datetime(2026, 10, day, 9, 30, tzinfo=ny).timestamp() * 1000)

            def fetch_history(symbol):
                self.assertEqual(symbol, "AAPL")
                return {"candles": [
                    {"datetime": timestamp(14), "open": 10, "high": 11, "low": 9, "close": 10, "volume": 100},
                    {"datetime": timestamp(15), "open": 10, "high": 11, "low": 9, "close": 10, "volume": 200},
                ]}

            result = run_scan_cycle(
                store,
                universe_loader=lambda: {"symbols": symbols},
                quote_fetcher=fetch,
                candle_fetcher=fetch_history,
            )
            self.assertEqual(result["candle_symbol"], "AAPL")
            self.assertEqual(result["candles_saved"], 2)
            self.assertIsNone(result["rvol"])
            self.assertEqual(result["rvol_sessions_used"], 0)



if __name__ == "__main__":
    unittest.main()
