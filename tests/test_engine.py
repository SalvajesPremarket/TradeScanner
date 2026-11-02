import os
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from unittest.mock import patch

from tradescanner.engine import _get_multi_session_minute_history, run_scan_cycle
from tradescanner.schwab import SchwabAPIError
from tradescanner.snapshots import SnapshotStore


class AutomaticEngineTests(unittest.TestCase):
    def test_multi_session_strategy_history_requests_five_sessions(self):
        with patch("tradescanner.engine.get_price_history", return_value={"candles": []}) as fetch:
            _get_multi_session_minute_history("AAPL")
        fetch.assert_called_once_with("AAPL", period=5, frequency=1)

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
                "TRADESCANNER_BATCHES_PER_CYCLE": "1",
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

    def test_default_scan_cycle_sweeps_eight_quote_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            symbols = [f"SYM{i:04d}" for i in range(900)]
            requested_batches = []
            from threading import Lock
            calls_lock = Lock()

            def fetch(batch):
                with calls_lock:
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
                "TRADESCANNER_BATCHES_PER_CYCLE": "8",
            }):
                result = run_scan_cycle(
                    store, universe_loader=lambda: {"symbols": symbols}, quote_fetcher=fetch
                )

            self.assertEqual(result["cursor"], 800)
            self.assertEqual(result["requested_count"], 800)
            self.assertEqual(result["received_count"], 800)
            self.assertEqual(store.stats()["snapshot_count"], 800)
            self.assertEqual(sorted(len(batch) for batch in requested_batches), [100] * 8)

    def test_stale_quotes_do_not_refresh_market_data_connectivity(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            symbols = ["AAPL"]
            def fetch(batch):
                return {symbol: {"quote": {
                    "lastPrice": 10.0, "closePrice": 9.0,
                    "quoteTimeInLong": 1_000,
                }, "reference": {}} for symbol in batch}
            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }):
                result = run_scan_cycle(store, universe_loader=lambda: {"symbols": symbols}, quote_fetcher=fetch)
            self.assertEqual(result["status"], "running")
            self.assertNotIn("last_quote_received_at", result)

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


    def test_active_strategy_refreshes_history_older_than_fresh_quote(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            from tradescanner.candles import CandleStore
            ny = ZoneInfo("America/New_York")
            old_stamp = int(datetime(2026, 10, 29, 9, 30, tzinfo=ny).timestamp() * 1000)
            CandleStore(store.path).save([
                {
                    "symbol": "AAPL", "candle_time_ms": old_stamp + index * 60_000,
                    "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
                    "volume": 100,
                }
                for index in range(40)
            ])
            store.set_state("user_strategies_v1", [{
                "id": "test-strategy", "name": "TestFormula", "language": "formula",
                "source": "price > 0", "enabled": True, "execution_ready": True,
            }])
            requested_history = []
            def fetch_quotes(batch):
                return {symbol: {
                    "quote": {
                        "lastPrice": 11.0, "closePrice": 10.0, "totalVolume": 1000,
                        "quoteTime": int(time.time() * 1000),
                    },
                    "reference": {"description": symbol},
                } for symbol in batch}
            def fetch_history(symbol):
                requested_history.append(symbol)
                return {"candles": []}
            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }):
                run_scan_cycle(
                    store,
                    universe_loader=lambda: {"symbols": ["AAPL"]},
                    quote_fetcher=fetch_quotes,
                    candle_fetcher=fetch_history,
                )
            self.assertEqual(requested_history, ["AAPL"])

    def test_inactive_strategy_still_refreshes_stale_candles_for_volume_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            from tradescanner.candles import CandleStore
            ny = ZoneInfo("America/New_York")
            old_stamp = int(datetime(2026, 10, 29, 9, 30, tzinfo=ny).timestamp() * 1000)
            CandleStore(store.path).save([
                {
                    "symbol": "VOLUMEONLY", "candle_time_ms": old_stamp + index * 60_000,
                    "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
                    "volume": 100,
                }
                for index in range(40)
            ])
            requested_history = []
            def fetch_quotes(batch):
                return {symbol: {
                    "quote": {
                        "lastPrice": 11.0, "closePrice": 10.0,
                        "quoteTime": int(time.time() * 1000),
                    },
                    "reference": { "description": symbol },
                } for symbol in batch}
            def fetch_history(symbol):
                requested_history.append(symbol)
                return {"candles": []}
            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }), patch("tradescanner.engine.refresh_float_cache", return_value={
                "status": "cached", "provider": "Massive", "rows_saved": 0,
            }):
                run_scan_cycle(
                    store,
                    universe_loader=lambda: {"symbols": ["VOLUMEONLY"]},
                    quote_fetcher=fetch_quotes,
                    candle_fetcher=fetch_history,
                )
            self.assertEqual(requested_history, ["VOLUMEONLY"])

    def test_scan_status_reports_cached_float_value(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            with store._connect() as connection:
                connection.execute(
                    """CREATE TABLE float_cache (
                        symbol TEXT PRIMARY KEY, float_shares REAL NOT NULL,
                        source_date TEXT, cached_at REAL NOT NULL, source TEXT NOT NULL
                    )"""
                )
                connection.execute(
                    "INSERT INTO float_cache VALUES (?, ?, ?, ?, ?)",
                    ("AAPL", 123456.0, "2026-10-01", 100000.0, "massive"),
                )

            def fetch(batch):
                return {symbol: {
                    "quote": {"lastPrice": 11.0, "closePrice": 10.0, "totalVolume": 1000},
                    "reference": {"description": symbol},
                } for symbol in batch}

            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }), patch("tradescanner.engine.refresh_float_cache", return_value={
                "status": "cached", "provider": "Massive", "rows_saved": 0
            }):
                result = run_scan_cycle(
                    store, universe_loader=lambda: {"symbols": ["AAPL"]}, quote_fetcher=fetch
                )

            self.assertEqual(result["targeted_float_status"], "cached")
            self.assertEqual(result["float_symbol"], "AAPL")
            self.assertEqual(result["float_shares"], 123456.0)
            self.assertEqual(result["float_as_of"], "2026-10-01")
            self.assertEqual(result["float_source"], "massive")

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
            self.assertEqual(result["candles_saved"], 4)
            self.assertEqual(result["history_requests"], 2)
            self.assertEqual(result["candle_symbols"], ["AAPL", "MSFT"])
            self.assertIsNone(result["rvol"])
            self.assertEqual(result["rvol_sessions_used"], 0)



    def test_history_sampling_prioritizes_low_volume_when_no_strategy_is_active(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            symbols = ["THIN", "LIQUID"]

            def fetch(batch):
                return {
                    "THIN": {
                        "quote": {"lastPrice": 20.0, "closePrice": 10.0, "totalVolume": 100},
                        "reference": {"description": "Thin stock"},
                    },
                    "LIQUID": {
                        "quote": {"lastPrice": 11.0, "closePrice": 10.0, "totalVolume": 10000},
                        "reference": {"description": "Liquid stock"},
                    },
                }

            requested_history = []
            def fetch_history(symbol):
                requested_history.append(symbol)
                return {"candles": []}

            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }):
                run_scan_cycle(
                    store,
                    universe_loader=lambda: {"symbols": symbols},
                    quote_fetcher=fetch,
                    candle_fetcher=fetch_history,
                )

            self.assertEqual(requested_history, ["THIN", "LIQUID"])


    def test_infisical_can_supply_refresh_token_without_render_refresh_variable(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            env = {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "",
                "INFISICAL_CLIENT_ID": "vault-client",
                "INFISICAL_CLIENT_SECRET": "vault-secret",
                "INFISICAL_PROJECT_ID": "project-id",
                "INFISICAL_ENVIRONMENT": "prod",
            }
            def fetch(batch):
                return {symbol: {
                    "quote": {"lastPrice": 10.0, "closePrice": 9.0},
                    "reference": {"description": symbol},
                } for symbol in batch}

            with patch.dict(os.environ, env, clear=True), patch(
                "tradescanner.engine.infisical_is_configured", return_value=True
            ):
                result = run_scan_cycle(
                    store,
                    universe_loader=lambda: {"symbols": ["AAPL"]},
                    quote_fetcher=fetch,
                )

            self.assertEqual(result["status"], "running")
            self.assertEqual(result["received_count"], 1)

    def test_float_diagnostics_do_not_get_overwritten_by_candle_symbol(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            with store._connect() as connection:
                connection.execute(
                    """CREATE TABLE float_cache (
                        symbol TEXT PRIMARY KEY, float_shares REAL NOT NULL,
                        source_date TEXT, cached_at REAL NOT NULL, source TEXT NOT NULL
                    )"""
                )
                connection.execute(
                    "INSERT INTO float_cache VALUES (?, ?, ?, ?, ?)",
                    ("AAPL", 123456.0, "2026-10-01", 100000.0, "massive"),
                )

            def fetch(batch):
                return {
                    "AAPL": {
                        "quote": {"lastPrice": 11.0, "closePrice": 10.0, "totalVolume": 100},
                        "reference": {"description": "AAPL"},
                    },
                    "MSFT": {
                        "quote": {"lastPrice": 21.0, "closePrice": 20.0, "totalVolume": 200},
                        "reference": {"description": "MSFT"},
                    },
                }

            float_rows = [
                {"symbol": "AAPL", "price": 11.0, "volume": 100, "float_shares": None},
                {"symbol": "MSFT", "price": 21.0, "volume": 50, "float_shares": 999999.0},
            ]
            candle_rows = [
                {"symbol": "AAPL", "price": 11.0, "volume": 100, "float_shares": None},
                {"symbol": "MSFT", "price": 21.0, "volume": 50, "float_shares": 999999.0},
            ]
            fresh_rows = [
                {"symbol": "AAPL", "quote_is_fresh": True},
                {"symbol": "MSFT", "quote_is_fresh": True},
            ]
            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }), patch.object(store, "latest_for_symbols", side_effect=[
                float_rows, candle_rows, fresh_rows
            ]), patch("tradescanner.engine.refresh_float_cache", return_value={
                "status": "cached", "provider": "Massive", "rows_saved": 0
            }):
                result = run_scan_cycle(
                    store,
                    universe_loader=lambda: {"symbols": ["AAPL", "MSFT"]},
                    quote_fetcher=fetch,
                    candle_fetcher=lambda symbol: {"candles": []},
                )

            self.assertEqual(result["targeted_float_symbol"], "AAPL")
            self.assertEqual(result["float_symbol"], "AAPL")
            self.assertEqual(result["float_shares"], 123456.0)
            self.assertEqual(result["candle_symbol"], "MSFT")

    def test_history_backfill_is_bounded_and_fetches_multiple_missing_symbols(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "engine.sqlite3")
            symbols = [f"SYM{i:02d}" for i in range(8)]

            def fetch(batch):
                return {
                    symbol: {
                        "quote": {"lastPrice": 11.0, "closePrice": 10.0, "totalVolume": 1000 - i},
                        "reference": {"description": symbol},
                    }
                    for i, symbol in enumerate(batch)
                }

            requested_history = []
            def fetch_history(symbol):
                requested_history.append(symbol)
                return {"candles": []}

            with patch.dict(os.environ, {
                "SCHWAB_CLIENT_ID": "test-id",
                "SCHWAB_CLIENT_SECRET": "test-secret",
                "SCHWAB_REFRESH_TOKEN": "test-refresh",
            }):
                result = run_scan_cycle(
                    store,
                    universe_loader=lambda: {"symbols": symbols},
                    quote_fetcher=fetch,
                    candle_fetcher=fetch_history,
                )

            self.assertEqual(len(requested_history), 8)
            self.assertEqual(result["history_requests"], 8)
            self.assertEqual(result["candle_symbols"], requested_history)



if __name__ == "__main__":
    unittest.main()
