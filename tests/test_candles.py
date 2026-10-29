import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from tradescanner.candles import CandleStore, normalize_price_history


NY = ZoneInfo("America/New_York")


def ms(day, hour, minute):
    return int(datetime(2026, 10, day, hour, minute, tzinfo=NY).timestamp() * 1000)


class CandleTests(unittest.TestCase):
    def test_normalizes_provider_candles_without_filling_missing_fields(self):
        payload = {"candles": [
            {"datetime": ms(14, 9, 30), "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
            {"datetime": "invalid", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
            {"datetime": ms(14, 9, 31), "open": 10, "high": None, "low": 9, "close": 10.5, "volume": 100},
        ]}
        rows = normalize_price_history("aapl", payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "AAPL")
        self.assertEqual(rows[0]["volume"], 100)

    def test_rejects_invalid_ohlcv_and_fractional_volume(self):
        payload = {"candles": [
            {"datetime": ms(14, 9, 30), "open": 10, "high": 9, "low": 8, "close": 10, "volume": 100},
            {"datetime": ms(14, 9, 31), "open": 10, "high": 11, "low": 10.5, "close": 10.2, "volume": 100},
            {"datetime": ms(14, 9, 32), "open": 10, "high": 11, "low": 9, "close": 10.2, "volume": 10.5},
            {"datetime": ms(14, 9, 33), "open": 0, "high": 11, "low": 0, "close": 10.2, "volume": 100},
        ]}
        self.assertEqual(normalize_price_history("AAPL", payload), [])

    def test_counts_by_symbols_uses_normalized_symbols(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CandleStore(Path(directory) / "candles.sqlite3")
            rows = []
            for symbol, count in (("AAPL", 40), ("MSFT", 34)):
                for index in range(count):
                    rows.append({
                        "symbol": symbol,
                        "candle_time_ms": ms(14, 9, 30) + index * 60_000,
                        "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
                        "volume": 100 + index,
                    })
            store.save(rows)
            counts = store.counts_by_symbols(["aapl", "MSFT", "UNKNOWN", "AAPL"])
            self.assertEqual(counts, {"AAPL": 40, "MSFT": 34})

    def test_rvol_requires_two_comparable_historical_sessions(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CandleStore(Path(directory) / "candles.sqlite3")
            store.save([
                {"symbol": "AAPL", "candle_time_ms": ms(13, 9, 30),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 100},
                {"symbol": "AAPL", "candle_time_ms": ms(14, 9, 30),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 200},
            ])
            result = store.rvol("AAPL")
            self.assertIsNone(result["rvol"])
            self.assertEqual(result["sessions_used"], 0)

    def test_calculates_rvol_against_prior_sessions_at_same_time(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CandleStore(Path(directory) / "candles.sqlite3")
            rows = []
            for day, volumes in ((12, (50, 50)), (13, (100, 100)), (14, (150, 250))):
                for minute, volume in zip((30, 31), volumes):
                    rows.append({
                        "symbol": "AAPL",
                        "candle_time_ms": ms(day, 9, minute),
                        "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
                        "volume": volume,
                    })
            self.assertEqual(store.save(rows), 6)
            result = store.rvol("AAPL")
            self.assertEqual(result["current_volume"], 400)
            self.assertEqual(result["average_volume"], 150)
            self.assertEqual(result["sessions_used"], 2)
            self.assertEqual(result["rvol"], 2.6667)

    def test_rvol_allows_missing_interior_minute_buckets(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CandleStore(Path(directory) / "candles.sqlite3")
            rows = [
                # Historical session 12 skips 09:31 but has a later bar proving
                # that the session reached the current 09:31 comparison cutoff.
                {"symbol": "AAPL", "candle_time_ms": ms(12, 9, 30),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 10},
                {"symbol": "AAPL", "candle_time_ms": ms(12, 9, 32),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 20},
                {"symbol": "AAPL", "candle_time_ms": ms(13, 9, 30),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 100},
                {"symbol": "AAPL", "candle_time_ms": ms(13, 9, 31),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 100},
                {"symbol": "AAPL", "candle_time_ms": ms(14, 9, 30),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 150},
                {"symbol": "AAPL", "candle_time_ms": ms(14, 9, 31),
                 "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 250},
            ]
            store.save(rows)
            result = store.rvol("AAPL")
            self.assertEqual(result["sessions_used"], 2)
            self.assertEqual(result["current_volume"], 400)
            self.assertEqual(result["average_volume"], 105)
            self.assertEqual(result["rvol"], 3.8095)

    def test_rvol_is_unavailable_without_prior_sessions(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CandleStore(Path(directory) / "candles.sqlite3")
            store.save([{
                "symbol": "AAPL", "candle_time_ms": ms(14, 9, 30),
                "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 100,
            }])
            self.assertIsNone(store.rvol("AAPL")["rvol"])


if __name__ == "__main__":
    unittest.main()
