import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from unittest.mock import patch

from tradescanner.engine import _evaluate_rompiendo_maximos
from tradescanner.snapshots import SnapshotStore
from tradescanner.strategies import formula_matches, save_strategy


class RompiendoMaximosTests(unittest.TestCase):
    def test_safe_formula_can_compare_live_price_with_cached_ath(self):
        strategy = save_strategy(
            "RompiendoMaximos", "python",
            "def evaluate(stock): return stock['price'] > stock['all_time_high']",
            store=SnapshotStore(Path(tempfile.mkdtemp()) / "formula.sqlite3"),
        )
        self.assertTrue(strategy["execution_ready"])
        self.assertTrue(formula_matches("price > all_time_high", {"price": 25.41, "all_time_high": 25.40}))
        self.assertFalse(formula_matches("price > all_time_high", {"price": 25.40, "all_time_high": 25.40}))

    def test_live_breakout_uses_prior_completed_session_and_excludes_today(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "ath.sqlite3")
            ny = ZoneInfo("America/New_York")
            quote_time = int(datetime(2026, 10, 30, 10, 0, tzinfo=ny).timestamp() * 1000)
            old_day = int(datetime(2026, 10, 29, 16, 0, tzinfo=ny).timestamp() * 1000)
            today = int(datetime(2026, 10, 30, 9, 30, tzinfo=ny).timestamp() * 1000)
            payload = {"candles": [
                {"datetime": old_day, "high": 25.00},
                {"datetime": today, "high": 99.00},
            ]}
            with patch("tradescanner.engine.get_all_time_daily_history", return_value=payload):
                from tradescanner.engine import _fetch_ath_in_background
                _fetch_ath_in_background("XYZ", quote_time, store.path)
            row = {
                "symbol": "XYZ", "price": 25.10, "quote_time_ms": quote_time,
                "quote_is_fresh": True,
            }
            # Cache the result written by the background fetcher and evaluate against it.
            state = store.get_state("rompiendo_maximos_ath:XYZ", {})
            self.assertEqual(state["high"], 25.00)
            result = _evaluate_rompiendo_maximos(
                store, [row], {"id": "test", "name": "RompiendoMaximos"}
            )
            self.assertEqual(result["matches"], ["XYZ"])
            self.assertEqual(result["evaluations"][0]["ath_date"], "2026-10-29")

    def test_equal_price_is_not_a_breakout(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "ath.sqlite3")
            store.set_state("rompiendo_maximos_ath:XYZ", {
                "status": "ready", "high": 25.00, "ath_date": "2026-10-29",
                "cached_at": 9999999999,
            })
            row = {
                "symbol": "XYZ", "price": 25.00, "quote_time_ms": 1793372400000,
                "quote_is_fresh": True,
            }
            result = _evaluate_rompiendo_maximos(
                store, [row], {"id": "test", "name": "RompiendoMaximos"}
            )
            self.assertEqual(result["matches"], [])
            self.assertFalse(result["evaluations"][0]["passed"])


if __name__ == "__main__":
    unittest.main()
