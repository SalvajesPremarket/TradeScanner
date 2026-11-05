import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tradescanner.candles import CandleStore
from tradescanner.retention import prune_expired_market_data
from tradescanner.snapshots import SnapshotStore


class MarketDataRetentionTests(unittest.TestCase):
    def test_prunes_only_rows_older_than_seven_days(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "retention.sqlite3"
            store = SnapshotStore(path)
            candles = CandleStore(path)
            now = datetime.now(timezone.utc)
            expired_at = (now - timedelta(days=8)).isoformat()
            retained_at = (now - timedelta(days=6)).isoformat()
            expired_ms = int((now - timedelta(days=8)).timestamp() * 1000)
            retained_ms = int((now - timedelta(days=6)).timestamp() * 1000)

            with store._connect() as connection:
                connection.execute(
                    "INSERT INTO quote_snapshots(captured_at, symbol, data_source, payload_json) "
                    "VALUES (?, ?, ?, ?)",
                    (expired_at, "OLDQ", "test", "{}"),
                )
                connection.execute(
                    "INSERT INTO quote_snapshots(captured_at, symbol, data_source, payload_json) "
                    "VALUES (?, ?, ?, ?)",
                    (retained_at, "NEWQ", "test", "{}"),
                )
            candles.save([
                {"symbol": "OLDC", "candle_time_ms": expired_ms, "open": 1, "high": 1,
                 "low": 1, "close": 1, "volume": 1},
                {"symbol": "NEWC", "candle_time_ms": retained_ms, "open": 1, "high": 1,
                 "low": 1, "close": 1, "volume": 1},
            ])

            result = prune_expired_market_data(store, now=now)

            self.assertEqual(result["retention_days"], 7)
            self.assertEqual(result["quote_snapshots_deleted"], 1)
            self.assertEqual(result["intraday_candles_deleted"], 1)
            self.assertEqual(result["expired_quote_snapshots_remaining"], 0)
            self.assertEqual(result["expired_intraday_candles_remaining"], 0)
            self.assertEqual(store.stats()["snapshot_count"], 1)
            self.assertEqual(candles.history_status_by_symbols(["OLDC", "NEWC"])["NEWC"]["count"], 1)
            self.assertNotIn("OLDC", candles.history_status_by_symbols(["OLDC", "NEWC"]))

    def test_cleanup_is_bounded_per_table_and_can_continue_next_run(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bounded.sqlite3"
            store = SnapshotStore(path)
            candles = CandleStore(path)
            now = datetime.now(timezone.utc)
            old_text = (now - timedelta(days=9)).isoformat()
            old_ms = int((now - timedelta(days=9)).timestamp() * 1000)
            with store._connect() as connection:
                for index in range(3):
                    connection.execute(
                        "INSERT INTO quote_snapshots(captured_at, symbol, data_source, payload_json) "
                        "VALUES (?, ?, ?, ?)",
                        (old_text, f"Q{index}", "test", "{}"),
                    )
            candles.save([
                {"symbol": f"C{index}", "candle_time_ms": old_ms + index, "open": 1,
                 "high": 1, "low": 1, "close": 1, "volume": 1}
                for index in range(3)
            ])

            first = prune_expired_market_data(
                store, now=now, batch_size=1, max_batches_per_table=1
            )
            self.assertEqual(first["quote_snapshots_deleted"], 1)
            self.assertEqual(first["intraday_candles_deleted"], 1)
            self.assertEqual(first["expired_quote_snapshots_remaining"], 2)
            self.assertEqual(first["expired_intraday_candles_remaining"], 2)

            second = prune_expired_market_data(
                store, now=now, batch_size=1, max_batches_per_table=5
            )
            self.assertEqual(second["expired_quote_snapshots_remaining"], 0)
            self.assertEqual(second["expired_intraday_candles_remaining"], 0)

    def test_retention_window_cannot_be_changed_accidentally(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SnapshotStore(Path(directory) / "retention.sqlite3")
            with self.assertRaisesRegex(ValueError, "must remain 7"):
                prune_expired_market_data(store, retention_days=30)


if __name__ == "__main__":
    unittest.main()
