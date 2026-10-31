import os
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from tradescanner.float_provider import (
    apply_cached_float,
    refresh_float_cache,
    refresh_symbol_float,
)
from tradescanner.snapshots import SnapshotStore


class MassiveFloatProviderTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SnapshotStore(Path(self.tempdir.name) / "test.sqlite3")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_missing_massive_key_disables_provider_without_network(self):
        with patch.dict(os.environ, {}, clear=True):
            result = refresh_float_cache(
                self.store, fetcher=lambda *_: self.fail("must not call provider")
            )
        self.assertEqual(result["status"], "not_configured")
        self.assertEqual(result["provider"], "Massive")

    def test_bulk_refresh_normalizes_free_float_and_caches_daily(self):
        calls = []

        def fetch(url, key):
            calls.append((url, key))
            return {"results": [
                {"ticker": "AAPL", "free_float": 123456, "effective_date": "2026-10-01"},
                {"ticker": "BAD", "free_float": 0},
                {"ticker": "INVALID", "free_float": "unknown"},
            ]}

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            first = refresh_float_cache(self.store, fetcher=fetch, now=100000)
            second = refresh_float_cache(
                self.store,
                fetcher=lambda *_: self.fail("daily cache should prevent request"),
                now=100001,
            )

        self.assertEqual(first["status"], "updated")
        self.assertEqual(first["provider"], "Massive")
        self.assertEqual(first["rows_saved"], 1)
        self.assertEqual(second["status"], "cached")
        self.assertEqual(len(calls), 1)
        rows = [{"symbol": "AAPL", "float_shares": None}]
        apply_cached_float(rows, self.store)
        self.assertEqual(rows[0]["float_shares"], 123456)
        self.assertEqual(rows[0]["float_source"], "massive")
        self.assertEqual(rows[0]["float_as_of"], "2026-10-01")

    def test_bulk_refresh_follows_bounded_pagination(self):
        calls = []

        def fetch(url, key):
            calls.append(url)
            if len(calls) == 1:
                return {
                    "results": [{"ticker": "AAPL", "free_float": 1000}],
                    "next_url": "https://api.massive.com/stocks/vX/float?cursor=next",
                }
            return {"results": [{"ticker": "MSFT", "free_float": 2000}]}

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            result = refresh_float_cache(self.store, fetcher=fetch, now=100000)

        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["pages_fetched"], 2)
        self.assertEqual(len(calls), 2)
        rows = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
        apply_cached_float(rows, self.store)
        self.assertEqual([row["float_shares"] for row in rows], [1000, 2000])

    def test_empty_cache_does_not_trust_recent_success_timestamp(self):
        self.store.set_state("massive_float_last_success_at", 99999)
        calls = []

        def fetch(url, key):
            calls.append(url)
            return {"results": [{"ticker": "AAPL", "free_float": 123456}]}

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            result = refresh_float_cache(self.store, fetcher=fetch, now=100000)

        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["rows_saved"], 1)
        self.assertEqual(len(calls), 1)

    def test_targeted_refresh_caches_single_symbol(self):
        calls = []

        def fetch(url, key):
            calls.append(url)
            return {"results": [
                {"ticker": "AAPL", "free_float": 123456, "effective_date": "2026-10-01"}
            ]}

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            first = refresh_symbol_float(self.store, "aapl", fetcher=fetch, now=100000)
            second = refresh_symbol_float(
                self.store, "AAPL",
                fetcher=lambda *_: self.fail("cached symbol should skip request"),
                now=100001,
            )

        self.assertEqual(first["status"], "updated")
        self.assertEqual(second["status"], "cached")
        self.assertEqual(first["float_shares"], 123456)
        self.assertEqual(len(calls), 1)

    def test_legacy_fmp_rows_are_ignored_and_never_overwrite_schwab(self):
        with self.store._connect() as connection:
            connection.execute(
                """CREATE TABLE float_cache (
                    symbol TEXT PRIMARY KEY, float_shares REAL NOT NULL,
                    source_date TEXT, cached_at REAL NOT NULL, source TEXT NOT NULL
                )"""
            )
            connection.execute(
                "INSERT INTO float_cache VALUES (?, ?, ?, ?, ?)",
                ("AAPL", 123456, "2026-10-01", 100000, "fmp"),
            )
            connection.execute(
                "INSERT INTO float_cache VALUES (?, ?, ?, ?, ?)",
                ("MSFT", 654321, "2026-10-01", 100000, "massive"),
            )

        rows = [
            {"symbol": "AAPL", "float_shares": None},
            {"symbol": "MSFT", "float_shares": 999, "float_source": "schwab"},
        ]
        apply_cached_float(rows, self.store)
        self.assertIsNone(rows[0]["float_shares"])
        self.assertEqual(rows[1]["float_shares"], 999)
        self.assertEqual(rows[1]["float_source"], "schwab")

    def test_http_error_is_reported_without_provider_body(self):
        error = urllib.error.HTTPError(
            "https://provider.invalid/endpoint", 404, "not found", {}, None
        )
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            result = refresh_float_cache(
                self.store, fetcher=lambda *_: (_ for _ in ()).throw(error), now=100000
            )
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["error"], "HTTP 404")
        self.assertNotIn("provider.invalid", str(result))

    def test_failed_bulk_refresh_uses_six_hour_cooldown(self):
        calls = []

        def fail(url, key):
            calls.append(url)
            raise RuntimeError("provider unavailable")

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            first = refresh_float_cache(self.store, fetcher=fail, now=100000)
            second = refresh_float_cache(self.store, fetcher=fail, now=100001)

        self.assertEqual(first["status"], "unavailable")
        self.assertEqual(second["status"], "cooldown")
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
