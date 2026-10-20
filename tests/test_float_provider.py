import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tradescanner.float_provider import apply_cached_float, refresh_float_cache, refresh_symbol_float
from tradescanner.snapshots import SnapshotStore


class FMPFloatProviderTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SnapshotStore(Path(self.tempdir.name) / "test.sqlite3")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_missing_key_disables_fmp_without_network(self):
        with patch.dict(os.environ, {}, clear=True):
            result = refresh_float_cache(self.store, fetcher=lambda *_: self.fail("must not call FMP"))
        self.assertEqual(result["status"], "not_configured")

    def test_successful_refresh_is_cached_and_only_float_fields_are_used(self):
        calls = []
        def fetch(page, key):
            calls.append((page, key))
            return [
                {"symbol": "AAPL", "floatShares": 123456, "date": "2026-10-01", "outstandingShares": 130000},
                {"symbol": "BAD", "floatShares": 0},
                {"symbol": "INVALID", "floatShares": "unknown"},
            ]
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            first = refresh_float_cache(self.store, fetcher=fetch, now=100000)
            second = refresh_float_cache(self.store, fetcher=lambda *_: self.fail("daily cache should prevent call"), now=100001)
        self.assertEqual(first["status"], "updated")
        self.assertEqual(first["rows_saved"], 1)
        self.assertEqual(second["status"], "cached")
        self.assertEqual(calls, [(0, "test-key")])
        rows = [{"symbol": "AAPL", "float_shares": None, "float_source": None}]
        self.assertEqual(apply_cached_float(rows, self.store)[0]["float_shares"], 123456)
        self.assertEqual(rows[0]["float_source"], "fmp")
        self.assertEqual(rows[0]["float_as_of"], "2026-10-01")

    def test_empty_cache_does_not_trust_recent_success_timestamp(self):
        self.store.set_state("fmp_float_last_success_at", 99999)
        calls = []
        def fetch(page, key):
            calls.append(page)
            return [{"symbol": "AAPL", "floatShares": 123456, "date": "2026-10-01"}]
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            result = refresh_float_cache(self.store, fetcher=fetch, now=100000)
        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["rows_saved"], 1)
        self.assertEqual(calls, [0])

    def test_targeted_float_fallback_caches_symbol_value(self):
        calls = []
        def fetch(symbol, key):
            calls.append((symbol, key))
            return [{"symbol": symbol, "floatShares": 123456, "date": "2026-10-01"}]
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            first = refresh_symbol_float(self.store, "AAPL", fetcher=fetch, now=100000)
            second = refresh_symbol_float(self.store, "AAPL", fetcher=lambda *_: self.fail("cached value should skip network"), now=100001)
        self.assertEqual(first["status"], "updated")
        self.assertEqual(second["status"], "cached")
        self.assertEqual(calls, [("AAPL", "test-key")])
        rows = [{"symbol": "AAPL", "float_shares": None}]
        apply_cached_float(rows, self.store)
        self.assertEqual(rows[0]["float_shares"], 123456)

    def test_targeted_float_failure_uses_six_hour_cooldown(self):
        calls = []
        def fail(symbol, key):
            calls.append(symbol)
            raise RuntimeError("provider unavailable")
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            first = refresh_symbol_float(self.store, "AAPL", fetcher=fail, now=100000)
            second = refresh_symbol_float(self.store, "AAPL", fetcher=fail, now=100001)
        self.assertEqual(first["status"], "unavailable")
        self.assertEqual(second["status"], "cooldown")
        self.assertEqual(calls, ["AAPL"])

    def test_existing_schwab_float_is_never_overwritten(self):
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            refresh_float_cache(
                self.store,
                fetcher=lambda *_: [{"symbol": "AAPL", "floatShares": 123456}],
                now=100000,
            )
        rows = [{"symbol": "AAPL", "float_shares": 555, "float_source": "schwab"}]
        apply_cached_float(rows, self.store)
        self.assertEqual(rows[0]["float_shares"], 555)
        self.assertEqual(rows[0]["float_source"], "schwab")

    def test_bulk_http_error_reports_status_without_provider_body(self):
        import urllib.error
        error = urllib.error.HTTPError(
            "https://provider.invalid/endpoint", 404, "not found", {}, None
        )
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            result = refresh_float_cache(
                self.store, fetcher=lambda *_: (_ for _ in ()).throw(error), now=100000
            )
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["error"], "HTTP 404")
        self.assertNotIn("provider.invalid", str(result))

    def test_targeted_http_error_reports_status_without_provider_body(self):
        import urllib.error
        error = urllib.error.HTTPError(
            "https://provider.invalid/endpoint", 404, "not found", {}, None
        )
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            result = refresh_symbol_float(
                self.store, "AAPL",
                fetcher=lambda *_: (_ for _ in ()).throw(error), now=100000
            )
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["error"], "HTTP 404")
        self.assertNotIn("provider.invalid", str(result))

    def test_failure_is_cooled_down_without_retrying_each_cycle(self):
        calls = []
        def fail(page, key):
            calls.append(page)
            raise RuntimeError("provider unavailable")
        with patch.dict(os.environ, {"FMP_API_KEY": "test-key"}):
            first = refresh_float_cache(self.store, fetcher=fail, now=100000)
            second = refresh_float_cache(self.store, fetcher=fail, now=100001)
        self.assertEqual(first["status"], "unavailable")
        self.assertEqual(second["status"], "cooldown")
        self.assertEqual(calls, [0])


if __name__ == "__main__":
    unittest.main()
