import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from tradescanner.news_provider import cached_news_for_symbol, refresh_news_cache
from tradescanner.snapshots import SnapshotStore


class NewsProviderTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SnapshotStore(Path(self.tempdir.name) / "news.sqlite3")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_news_is_cached_and_normalized_for_a_symbol(self):
        now = datetime.now(timezone.utc)
        article = {
            "symbol": "TEST",
            "publishedDate": now.isoformat(),
            "title": "Company announces contract",
            "publisher": "Example Wire",
            "url": "https://example.test/story",
        }
        with patch.dict("os.environ", {"FMP_API_KEY": "test-key"}):
            result = refresh_news_cache(self.store, ["test", "TEST"], fetcher=lambda symbols, key: [article], now=1_800_000_000)
            self.assertEqual(result["status"], "updated")
            cached = cached_news_for_symbol(self.store, "TEST")
            self.assertEqual(len(cached), 1)
            self.assertEqual(cached[0]["headline"], "Company announces contract")
            self.assertEqual(cached[0]["url"], "https://example.test/story")
            called = []
            second = refresh_news_cache(self.store, ["TEST"], fetcher=lambda symbols, key: called.append(True) or [], now=1_800_000_100)
            self.assertEqual(second["status"], "cached")
            self.assertEqual(called, [])

    def test_news_cache_ttl_is_tracked_per_symbol(self):
        calls = []
        articles = {
            "AAA": [{
                "symbol": "AAA",
                "publishedDate": "2026-10-22T12:00:00Z",
                "title": "AAA announces contract",
                "publisher": "Example Wire",
            }],
            "BBB": [{
                "symbol": "BBB",
                "publishedDate": "2026-10-22T12:00:00Z",
                "title": "BBB announces contract",
                "publisher": "Example Wire",
            }],
        }

        def fetch(symbols, key):
            calls.append(list(symbols))
            return [article for symbol in symbols for article in articles.get(symbol, [])]

        with patch.dict("os.environ", {"FMP_API_KEY": "test-key"}):
            first = refresh_news_cache(self.store, ["AAA"], fetcher=fetch, now=1_800_000_000)
            second = refresh_news_cache(self.store, ["BBB"], fetcher=fetch, now=1_800_000_001)
            third = refresh_news_cache(self.store, ["AAA", "BBB"], fetcher=fetch, now=1_800_000_002)

        self.assertEqual(first["symbols_checked"], 1)
        self.assertEqual(second["symbols_checked"], 1)
        self.assertEqual(calls, [["AAA"], ["BBB"]])
        self.assertEqual(third["status"], "cached")
        self.assertEqual(cached_news_for_symbol(self.store, "AAA")[0]["headline"], "AAA announces contract")
        self.assertEqual(cached_news_for_symbol(self.store, "BBB")[0]["headline"], "BBB announces contract")

    def test_missing_api_key_does_not_attempt_network(self):
        with patch.dict("os.environ", {}, clear=True):
            result = refresh_news_cache(self.store, ["TEST"], fetcher=lambda symbols, key: self.fail("unexpected fetch"))
        self.assertEqual(result["status"], "not_configured")


if __name__ == "__main__":
    unittest.main()
