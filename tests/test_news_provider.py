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

    def test_missing_api_key_does_not_attempt_network(self):
        with patch.dict("os.environ", {}, clear=True):
            result = refresh_news_cache(self.store, ["TEST"], fetcher=lambda symbols, key: self.fail("unexpected fetch"))
        self.assertEqual(result["status"], "not_configured")


if __name__ == "__main__":
    unittest.main()
