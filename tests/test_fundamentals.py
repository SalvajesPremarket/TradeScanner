import tempfile
import unittest
from pathlib import Path

from tradescanner.fundamentals import FloatStore, normalize_float_payload


class FloatDataTests(unittest.TestCase):
    def test_normalizes_only_matching_symbol_with_real_float_value(self):
        payload = [
            {"symbol": "MSFT", "floatShares": 123},
            {"symbol": "AAPL", "floatShares": 1000, "freeFloat": 90.5,
             "outstandingShares": 1100, "date": "2026-10-16", "source": "https://sec.gov/example"},
        ]
        result = normalize_float_payload("aapl", payload)
        self.assertEqual(result["float_shares"], 1000)
        self.assertEqual(result["free_float_pct"], 90.5)
        self.assertEqual(result["data_date"], "2026-10-16")
        self.assertIsNone(normalize_float_payload("TSLA", payload))

    def test_float_is_cached_and_not_refetched_within_ttl(self):
        with tempfile.TemporaryDirectory() as directory:
            store = FloatStore(Path(directory) / "float.sqlite3")
            calls = []

            def fetch(symbol):
                calls.append(symbol)
                return [{"symbol": symbol, "floatShares": 2500, "date": "2026-10-16"}]

            first = store.get_or_fetch("aapl", fetcher=fetch)
            second = store.get_or_fetch("AAPL", fetcher=fetch)
            self.assertEqual(first["float_shares"], 2500)
            self.assertEqual(second["float_shares"], 2500)
            self.assertEqual(calls, ["AAPL"])

    def test_missing_float_is_not_fabricated(self):
        self.assertIsNone(normalize_float_payload("AAPL", [{"symbol": "AAPL"}]))


if __name__ == "__main__":
    unittest.main()
