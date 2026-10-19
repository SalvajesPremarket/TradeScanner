import unittest

from tradescanner.core import apply_filters, normalize_schwab_quotes


class NormalizeSchwabQuotesTests(unittest.TestCase):
    def test_normalizes_present_quote_fields_and_calculates_gap(self):
        rows = normalize_schwab_quotes({
            "aapl": {
                "quote": {
                    "lastPrice": 105.0,
                    "closePrice": 100.0,
                    "netChange": 5.0,
                    "netPercentChange": 5.0,
                    "totalVolume": 1200,
                    "quoteTimeInLong": 123456,
                },
                "reference": {"description": "Apple Inc.", "exchangeName": "NASDAQ"},
            }
        })
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "AAPL")
        self.assertAlmostEqual(rows[0]["gap_pct"], 5.0, places=8)
        self.assertIsNone(rows[0]["sector"])
        self.assertEqual(rows[0]["exchange"], "NASDAQ")
        self.assertIsNone(rows[0]["float_shares"])
        self.assertIsNone(rows[0]["rvol"])

    def test_quote_freshness_requires_recent_provider_timestamp(self):
        import time
        now_ms = int(time.time() * 1000)
        rows = normalize_schwab_quotes({
            "FRESH": {"quote": {"lastPrice": 10, "closePrice": 9, "quoteTimeInLong": now_ms}, "reference": {}},
            "STALE": {"quote": {"lastPrice": 10, "closePrice": 9, "quoteTimeInLong": now_ms - 600_000}, "reference": {}},
            "MISSING": {"quote": {"lastPrice": 10, "closePrice": 9}, "reference": {}},
        })
        self.assertTrue(rows[0]["quote_is_fresh"])
        self.assertFalse(rows[1]["quote_is_fresh"])
        self.assertFalse(rows[2]["quote_is_fresh"])

    def test_uses_schwab_rest_quote_time_field_for_freshness(self):
        import time
        now_ms = int(time.time() * 1000)
        rows = normalize_schwab_quotes({
            "FRESH": {"quote": {"lastPrice": 10, "closePrice": 9, "quoteTime": now_ms}, "reference": {}},
            "STALE": {"quote": {"lastPrice": 10, "closePrice": 9, "quoteTime": now_ms - 600_000}, "reference": {}},
        })
        self.assertEqual(rows[0]["quote_time_ms"], now_ms)
        self.assertTrue(rows[0]["quote_is_fresh"])
        self.assertFalse(rows[1]["quote_is_fresh"])

    def test_quote_time_prefers_current_rest_field_then_legacy_field(self):
        import time
        now_ms = int(time.time() * 1000)
        rows = normalize_schwab_quotes({
            "CURRENT": {"quote": {"lastPrice": 10, "quoteTime": now_ms, "quoteTimeInLong": now_ms - 600_000}, "reference": {}},
            "LEGACY": {"quote": {"lastPrice": 10, "quoteTimeInLong": now_ms}, "reference": {}},
        })
        self.assertTrue(rows[0]["quote_is_fresh"])
        self.assertTrue(rows[1]["quote_is_fresh"])

    def test_non_finite_and_non_positive_quote_values_are_not_used(self):
        rows = normalize_schwab_quotes({
            "BAD": {"quote": {"lastPrice": float("nan"), "closePrice": 0, "totalVolume": float("inf")}, "reference": {}}
        })
        self.assertIsNone(rows[0]["price"])
        self.assertIsNone(rows[0]["previous_close"])
        self.assertIsNone(rows[0]["volume"])
        self.assertIsNone(rows[0]["gap_pct"])

    def test_uses_only_explicit_schwab_float_not_shares_outstanding(self):
        rows = normalize_schwab_quotes({
            "AAPL": {
                "quote": {"lastPrice": 100, "closePrice": 99},
                "reference": {"description": "Apple"},
                "fundamental": {"sharesOutstanding": 1500000},
            },
            "XYZ": {
                "quote": {"lastPrice": 10, "closePrice": 9},
                "reference": {"description": "Example"},
                "fundamental": {"floatShares": 250000},
            },
        })
        self.assertIsNone(rows[0]["float_shares"])
        self.assertEqual(rows[1]["float_shares"], 250000)
        self.assertEqual(rows[1]["float_source"], "schwab")

    def test_missing_fields_are_not_invented(self):
        rows = normalize_schwab_quotes({"XYZ": {"quote": {}, "reference": {}}})
        self.assertIsNone(rows[0]["price"])
        self.assertIsNone(rows[0]["gap_pct"])
        self.assertIsNone(rows[0]["volume"])
        self.assertIsNone(rows[0]["rvol"])

    def test_filters_exclude_missing_values_only_when_filter_enabled(self):
        rows = [
            {"symbol": "A", "price": 5.0, "gap_pct": 10.0, "rvol": None, "volume": 1000},
            {"symbol": "B", "price": 8.0, "gap_pct": None, "rvol": 2.0, "volume": 3000},
        ]
        self.assertEqual([r["symbol"] for r in apply_filters(rows, min_price=4)], ["A", "B"])
        self.assertEqual([r["symbol"] for r in apply_filters(rows, min_gap_pct=5)], ["A"])
        self.assertEqual([r["symbol"] for r in apply_filters(rows, min_rvol=1)], ["B"])


if __name__ == "__main__":
    unittest.main()
