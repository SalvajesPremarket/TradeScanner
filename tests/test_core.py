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
        self.assertEqual(rows[0]["gap_pct"], 5.0)
        self.assertIsNone(rows[0]["sector"])
        self.assertEqual(rows[0]["exchange"], "NASDAQ")
        self.assertIsNone(rows[0]["float_shares"])
        self.assertIsNone(rows[0]["rvol"])

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
