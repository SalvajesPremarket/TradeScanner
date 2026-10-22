import unittest
from datetime import datetime, timezone

from tradescanner.momentum import DEFAULT_CONFIG, evaluate_momentum


class MomentumTests(unittest.TestCase):
    def test_missing_data_is_reported_instead_of_fabricated(self):
        result = evaluate_momentum([], {"symbol": "TEST"}, now=datetime(2026, 10, 22, tzinfo=timezone.utc))
        self.assertFalse(result["passed"])
        self.assertFalse(result["checks"]["history"])
        self.assertIsNone(result["metrics"]["gap_pct"])
        self.assertIn("news", result["checks"])

    def test_ema_and_macd_metrics_are_computed_from_candles(self):
        candles = []
        for i in range(40):
            stamp = datetime(2026, 10, 22, 8, 0, tzinfo=timezone.utc).timestamp() + i * 60
            candles.append({"timestamp": stamp, "close": 5 + i * 0.1, "volume": 1000})
        config = {**DEFAULT_CONFIG, "min_bars": 35, "news_required": False}
        result = evaluate_momentum(
            candles,
            {"symbol": "TEST", "prev_close": 5, "avg_volume_30d": 100, "float_shares": 1_000_000},
            config,
            now=datetime(2026, 10, 22, 10, 0, tzinfo=timezone.utc),
        )
        self.assertIsNotNone(result["metrics"]["ema"])
        self.assertIsNotNone(result["metrics"]["macd"])
        self.assertEqual(len(result["checks"]), 9)

    def test_configurable_news_keyword_requirement(self):
        now = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)
        context = {"news": [{"published_at": "2026-10-22T11:00:00Z", "headline": "Company announces new contract"}]}
        result = evaluate_momentum([], context, {"news_required": True, "news_require_keyword": True}, now=now)
        self.assertTrue(result["checks"]["news"])
        context["news"] = [{"published_at": "2026-10-22T11:00:00Z", "headline": "Market update"}]
        result = evaluate_momentum([], context, {"news_required": True, "news_require_keyword": True}, now=now)
        self.assertFalse(result["checks"]["news"])


if __name__ == "__main__":
    unittest.main()
