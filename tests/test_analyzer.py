import unittest
from tradescanner.analyzer import analyze_candles


class AnalyzerTests(unittest.TestCase):
    def make_candles(self, count=240):
        rows = []
        base_minute = 1_800_000_000_000 // 60_000
        for index in range(count):
            close = 10.0 + index * 0.01
            rows.append({
                "candle_time_ms": (base_minute + index) * 60_000,
                "open": close - 0.01, "high": close + 0.02,
                "low": close - 0.02, "close": close, "volume": 1000,
            })
        return rows

    def test_indicators_use_only_completed_bars(self):
        candles = self.make_candles()
        result = analyze_candles(candles, quote_time_ms=candles[-1]["candle_time_ms"] + 30_000)
        self.assertTrue(result["ready"])
        self.assertEqual(result["candles_used"], len(candles) - 1)
        self.assertIsNotNone(result["ema200"])
        self.assertIsNotNone(result["bollinger_upper"])

    def test_rejects_insufficient_history(self):
        result = analyze_candles(self.make_candles(50))
        self.assertFalse(result["ready"])
        self.assertFalse(result["signal"])

    def test_signal_requires_close_breakout_volume_ema_and_positive_macd(self):
        candles = self.make_candles()
        last = candles[-1]
        last.update({"open": 14.0, "close": 15.0, "high": 15.1, "low": 13.9, "volume": 5000})
        result = analyze_candles(candles)
        self.assertTrue(result["ready"])
        self.assertTrue(result["signal"])
        self.assertEqual(result["reason"], "confirmed_close_breakout")

    def test_invalid_candles_are_ignored(self):
        candles = self.make_candles()
        candles.append({"candle_time_ms": 1, "open": 0, "high": 0, "low": 0, "close": 0, "volume": -1})
        result = analyze_candles(candles)
        self.assertTrue(result["ready"])


if __name__ == "__main__":
    unittest.main()
