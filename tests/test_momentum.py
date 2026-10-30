import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from tradescanner.momentum import _ema, quote_only_checks, scan


ET = ZoneInfo("America/New_York")


def candle_stamp(day, hour, minute):
    return int(datetime(2026, 10, day, hour, minute, tzinfo=ET).timestamp() * 1000)


class MomentumScanTests(unittest.TestCase):
    def test_quote_only_checks_keep_unchanged_jeanlong_thresholds(self):
        checks = quote_only_checks({
            "quote_time_ms": candle_stamp(30, 7, 0),
            "quote_price": 10.0,
            "prev_close": 9.0,
            "current_volume": 700_000,
            "float_shares": 50_000_000,
        }, {
            "session_start": datetime(2026, 10, 30, 4, 0, tzinfo=ET).time(),
            "session_end": datetime(2026, 10, 30, 16, 0, tzinfo=ET).time(),
            "price_min": 1.0,
            "price_max": 20.0,
            "gap_min_pct": 2.0,
            "volume_min": 750_000,
            "float_max": 50_000_000,
        })
        self.assertTrue(checks["hora"][0])
        self.assertTrue(checks["precio"][0])
        self.assertTrue(checks["gap_pct"][0])
        self.assertFalse(checks["volumen_minimo"][0])
        self.assertFalse(checks["float"][0])

    def test_rejects_previous_session_candles_for_fresh_quote(self):
        candles = []
        start = datetime(2026, 10, 29, 4, 0, tzinfo=ET)
        for index in range(40):
            close = 10.0 + index * 0.01
            candles.append({
                "candle_time_ms": int((start + timedelta(minutes=index)).timestamp() * 1000),
                "open": close - 0.005, "high": close + 0.01,
                "low": close - 0.01, "close": close, "volume": 1000,
            })
        result = scan(candles, {
            "symbol": "TEST",
            "quote_time_ms": candle_stamp(30, 4, 39),
            "quote_price": 10.5,
            "current_volume": 1_000_000,
            "prev_close": 10.0,
            "rvol": 3.0,
            "float_shares": 10_000_000,
            "news": [],
        })
        self.assertFalse(result["passed"])
        self.assertIn(
            "velas: falta la vela de 1 minuto correspondiente a la cotización actual",
            result["reasons"],
        )

    def test_uses_current_quote_minute_for_ema_and_macd(self):
        start = datetime(2026, 10, 30, 4, 0, tzinfo=ET)
        candles = []
        closes = [10.0 + index * 0.01 for index in range(49)]
        ema_before = _ema(closes[:-1], 20)[-1]
        for index, close in enumerate(closes):
            stamp = start + timedelta(minutes=index)
            candle_open = close - 0.005
            candle_close = close
            if index == 48:
                candle_open = ema_before * 1.005
            candles.append({
                "candle_time_ms": int(stamp.timestamp() * 1000),
                "open": candle_open,
                "high": max(candle_open, candle_close) + 0.01,
                "low": min(candle_open, candle_close) - 0.01,
                "close": candle_close,
                "volume": 20_000,
            })
        quote_minute = start + timedelta(minutes=48)
        result = scan(candles, {
            "symbol": "TEST",
            "quote_time_ms": int((quote_minute + timedelta(seconds=30)).timestamp() * 1000),
            "quote_price": 10.5,
            "current_volume": 1_000_000,
            "prev_close": 10.0,
            "rvol": 3.0,
            "float_shares": 10_000_000,
            "news": [],
        }, {"news_required": False, "macd_require_positive_histogram": True, "timeframe": "1m"})
        self.assertEqual(result["metrics"]["timeframe"], "1m")
        self.assertGreater(result["metrics"]["macd_histogram"], 0)
        self.assertGreater(result["metrics"]["apertura_sobre_ema20"], 0)
        self.assertTrue(result["passed"], result["reasons"])
        self.assertFalse(any("falta la vela de 1 minuto" in reason for reason in result["reasons"]))


if __name__ == "__main__":
    unittest.main()
