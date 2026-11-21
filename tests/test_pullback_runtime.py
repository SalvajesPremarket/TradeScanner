import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from tradescanner.candles import DailyCandleStore, CandleStore
from tradescanner.pullback_runtime import evaluate, strategy_config, to_weekly
from tradescanner.strategies import is_momentum_scan_program, is_pullback_scan_program


ET = ZoneInfo("America/New_York")


def daily_rows(count=70):
    rows = []
    for index in range(count):
        stamp = int((datetime(2026, 1, 2, 16, 0, tzinfo=ET) + timedelta(days=index)).timestamp() * 1000)
        close = 10.0 + index * 0.05
        rows.append({
            "symbol": "TEST", "candle_time_ms": stamp,
            "open": close - 0.1, "high": close + 0.3,
            "low": close - 0.3, "close": close, "volume": 500_000 + index,
        })
    return rows


class PullbackRuntimeTests(unittest.TestCase):
    def test_weekly_bars_are_aggregated_from_daily_ohlcv(self):
        daily = daily_rows(70)
        weekly = to_weekly(daily)
        self.assertGreaterEqual(len(weekly), 10)
        self.assertEqual(weekly[0]["open"], daily[0]["open"])
        self.assertEqual(weekly[0]["volume"], sum(
            row["volume"] for row in daily
            if datetime.fromtimestamp(row["candle_time_ms"] / 1000, ET).isocalendar()[:2]
            == datetime.fromtimestamp(weekly[0]["candle_time_ms"] / 1000, ET).isocalendar()[:2]
        ))

    def test_saved_literal_configuration_is_read_without_executing_source(self):
        source = '''
STRATEGY_NAME = "Pullback"
CONFIG = {"price_min": 3.0, "float_max": 40000000, "session_start": dtime(5, 0)}
TF_CONFIG = {"D": {"min_bars": 70}, "W": {"min_bars": 55}}
def scan(df, ctx, timeframe="D"):
    pass
'''
        cfg, tf = strategy_config(source)
        self.assertEqual(cfg["price_min"], 3.0)
        self.assertEqual(cfg["float_max"], 40_000_000)
        self.assertEqual(cfg["session_start"].hour, 5)
        self.assertEqual(tf["D"]["min_bars"], 70)

    def test_pullback_contract_is_not_classified_as_momentum(self):
        strategy = {
            "name": "PullBackArteaga", "language": "python",
            "source": 'STRATEGY_NAME = "Pullback"\nCONFIG = {}\nTF_CONFIG = {}\ndef to_weekly(df): pass\ndef scan(df, ctx): pass',
        }
        self.assertTrue(is_pullback_scan_program(strategy))
        self.assertFalse(is_momentum_scan_program(strategy))

    def test_evaluator_reports_missing_daily_history_instead_of_using_intraday_bars(self):
        quote_time = int(datetime(2026, 11, 19, 10, 0, tzinfo=ET).timestamp() * 1000)
        result = evaluate([], {"symbol": "TEST", "quote_time_ms": quote_time, "float_shares": 10_000_000}, "")
        self.assertFalse(result["passed"])
        self.assertTrue(any("historial diario" in reason for reason in result["reasons"]))

    def test_daily_candles_are_isolated_from_intraday_candle_table(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "candles.sqlite3"
            row = daily_rows(1)[0]
            self.assertEqual(DailyCandleStore(path).save([row]), 1)
            self.assertEqual(len(DailyCandleStore(path).latest("TEST")), 1)
            self.assertEqual(CandleStore(path).latest("TEST"), [])


if __name__ == "__main__":
    unittest.main()
