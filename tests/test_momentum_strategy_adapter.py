import tempfile
import unittest
from pathlib import Path

from tradescanner.snapshots import SnapshotStore
from tradescanner.strategies import save_strategy, set_strategy_enabled, active_strategy


MOMENTUM_SHAPE = '''
CONFIG = {"price_min": 1, "price_max": 20, "news_required": True}
def _ema(values, period):
    return values
def _macd(close, fast, slow, signal):
    return close, signal
def _f_session(df, cfg):
    return True
def _f_price(price, cfg):
    return True
def _f_gap(price, prev_close, cfg):
    return True
def _f_rvol(volume, baseline, cfg):
    return True
def _f_float(float_shares, cfg):
    return True
def _f_first_candle_above_ema(df, cfg):
    return True
def _f_macd(df, cfg):
    return True
def _f_news(news, cfg):
    return True
def scan(df, ctx, cfg=None):
    return {"passed": True}
'''


class NativeMomentumStrategyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SnapshotStore(Path(self.tempdir.name) / "test.sqlite3")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_recognized_momentum_source_is_stored_as_native_not_executed(self):
        strategy = save_strategy("Momentum", "python", MOMENTUM_SHAPE, store=self.store)
        self.assertEqual(strategy["execution_mode"], "momentum_native")
        self.assertEqual(strategy["config"]["price_min"], 1)
        self.assertTrue(strategy["execution_ready"])
        activated = set_strategy_enabled(strategy["id"], True, store=self.store)
        self.assertTrue(activated["enabled"])
        self.assertEqual(active_strategy(self.store)["id"], strategy["id"])

    def test_unrecognized_full_python_script_is_rejected(self):
        with self.assertRaises(ValueError):
            save_strategy("Arbitrary script", "python", "import os\ndef run(): return True", store=self.store)


if __name__ == "__main__":
    unittest.main()
