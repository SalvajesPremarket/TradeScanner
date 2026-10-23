import unittest

from tradescanner.momentum_runtime import _strategy_config, _strategy_side


class MomentumRuntimeTests(unittest.TestCase):
    def test_reads_long_side_from_original_source(self):
        self.assertEqual(_strategy_side({"source": 'SIDE = "LONG"'}), "LONG")

    def test_reads_short_side_from_original_source(self):
        self.assertEqual(_strategy_side({"source": 'SIDE = "SHORT"'}), "SHORT")

    def test_reads_literal_config_from_original_program(self):
        source = '''
SIDE = "SHORT"
CONFIG: dict = {
    "ema_fast": 15,
    "rvol_min": 7.5,
    "news_required": False,
    "float_max": 10_000_000,
}
class MomentumTrader:
    def on_tick(self, price, ts, day_volume=None): return []
    def _entry_filters(self, price, ts): return []
    def _manage_position(self, price, sec, dev): return []
'''
        config = _strategy_config({"source": source})
        self.assertEqual(config["ema_fast"], 15)
        self.assertEqual(config["rvol_min"], 7.5)
        self.assertFalse(config["news_required"])
        self.assertEqual(config["float_max"], 10_000_000)

    def test_ignores_executable_config_expressions(self):
        source = '''
CONFIG = {"ema_fast": __import__("os").system("false"), "rvol_min": 8.0}
'''
        config = _strategy_config({"source": source})
        self.assertNotIn("ema_fast", config)
        self.assertEqual(config["rvol_min"], 8.0)

    def test_does_not_execute_uploaded_python_to_detect_side(self):
        source = 'import os\nSIDE = os.getenv("SIDE", "SHORT")'
        self.assertEqual(_strategy_side({"source": source}), "LONG")


if __name__ == "__main__":
    unittest.main()
