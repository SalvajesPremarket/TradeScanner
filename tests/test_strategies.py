import tempfile
import unittest
from pathlib import Path

from tradescanner.snapshots import SnapshotStore
from tradescanner.strategies import (
    active_strategy,
    delete_strategy,
    formula_matches,
    strategy_matches,
    list_strategies,
    is_momentum_scan_program,
    save_strategy,
    set_strategy_enabled,
)


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SnapshotStore(Path(self.tempdir.name) / "test.sqlite3")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_formula_matches_only_rows_satisfying_expression(self):
        expression = "gap_pct >= 3 and volume >= 500000"
        self.assertTrue(formula_matches(expression, {"gap_pct": 4, "volume": 900000}))
        self.assertFalse(formula_matches(expression, {"gap_pct": 2, "volume": 900000}))
        self.assertFalse(formula_matches(expression, {"gap_pct": 4, "volume": None}))

    def test_negated_missing_field_does_not_match(self):
        self.assertFalse(formula_matches("not (gap_pct > 3)", {"gap_pct": None}))
        self.assertFalse(formula_matches("not (gap_pct > 3)", {}))

    def test_formula_rejects_python_execution_constructs(self):
        with self.assertRaises(ValueError):
            save_strategy("Unsafe", "formula", "__import__('os').system('echo nope')", store=self.store)

    def test_formula_complexity_and_large_constants_are_rejected(self):
        with self.assertRaises(ValueError):
            save_strategy("Too complex", "formula", " and ".join(["True"] * 400), store=self.store)
        with self.assertRaises(ValueError):
            save_strategy("Huge string", "formula", repr("x" * 257) + " == 'x'", store=self.store)

    def test_string_repetition_is_bounded_during_evaluation(self):
        self.assertFalse(formula_matches("'x' * 1000000000 == 'x'", {}))

    def test_boolean_operators_short_circuit_missing_values(self):
        self.assertFalse(formula_matches("gap_pct > 3 and volume > 10", {"gap_pct": 1, "volume": 100}))
        self.assertTrue(formula_matches("gap_pct > 3 or volume > 10", {"gap_pct": 1, "volume": 100}))

    def test_only_one_formula_strategy_can_be_active(self):
        first = save_strategy("GAP", "formula", "gap_pct >= 3", store=self.store)
        second = save_strategy("Volume", "formula", "volume >= 500000", store=self.store)
        set_strategy_enabled(first["id"], True, store=self.store)
        set_strategy_enabled(second["id"], True, store=self.store)
        self.assertEqual(active_strategy(self.store)["id"], second["id"])
        items = list_strategies(self.store)
        self.assertEqual(sum(bool(item["enabled"]) for item in items), 1)

    def test_restricted_python_expression_and_function_are_executable(self):
        expression = save_strategy("Python expression", "python", "gap_pct >= 3 and volume >= 500000", store=self.store)
        self.assertTrue(expression["execution_ready"])
        self.assertTrue(strategy_matches(expression, {"gap_pct": 4, "volume": 800000}))
        function = save_strategy(
            "Python function", "python",
            "def evaluate(stock): return stock['gap_pct'] >= 3 and stock['volume'] >= 500000",
            store=self.store,
        )
        self.assertTrue(strategy_matches(function, {"gap_pct": 4, "volume": 800000}))
        self.assertFalse(strategy_matches(function, {"gap_pct": 1, "volume": 800000}))

    def test_python_rejects_imports_and_arbitrary_attribute_access(self):
        with self.assertRaises(ValueError):
            save_strategy("Unsafe import", "python", "import os", store=self.store)
        with self.assertRaises(ValueError):
            save_strategy("Unsafe attribute", "python", "def evaluate(stock): return stock.__class__", store=self.store)

    def test_active_strategy_can_be_edited_to_supported_python_adapter(self):
        strategy = save_strategy("Editable", "formula", "gap_pct >= 3", store=self.store)
        set_strategy_enabled(strategy["id"], True, store=self.store)
        edited = save_strategy(
            "Editable Python", "python",
            "def evaluate(stock): return stock['gap_pct'] >= 3",
            strategy_id=strategy["id"], store=self.store,
        )
        self.assertTrue(edited["enabled"])
        self.assertTrue(strategy_matches(edited, {"gap_pct": 4}))
        self.assertEqual(active_strategy(self.store)["language"], "python")

    def test_simple_thinkscript_style_scan_is_supported_but_history_is_not(self):
        strategy = save_strategy("ThinkScript", "thinkscript", "plot scan = close > 5;", store=self.store)
        self.assertTrue(strategy["execution_ready"])
        self.assertTrue(strategy_matches(strategy, {"price": 10}))
        self.assertFalse(strategy_matches(strategy, {"price": 4}))
        with self.assertRaises(ValueError):
            save_strategy("History unsupported", "thinkscript", "plot scan = close > Average(close, 20);", store=self.store)

    def test_edit_preserves_id_and_delete_removes_strategy(self):
        saved = save_strategy("First name", "formula", "price > 5", store=self.store)
        edited = save_strategy("Renamed", "formula", "price > 10", strategy_id=saved["id"], store=self.store)
        self.assertEqual(edited["id"], saved["id"])
        self.assertEqual(edited["name"], "Renamed")
        delete_strategy(saved["id"], store=self.store)
        self.assertEqual(list_strategies(self.store), [])

    def test_original_scan_based_momentum_program_can_be_activated(self):
        source = '''
from datetime import time as dtime
STRATEGY_NAME = "Momentum"
CONFIG: dict = {"session_start": dtime(4, 0), "price_min": 1.0}
def scan(df, ctx, cfg=None):
    return {"passed": False, "metrics": {}, "reasons": []}
'''
        strategy = save_strategy("JeanLong", "python", source, store=self.store)
        self.assertTrue(is_momentum_scan_program(strategy))
        self.assertTrue(strategy["execution_ready"], strategy["execution_error"])
        activated = set_strategy_enabled(strategy["id"], True, store=self.store)
        self.assertTrue(activated["enabled"])
        self.assertFalse(strategy_matches(activated, {"price": 10}))

    def test_original_momentum_trader_program_can_be_activated_without_execing_upload(self):
        source = '''
class MomentumTrader:
    def on_tick(self, price, ts, day_volume=None):
        return []
    def _entry_filters(self, price, ts):
        return []
    def _manage_position(self, price, sec, dev):
        return []
'''
        strategy = save_strategy("JeanLong", "python", source, store=self.store)
        self.assertTrue(strategy["execution_ready"])
        self.assertTrue(strategy["enabled"] is False)
        activated = set_strategy_enabled(strategy["id"], True, store=self.store)
        self.assertTrue(activated["enabled"])
        self.assertFalse(strategy_matches(activated, {"price": 10}))


if __name__ == "__main__":
    unittest.main()
