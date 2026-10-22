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

    def test_thinkscript_supports_safe_indicators_and_historical_references(self):
        strategy = save_strategy("ThinkScript", "thinkscript", "plot scan = close > Average(close, 3);", store=self.store)
        self.assertTrue(strategy["execution_ready"])
        history = [
            {"open": 10, "high": 11, "low": 9, "close": 10, "volume": 100},
            {"open": 11, "high": 12, "low": 10, "close": 11, "volume": 110},
            {"open": 12, "high": 13, "low": 11, "close": 12, "volume": 120},
            {"open": 13, "high": 14, "low": 12, "close": 13, "volume": 130},
            {"open": 14, "high": 15, "low": 13, "close": 14, "volume": 140},
        ]
        self.assertTrue(strategy_matches(strategy, {"price": 14}, history))
        self.assertFalse(strategy_matches(strategy, {"price": 11}, history))
        previous = save_strategy("Previous close", "thinkscript", "plot scan = close > close[1];", store=self.store)
        self.assertTrue(strategy_matches(previous, {"price": 14}, history))
        highest = save_strategy("High breakout", "thinkscript", "plot scan = close >= Highest(high, 3);", store=self.store)
        self.assertFalse(strategy_matches(highest, {"price": 14}, history))

    def test_edit_preserves_id_and_delete_removes_strategy(self):
        saved = save_strategy("First name", "formula", "price > 5", store=self.store)
        edited = save_strategy("Renamed", "formula", "price > 10", strategy_id=saved["id"], store=self.store)
        self.assertEqual(edited["id"], saved["id"])
        self.assertEqual(edited["name"], "Renamed")
        delete_strategy(saved["id"], store=self.store)
        self.assertEqual(list_strategies(self.store), [])


if __name__ == "__main__":
    unittest.main()
