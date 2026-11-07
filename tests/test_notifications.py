import unittest

from tradescanner.notifications import _signal_key


class NotificationTests(unittest.TestCase):
    def test_signal_key_is_stable_and_scoped_to_strategy_symbol_timestamp(self):
        action = {"action": "ENTER", "symbol": "ABCD", "quote_time_ms": 123456, "price": 4.2}
        first = _signal_key(action, {"id": "s1"})
        self.assertEqual(first, _signal_key(dict(action), {"id": "s1"}))
        self.assertNotEqual(first, _signal_key(action, {"id": "s2"}))
        self.assertNotEqual(first, _signal_key({**action, "quote_time_ms": 123457}, {"id": "s1"}))

    def test_action_type_is_part_of_signal_key(self):
        base = {"symbol": "ABCD", "quote_time_ms": 123456}
        self.assertNotEqual(
            _signal_key({**base, "action": "ENTER"}, {"id": "s1"}),
            _signal_key({**base, "action": "EXIT"}, {"id": "s1"}),
        )


if __name__ == "__main__":
    unittest.main()
