import unittest

from tradescanner.momentum_runtime import _strategy_side


class MomentumRuntimeTests(unittest.TestCase):
    def test_reads_long_side_from_original_source(self):
        self.assertEqual(_strategy_side({"source": 'SIDE = "LONG"'}), "LONG")

    def test_reads_short_side_from_original_source(self):
        self.assertEqual(_strategy_side({"source": 'SIDE = "SHORT"'}), "SHORT")

    def test_does_not_execute_uploaded_python_to_detect_side(self):
        source = 'import os\nSIDE = os.getenv("SIDE", "SHORT")'
        self.assertEqual(_strategy_side({"source": source}), "LONG")


if __name__ == "__main__":
    unittest.main()
