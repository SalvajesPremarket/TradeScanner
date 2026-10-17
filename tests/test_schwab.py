import os
import unittest
from unittest.mock import patch

from tradescanner.schwab import (
    SchwabConfigurationError,
    get_quotes,
)


class SchwabAdapterTests(unittest.TestCase):
    def test_rejects_invalid_symbol_before_network_call(self):
        with self.assertRaisesRegex(ValueError, "Invalid ticker symbol"):
            get_quotes(["AAPL!"])

    def test_requires_new_schwab_credentials(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SchwabConfigurationError):
                get_quotes(["AAPL"])


if __name__ == "__main__":
    unittest.main()
