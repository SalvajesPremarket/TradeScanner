import unittest

from tradescanner.universe import UniverseUnavailableError, parse_symbol_directory


class UniverseTests(unittest.TestCase):
    def test_parses_live_listed_symbols_deduplicates_and_skips_test_or_invalid_symbols(self):
        content = (
            "Nasdaq Traded|Symbol|Security Name|Listing Exchange|Market Category|ETF|"
            "Round Lot Size|Test Issue|Financial Status|CQS Symbol|NASDAQ Symbol|NextShares\n"
            "Y|AAPL|Apple Inc. Common Stock|Q|Q|N|100|N|N||AAPL|N\n"
            "Y|ETF1|Sample ETF|Q|Q|Y|100|N|N||ETF1|N\n"
            "Y|BAD!|Invalid symbol|Q|Q|N|100|N|N||BAD!|N\n"
            "Y|TEST|Test Issue|Q|Q|N|100|Y|N||TEST|N\n"
            "Y|AAPL|Duplicate|Q|Q|N|100|N|N||AAPL|N\n"
            "File Creation Time: 101720261100|||||||||||\n"
        )
        result = parse_symbol_directory(content)
        self.assertEqual(result["symbols"], ["AAPL"])
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["generated_at"], "101720261100")

    def test_rejects_unknown_format(self):
        with self.assertRaises(UniverseUnavailableError):
            parse_symbol_directory("symbol|name\nAAPL|Apple\n")

    def test_rejects_empty_directory(self):
        with self.assertRaises(UniverseUnavailableError):
            parse_symbol_directory("Nasdaq Traded|Symbol|Security Name|x|x|x|x|x|x|x|x\n")


if __name__ == "__main__":
    unittest.main()
