import os
import unittest
from unittest.mock import patch

from tradescanner.schwab import (
    SchwabConfigurationError,
    SchwabAPIError,
    get_quotes,
    _clear_token_cache,
)


class SchwabAdapterTests(unittest.TestCase):
    def test_rejects_invalid_symbol_before_network_call(self):
        with self.assertRaisesRegex(ValueError, "Invalid ticker symbol"):
            get_quotes(["AAPL!"])

    def test_requires_new_schwab_credentials(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SchwabConfigurationError):
                get_quotes(["AAPL"])

    def test_reuses_access_token_across_quote_requests(self):
        _clear_token_cache()
        calls = []

        def fake_request(request, timeout=15):
            calls.append(request.full_url)
            if request.full_url == "https://api.schwabapi.com/v1/oauth/token":
                return {"access_token": "test-access-token", "expires_in": 3600}
            return {"AAPL": {"quote": {"lastPrice": 100.0}, "reference": {}}}

        with patch.dict(os.environ, {
            "SCHWAB_CLIENT_ID": "test-id",
            "SCHWAB_CLIENT_SECRET": "test-secret",
            "SCHWAB_REFRESH_TOKEN": "test-refresh",
        }), patch("tradescanner.schwab._request_json", side_effect=fake_request):
            get_quotes(["AAPL"])
            get_quotes(["AAPL"])

        self.assertEqual(calls.count("https://api.schwabapi.com/v1/oauth/token"), 1)
        self.assertEqual(sum(url.startswith("https://api.schwabapi.com/marketdata/v1/quotes") for url in calls), 2)
        _clear_token_cache()


    def test_identifies_oauth_stage_without_exposing_provider_body(self):
        _clear_token_cache()
        with patch.dict(os.environ, {
            "SCHWAB_CLIENT_ID": "test-id",
            "SCHWAB_CLIENT_SECRET": "test-secret",
            "SCHWAB_REFRESH_TOKEN": "test-refresh",
        }), patch("tradescanner.schwab._request_json", side_effect=SchwabAPIError("Schwab API returned HTTP 400.")):
            with self.assertRaisesRegex(SchwabAPIError, r"OAuth token request failed: Schwab API returned HTTP 400") as caught:
                get_quotes(["AAPL"])
        self.assertNotIn("test-secret", str(caught.exception))
        self.assertNotIn("test-refresh", str(caught.exception))
        _clear_token_cache()

    def test_identifies_quotes_stage_after_oauth_succeeds(self):
        _clear_token_cache()
        def fake_request(request, timeout=15):
            if request.full_url == "https://api.schwabapi.com/v1/oauth/token":
                return {"access_token": "test-access-token", "expires_in": 3600}
            raise SchwabAPIError("Schwab API returned HTTP 400.")

        with patch.dict(os.environ, {
            "SCHWAB_CLIENT_ID": "test-id",
            "SCHWAB_CLIENT_SECRET": "test-secret",
            "SCHWAB_REFRESH_TOKEN": "test-refresh",
        }), patch("tradescanner.schwab._request_json", side_effect=fake_request):
            with self.assertRaisesRegex(SchwabAPIError, r"market-data quotes request failed: Schwab API returned HTTP 400") as caught:
                get_quotes(["AAPL"])
        self.assertNotIn("test-secret", str(caught.exception))
        self.assertNotIn("test-refresh", str(caught.exception))
        _clear_token_cache()


if __name__ == "__main__":
    unittest.main()
