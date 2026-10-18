import os
import unittest
from unittest.mock import patch

from fastapi import HTTPException

import app as app_module
import tradescanner.oauth as oauth_module
from app import app
from tradescanner.oauth import router as oauth_router


class AppEntrypointTests(unittest.TestCase):
    def test_app_imports_and_exposes_health_route(self):
        self.assertEqual(app.title, "TradeScanner")
        paths = {route.path for route in app.routes if hasattr(route, "path")}
        self.assertIn("/health", paths)
        self.assertIn("/api/status", paths)
        self.assertIn("/api/engine/status", paths)
        self.assertIn("/api/candles/{symbol}", paths)
        self.assertIn("/api/snapshots/latest", paths)

    def test_automatic_scan_defaults_to_disabled(self):
        with patch.dict(os.environ, {}, clear=True), patch("app.get_engine_status", return_value={}):
            result = app_module.engine_status()
        self.assertFalse(result["automatic_scan_enabled"])

    def test_schwab_oauth_routes_are_registered(self):
        paths = {route.path for route in oauth_router.routes if hasattr(route, "path")}
        self.assertIn("/oauth/start", paths)
        self.assertIn("/oauth/callback", paths)
        self.assertIn("/oauth/token-ready", paths)

    def test_pending_refresh_tokens_expire_and_are_single_use(self):
        oauth_module._PENDING.clear()
        with patch("tradescanner.oauth.time.monotonic", return_value=100.0):
            oauth_module._store_pending_token("session-a", "test-refresh")
        with patch("tradescanner.oauth.time.monotonic", return_value=200.0):
            self.assertEqual(oauth_module._consume_pending_token("session-a"), "test-refresh")
            self.assertIsNone(oauth_module._consume_pending_token("session-a"))

        with patch("tradescanner.oauth.time.monotonic", return_value=300.0):
            oauth_module._store_pending_token("session-b", "expired-refresh")
        with patch("tradescanner.oauth.time.monotonic", return_value=901.0):
            self.assertIsNone(oauth_module._consume_pending_token("session-b"))
        oauth_module._PENDING.clear()

    def test_manual_scan_rejects_more_than_one_provider_batch(self):
        symbols = ",".join(f"SYM{i:03d}" for i in range(101))
        with self.assertRaises(HTTPException) as raised:
            app_module.scan(symbols)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertIn("100 unique symbols", raised.exception.detail)


if __name__ == "__main__":
    unittest.main()
