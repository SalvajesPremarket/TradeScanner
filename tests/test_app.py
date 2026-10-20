import os
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from starlette.requests import Request

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
        self.assertIn("/api/strategies", paths)
        self.assertIn("/api/strategies/active-matches", paths)

    def test_automatic_scan_defaults_to_disabled(self):
        with patch.dict(os.environ, {}, clear=True), patch("app.get_engine_status", return_value={}):
            result = app_module.engine_status()
        self.assertFalse(result["automatic_scan_enabled"])

    def test_market_data_connection_requires_a_recent_quote(self):
        with patch.dict(os.environ, {}, clear=True), patch(
            "app.get_engine_status",
            return_value={"last_quote_received_at": "2000-01-01T00:00:00+00:00"},
        ):
            stale = app_module.status()
        self.assertFalse(stale["market_data_connected"])

        recent_time = datetime.now(timezone.utc).isoformat()
        with patch.dict(os.environ, {}, clear=True), patch(
            "app.get_engine_status",
            return_value={"last_quote_received_at": recent_time},
        ):
            recent = app_module.status()
        self.assertTrue(recent["market_data_connected"])

    def test_schwab_oauth_routes_are_registered(self):
        paths = {route.path for route in oauth_router.routes if hasattr(route, "path")}
        self.assertIn("/oauth/start", paths)
        self.assertIn("/oauth/callback", paths)
        self.assertNotIn("/oauth/token-ready", paths)

    def test_oauth_callback_rejects_state_mismatch_before_token_exchange(self):
        request = Request({
            "type": "http",
            "method": "GET",
            "path": "/oauth/callback",
            "headers": [(b"cookie", b"tradescanner_oauth_state=expected-state")],
            "query_string": b"",
            "server": ("test", 443),
            "scheme": "https",
            "client": ("test", 12345),
            "root_path": "",
        })
        with patch("tradescanner.oauth.urllib.request.urlopen") as upstream:
            with self.assertRaises(HTTPException) as raised:
                oauth_module.oauth_callback(
                    request, code="test-code", state="wrong-state"
                )
        self.assertEqual(raised.exception.status_code, 400)
        upstream.assert_not_called()

    def test_snapshot_status_never_claims_durability_is_verified(self):
        with patch("app.SnapshotStore") as store_class:
            store_class.return_value.stats.return_value = {
                "snapshot_count": 0, "symbols_seen": 0, "latest_snapshot_at": None
            }
            with patch.dict(os.environ, {}, clear=True):
                result = app_module.snapshots_status()
        self.assertEqual(result["durability_status"], "not_verified")
        self.assertIn("durable mounted volume", result["durability_note"])


    def test_oauth_callback_returns_token_without_in_memory_handoff(self):
        request = Request({
            "type": "http",
            "method": "GET",
            "path": "/oauth/callback",
            "headers": [(b"cookie", b"tradescanner_oauth_state=expected-state")],
            "query_string": b"",
            "server": ("test", 443),
            "scheme": "https",
            "client": ("test", 12345),
            "root_path": "",
        })
        with patch.dict(os.environ, {
            "SCHWAB_CLIENT_ID": "test-client",
            "SCHWAB_CLIENT_SECRET": "test-secret",
            "SCHWAB_REDIRECT_URI": "https://example.onrender.com/oauth/callback",
        }), patch("tradescanner.oauth.infisical_is_configured", return_value=True), patch(
            "tradescanner.oauth.write_secret"
        ) as store_secret, patch("tradescanner.oauth.urllib.request.urlopen") as upstream:
            upstream.return_value.__enter__.return_value.read.return_value = (
                b'{"refresh_token":"test-refresh-value"}'
            )
            response = oauth_module.oauth_callback(
                request, code="test-code", state="expected-state"
            )
        self.assertEqual(response.status_code, 200)
        store_secret.assert_called_once_with("SCHWAB_REFRESH_TOKEN", "test-refresh-value")
        self.assertNotIn(b"test-refresh-value", response.body)
        self.assertEqual(response.headers["cache-control"], "no-store, no-cache, must-revalidate")
        self.assertEqual(response.headers["referrer-policy"], "no-referrer")
        self.assertIn("tradescanner_oauth_state", response.headers.get("set-cookie", ""))
        self.assertFalse(hasattr(oauth_module, "_PENDING"))


    def test_strategy_api_requires_private_token_and_rejects_wrong_token(self):
        request = Request({
            "type": "http", "method": "GET", "path": "/api/strategies",
            "headers": [(b"x-strategy-admin-token", b"wrong-token")],
            "query_string": b"", "server": ("test", 443), "scheme": "https",
            "client": ("test", 123), "root_path": "",
        })
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(HTTPException) as missing:
                app_module._authorize_strategy_access(request)
        self.assertEqual(missing.exception.status_code, 503)
        with patch.dict(os.environ, {"TRADESCANNER_STRATEGY_ADMIN_TOKEN": "expected-secret"}):
            with self.assertRaises(HTTPException) as wrong:
                app_module._authorize_strategy_access(request)
        self.assertEqual(wrong.exception.status_code, 401)
        authorized = Request({
            "type": "http", "method": "GET", "path": "/api/strategies",
            "headers": [(b"x-strategy-admin-token", b"expected-secret")],
            "query_string": b"", "server": ("test", 443), "scheme": "https",
            "client": ("test", 123), "root_path": "",
        })
        with patch.dict(os.environ, {"TRADESCANNER_STRATEGY_ADMIN_TOKEN": "expected-secret"}):
            self.assertIsNone(app_module._authorize_strategy_access(authorized))

    def test_manual_scan_rejects_more_than_one_provider_batch(self):
        symbols = ",".join(f"SYM{i:03d}" for i in range(101))
        with self.assertRaises(HTTPException) as raised:
            app_module.scan(symbols)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertIn("100 unique symbols", raised.exception.detail)

    def test_active_strategy_filters_snapshots_and_excludes_explicitly_stale_quotes(self):
        rows = [
            {"symbol": "AAA", "gap_pct": 4, "quote_is_fresh": True},
            {"symbol": "BBB", "gap_pct": 2, "quote_is_fresh": True},
            {"symbol": "CCC", "gap_pct": 5, "quote_is_fresh": False},
        ]
        with patch("app.SnapshotStore") as store_class, patch("app.CandleStore") as candle_class, patch(
            "app.active_strategy",
            return_value={"id": "strategy-1", "name": "GAP mínimo", "language": "formula", "source": "gap_pct >= 3", "enabled": True},
        ), patch("app.strategy_matches", side_effect=lambda strategy, row: row.get("gap_pct", 0) >= 3):
            store = store_class.return_value
            store.latest.return_value = rows
            store.path = "unused-test-db"
            candle_class.return_value.rvol.return_value = {"rvol": None, "sessions_used": 0}
            result = app_module.snapshots_latest(limit=100)
        self.assertEqual([row["symbol"] for row in result["results"]], ["AAA"])
        self.assertEqual(result["active_strategy"]["id"], "strategy-1")


if __name__ == "__main__":
    unittest.main()
