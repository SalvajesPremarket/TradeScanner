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
        self.assertIn("/api/quote/{symbol}", paths)
        self.assertIn("/api/snapshots/latest", paths)
        self.assertIn("/api/strategies", paths)
        self.assertIn("/api/strategies/active-matches", paths)
        self.assertNotIn("/api/diagnostics/float/fmp", paths)
        self.assertNotIn("/api/diagnostics/float/fmp/resolve-legacy-cooldown", paths)
        self.assertNotIn("/api/diagnostics/float/fmp/refresh-cache", paths)

    def test_automatic_scan_defaults_to_disabled(self):
        with patch.dict(os.environ, {}, clear=True), patch("app.get_engine_status", return_value={}):
            result = app_module.engine_status()
        self.assertFalse(result["automatic_scan_enabled"])

    def test_schwab_provider_is_configured_when_refresh_token_is_in_infisical(self):
        env = {
            "SCHWAB_CLIENT_ID": "test-client",
            "SCHWAB_CLIENT_SECRET": "test-secret",
            "SCHWAB_REFRESH_TOKEN": "",
        }
        with patch.dict(os.environ, env, clear=True), patch(
            "app.infisical_is_configured", return_value=True
        ), patch("app.get_engine_status", return_value={}):
            result = app_module.status()
        self.assertTrue(result["provider_configured"])
        self.assertFalse(result["market_data_connected"])

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


    def test_strategy_api_requires_neon_bearer_auth(self):
        request = Request({
            "type": "http", "method": "GET", "path": "/api/strategies",
            "headers": [(b"x-strategy-admin-token", b"legacy-token")],
            "query_string": b"", "server": ("test", 443), "scheme": "https",
            "client": ("test", 123), "root_path": "",
        })
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(HTTPException) as missing_config:
                app_module._authorize_strategy_access(request)
        self.assertEqual(missing_config.exception.status_code, 503)

        with patch.dict(os.environ, {
            "NEON_AUTH_JWKS_URL": "https://auth.example/.well-known/jwks.json",
            "NEON_AUTH_ISSUER": "https://auth.example",
        }, clear=True):
            with self.assertRaises(HTTPException) as missing_token:
                app_module._authorize_strategy_access(request)
        self.assertEqual(missing_token.exception.status_code, 401)

    def test_strategy_endpoints_fail_closed_without_neon_configuration(self):
        request = Request({
            "type": "http", "method": "GET", "path": "/api/strategies",
            "headers": [], "query_string": b"", "server": ("test", 443),
            "scheme": "https", "client": ("test", 123), "root_path": "",
        })
        calls = [
            lambda: app_module.strategies_list(request),
            lambda: app_module.strategies_save(request, {"name": "Test", "language": "formula", "source": "True"}),
            lambda: app_module.strategies_activate("missing", request, {"enabled": True}),
            lambda: app_module.strategies_delete("missing", request),
            lambda: app_module.strategies_active_matches(request),
        ]
        with patch.dict(os.environ, {}, clear=True):
            for call in calls:
                with self.subTest(endpoint=call):
                    with self.assertRaises(HTTPException) as raised:
                        call()
                    self.assertEqual(raised.exception.status_code, 503)

    def test_manual_scan_rejects_more_than_one_provider_batch(self):
        symbols = ",".join(f"SYM{i:03d}" for i in range(101))
        with self.assertRaises(HTTPException) as raised:
            app_module.scan(symbols)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertIn("100 unique symbols", raised.exception.detail)

    def test_radar_keeps_snapshots_independent_of_strategy_evaluations(self):
        rows = [
            {"symbol": "AAA", "gap_pct": 4, "quote_is_fresh": True, "quote_time_ms": 1000, "trade_time_ms": 900},
            {"symbol": "BBB", "gap_pct": 2, "quote_is_fresh": True, "quote_time_ms": 2000},
            {"symbol": "CCC", "gap_pct": 5, "quote_is_fresh": False},
            {"symbol": "DDD", "gap_pct": 5},
        ]
        with patch("app.SnapshotStore") as store_class, patch("app.CandleStore") as candle_class, patch(
            "app.active_strategy",
            return_value={"id": "strategy-1", "name": "GAP mínimo", "language": "formula", "source": "gap_pct >= 3", "enabled": True},
        ), patch("app.strategy_matches", side_effect=lambda strategy, row: row.get("gap_pct", 0) >= 3):
            store = store_class.return_value
            store.latest.return_value = rows
            store.path = "unused-test-db"
            candle_class.return_value.rvol.return_value = {"rvol": None, "sessions_used": 0}
            candle_class.return_value.premarket_volume.side_effect = lambda symbol, as_of_ms=None: 1234 if symbol == "AAA" else None
            result = app_module.snapshots_latest(limit=100, timeframe="1m")
        self.assertEqual([row["symbol"] for row in result["results"]], ["AAA", "BBB", "CCC", "DDD"])
        self.assertEqual(result["active_strategy"]["id"], "strategy-1")
        self.assertEqual(result["results"][0]["premarket_volume"], 1234)
        candle_class.return_value.premarket_volume.assert_any_call("AAA", as_of_ms=900)
        self.assertIsNone(result["results"][1]["premarket_volume"])
        self.assertIsNone(result["results"][2]["premarket_volume"])


    def test_snapshots_latest_applies_cached_float_to_rows(self):
        rows = [{"symbol": "AAPL", "float_shares": None, "quote_is_fresh": True}]
        def apply_float(cached_rows, store):
            cached_rows[0].update({
                "float_shares": 123456789,
                "float_as_of": "2026-10-01",
                "float_source": "massive",
            })
            return cached_rows

        with patch("app.SnapshotStore") as store_class, patch("app.CandleStore") as candle_class, patch(
            "app.active_strategy", return_value=None
        ), patch("app.apply_cached_float", side_effect=apply_float) as apply_cached:
            store = store_class.return_value
            store.latest.return_value = rows
            store.path = "unused-test-db"
            candle_class.return_value.rvol.return_value = {"rvol": None, "sessions_used": 0}
            result = app_module.snapshots_latest(limit=100, timeframe="1m")

        apply_cached.assert_called_once_with(rows, store)
        self.assertEqual(result["results"][0]["float_shares"], 123456789)
        self.assertEqual(result["results"][0]["float_source"], "massive")
        self.assertEqual(result["results"][0]["float_as_of"], "2026-10-01")


    def test_radar_volume_filter_uses_session_cumulative_volume_since_0400_et(self):
        html = (app_module.ROOT / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="volume-max"', html)
        self.assertIn('id="volume-max" type="number" min="0" step="1000" placeholder="MAX"', html)
        self.assertNotIn('id="volume-max" type="number" min="0" step="1000" value=', html)
        self.assertIn('VOL. ACUM. DESDE 04:00 ET', html)
        self.assertIn('item.premarket_volume', html)
        self.assertIn('["volume-max", maxVolume, item.premarket_volume, "max"]', html)
        self.assertIn('id === "volume-max" && Number(actual) >= threshold', html)
        self.assertNotIn('["volume-min", minVolume, item.volume, "min"]', html)

    def test_chart_is_integrated_into_scanner_and_standalone_tab_is_removed(self):
        html = (app_module.ROOT / "index.html").read_text(encoding="utf-8")
        self.assertNotIn('data-tab="charts"', html)
        self.assertNotIn('id="tab-charts"', html)
        scanner_start = html.find('data-tab="screener"')
        chart_position = html.find('id="integrated-chart"')
        strategies_position = html.find('id="tab-strategies"')
        self.assertGreaterEqual(scanner_start, 0)
        self.assertGreater(chart_position, scanner_start)
        self.assertGreater(strategies_position, chart_position)
        self.assertIn('button.addEventListener("click", () => showTab(button.dataset.tab))', html)
        self.assertIn("function showTab(name)", html)
        self.assertIn('BÚSQUEDA BÁSICA · ', html)
        self.assertIn('JEANLONG · ', html)
        self.assertIn('FILTROS BÁSICOS OK', html)
        self.assertIn('basic_filter_passed', html)
        self.assertIn('sourceRows = allResults;', html)
        self.assertIn('Radar independiente · ', html)
        self.assertNotIn('allResults.filter(item => passedSymbols.has', html)
        self.assertIn('del lote evaluado cumplen precio/GAP/volumen/FLOAT · muestra parcial del universo', html)

    def test_secondary_tabs_have_real_controls_and_data_endpoints(self):
        html = (app_module.ROOT / "index.html").read_text(encoding="utf-8")
        required = [
            'id="chart-form"',
            'fetch("/api/candles/"',
            'id="watchlist-form"',
            'localStorage.getItem("tradescanner_watchlist")',
            'localStorage.setItem("tradescanner_watchlist"',
            'id="refresh-settings"',
            'fetch("/api/snapshots/status"',
            'fetch("/api/engine/status"',
            'fetch("/api/universe/status"',
            'id="strategy-form"',
            'fetch("/api/strategies"',
            'function drawCandles(candles, liveQuote = null)',
            'fetch("/api/quote/"',
            'setInterval(() => void refreshChartQuote(symbol, sequence), 2000)',
            'id="chart-live-price"',
            'event.target.closest(".chart-ticker-link")',
        ]
        for fragment in required:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, html)


    def test_selected_chart_quote_comes_from_schwab_without_persisting_snapshots(self):
        quote = {"symbol": "AAPL", "price": 123.45, "quote_time_ms": 1_800_000_000_000}
        with patch("app.get_quotes", return_value={"AAPL": {}}) as fetch_quotes, patch(
            "app.normalize_schwab_quotes", return_value=[quote]
        ), patch("app.SnapshotStore") as store_class:
            result = app_module.quote_status("aapl")
        fetch_quotes.assert_called_once_with(["AAPL"])
        store_class.assert_not_called()
        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["provider"], "Schwab")
        self.assertEqual(result["price"], 123.45)
        self.assertEqual(result["source"], "schwab_quote")
        self.assertIn("is_live", result)

    def test_chart_endpoint_accepts_thirty_minute_timeframe(self):
        candle = {
            "candle_time_ms": 1_800_000,
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 100,
        }
        with patch("app.SnapshotStore") as store_class, patch("app.CandleStore") as candle_class, patch(
            "app.get_chart_history", return_value={"candles": [candle]}
        ), patch("tradescanner.candles.normalize_price_history", return_value=[candle]):
            store_class.return_value.path = "unused-test-db"
            candle_class.return_value.rvol.return_value = {"rvol": None, "sessions_used": 0}
            result = app_module.candles_status("AAPL", limit=100, timeframe="30m")

        self.assertEqual(result["timeframe"], "30m")
        self.assertEqual(result["timeframe_label"], "30 minutos")
        self.assertEqual(len(result["candles"]), 1)

    def test_scanner_aggregates_thirty_minute_candles(self):
        candles = [
            {"candle_time_ms": 1_800_000, "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
            {"candle_time_ms": 1_860_000, "open": 10.5, "high": 12, "low": 10, "close": 11.5, "volume": 150},
            {"candle_time_ms": 3_600_000, "open": 11.5, "high": 13, "low": 11, "close": 12.5, "volume": 200},
        ]
        aggregated = app_module._aggregate_scanner_candles(candles, "30m")
        self.assertEqual(len(aggregated), 2)
        self.assertEqual(aggregated[0]["high"], 12)
        self.assertEqual(aggregated[0]["low"], 9)
        self.assertEqual(aggregated[0]["close"], 11.5)
        self.assertEqual(aggregated[0]["volume"], 250)

    def test_daily_and_weekly_scanner_indicators_accept_schwab_candles(self):
        candles = [
            {"candle_time_ms": index * 86_400_000, "close": 10 + index / 10}
            for index in range(30)
        ]
        daily = app_module._scanner_indicators(candles, None, "1d")
        weekly = app_module._scanner_indicators(candles, None, "1w")
        self.assertIsNotNone(daily["ema20"])
        self.assertIsNotNone(weekly["ema20"])


if __name__ == "__main__":
    unittest.main()
