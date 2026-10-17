import unittest

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

    def test_schwab_oauth_routes_are_registered(self):
        paths = {route.path for route in oauth_router.routes if hasattr(route, "path")}
        self.assertIn("/oauth/start", paths)
        self.assertIn("/oauth/callback", paths)
        self.assertIn("/oauth/token-ready", paths)


if __name__ == "__main__":
    unittest.main()
