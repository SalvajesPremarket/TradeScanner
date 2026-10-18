import os
import unittest
from unittest.mock import patch

from tradescanner import infisical_vault as vault


class InfisicalVaultTests(unittest.TestCase):
    def setUp(self):
        vault._reset_cache_for_tests()
        self.env = patch.dict(os.environ, {
            "INFISICAL_CLIENT_ID": "client-id",
            "INFISICAL_CLIENT_SECRET": "client-secret",
            "INFISICAL_PROJECT_ID": "project-id",
            "INFISICAL_ENVIRONMENT": "prod",
            "INFISICAL_SECRET_PATH": "/",
        })
        self.env.start()

    def tearDown(self):
        self.env.stop()
        vault._reset_cache_for_tests()

    def test_configuration_requires_machine_identity_and_project(self):
        with patch.dict(os.environ, {"INFISICAL_CLIENT_SECRET": ""}):
            self.assertFalse(vault.is_configured())

    def test_read_secret_returns_value_without_logging_it(self):
        with patch.object(vault, "_access_token", return_value="test-access"), patch.object(
            vault, "_json_request", return_value={"secret": {"secretValue": "private-value"}}
        ):
            self.assertEqual(vault.read_secret("SCHWAB_REFRESH_TOKEN"), "private-value")

    def test_write_secret_creates_missing_secret(self):
        requests = []

        def fake_request(request, timeout=12):
            requests.append(request)
            if request.method == "GET":
                raise __import__("urllib.error").error.HTTPError(
                    request.full_url, 404, "not found", {}, None
                )
            return {"secret": {"secretKey": "SCHWAB_REFRESH_TOKEN"}}

        with patch.object(vault, "_access_token", return_value="test-access"), patch.object(
            vault, "_json_request", side_effect=fake_request
        ):
            vault.write_secret("SCHWAB_REFRESH_TOKEN", "private-value")

        self.assertEqual(requests[-1].method, "POST")
        self.assertNotIn("private-value", requests[-1].full_url)

    def test_write_secret_updates_existing_secret(self):
        requests = []

        def fake_request(request, timeout=12):
            requests.append(request)
            if request.method == "GET":
                return {"secret": {"secretValue": "old-value"}}
            return {"secret": {"secretKey": "SCHWAB_REFRESH_TOKEN"}}

        with patch.object(vault, "_access_token", return_value="test-access"), patch.object(
            vault, "_json_request", side_effect=fake_request
        ):
            vault.write_secret("SCHWAB_REFRESH_TOKEN", "new-private-value")

        self.assertEqual(requests[-1].method, "PATCH")
        self.assertNotIn("new-private-value", requests[-1].full_url)


if __name__ == "__main__":
    unittest.main()
