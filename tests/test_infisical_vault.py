import os
import unittest
import urllib.parse
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

    def test_configuration_requires_machine_identity_project_and_environment(self):
        with patch.dict(os.environ, {"INFISICAL_CLIENT_SECRET": ""}):
            self.assertFalse(vault.is_configured())
        with patch.dict(os.environ, {"INFISICAL_ENVIRONMENT": ""}):
            self.assertFalse(vault.is_configured())

    def test_universal_auth_uses_form_encoded_credentials(self):
        captured = []

        def fake_request(request, timeout=12):
            captured.append(request)
            return {"accessToken": "test-access-token", "expiresIn": 7200}

        with patch.object(vault, "_json_request", side_effect=fake_request):
            self.assertEqual(vault._access_token(), "test-access-token")

        request = captured[0]
        self.assertEqual(request.full_url, vault._AUTH_URL)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.get_header("Content-type"), "application/x-www-form-urlencoded")
        form = urllib.parse.parse_qs(request.data.decode())
        self.assertEqual(form["clientId"], ["client-id"])
        self.assertEqual(form["clientSecret"], ["client-secret"])

    def test_read_secret_returns_value_without_logging_it(self):
        with patch.object(vault, "_access_token", return_value="test-access"), patch.object(
            vault, "_json_request", return_value={"secret": {"secretValue": "private-value"}}
        ):
            self.assertEqual(vault.read_secret("SCHWAB_REFRESH_TOKEN"), "private-value")
        # v4 single-secret endpoint uses projectId, environment and secretPath.
        # Ensure the project ID is not sent under the obsolete workspaceId key.
        request = vault.urllib.request.Request(
            vault._secret_url("SCHWAB_REFRESH_TOKEN"),
            headers={"Authorization": "Bearer test-access"},
            method="GET",
        )
        self.assertTrue(request.full_url.startswith("https://app.infisical.com/api/v4/secrets/"))
        query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
        self.assertEqual(query["projectId"], ["project-id"])
        self.assertEqual(query["environment"], ["prod"])
        self.assertEqual(query["secretPath"], ["/"])
        self.assertNotIn("workspaceId", query)

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
        self.assertTrue(requests[-1].full_url.startswith("https://app.infisical.com/api/v4/secrets/"))
        self.assertNotIn("workspaceId", urllib.parse.parse_qs(urllib.parse.urlparse(requests[-1].full_url).query))
        self.assertEqual(requests[-1].get_header("Content-type"), "application/json")
        payload = __import__("json").loads(requests[-1].data.decode())
        self.assertEqual(payload["projectId"], "project-id")
        self.assertEqual(payload["environment"], "prod")
        self.assertEqual(payload["secretPath"], "/")
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
