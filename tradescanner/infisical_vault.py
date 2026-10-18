"""Minimal Infisical Universal Auth client for the TradeScanner token vault.

Only secret values are returned to the caller. Error messages deliberately omit
upstream response bodies, headers, and credentials.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

_BASE_URL = "https://app.infisical.com"
_AUTH_URL = f"{_BASE_URL}/api/v1/auth/universal-auth/login"
_SECRET_URL = f"{_BASE_URL}/api/v3/secrets/raw"
_LOCK = threading.Lock()
_ACCESS_TOKEN: str | None = None
_ACCESS_TOKEN_EXPIRES = 0.0
_SECRET_CACHE: dict[str, tuple[str | None, float]] = {}


class InfisicalError(RuntimeError):
    """Safe-to-display error from the Infisical integration."""


def is_configured() -> bool:
    return all(os.getenv(name, "").strip() for name in (
        "INFISICAL_CLIENT_ID", "INFISICAL_CLIENT_SECRET", "INFISICAL_PROJECT_ID"
    ))


def _config() -> tuple[str, str, str, str, str]:
    client_id = os.getenv("INFISICAL_CLIENT_ID", "").strip()
    client_secret = os.getenv("INFISICAL_CLIENT_SECRET", "").strip()
    project_id = os.getenv("INFISICAL_PROJECT_ID", "").strip()
    environment = os.getenv("INFISICAL_ENVIRONMENT", "production").strip()
    secret_path = os.getenv("INFISICAL_SECRET_PATH", "/").strip() or "/"
    if not all((client_id, client_secret, project_id)):
        raise InfisicalError(
            "Infisical is not configured. Set INFISICAL_CLIENT_ID, "
            "INFISICAL_CLIENT_SECRET and INFISICAL_PROJECT_ID."
        )
    if not environment or not secret_path.startswith("/"):
        raise InfisicalError("Infisical environment or secret path is invalid.")
    return client_id, client_secret, project_id, environment, secret_path


def _json_request(request: urllib.request.Request, timeout: int = 12) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise
        raise InfisicalError(f"Infisical API returned HTTP {exc.code}.") from None
    except (urllib.error.URLError, TimeoutError):
        raise InfisicalError("Could not reach Infisical.") from None
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise InfisicalError("Infisical returned an invalid response.") from None
    if not isinstance(payload, dict):
        raise InfisicalError("Infisical returned an unexpected response.")
    return payload


def _access_token() -> str:
    global _ACCESS_TOKEN, _ACCESS_TOKEN_EXPIRES
    now = time.monotonic()
    with _LOCK:
        if _ACCESS_TOKEN and now < _ACCESS_TOKEN_EXPIRES:
            return _ACCESS_TOKEN
        client_id, client_secret, _, _, _ = _config()
        request = urllib.request.Request(
            _AUTH_URL,
            data=json.dumps({"clientId": client_id, "clientSecret": client_secret}).encode(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        payload = _json_request(request)
        data = payload.get("data", payload)
        token = data.get("accessToken") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token:
            raise InfisicalError("Infisical authentication response did not include an access token.")
        try:
            ttl = float(data.get("expiresIn", 600))
        except (TypeError, ValueError):
            ttl = 600.0
        _ACCESS_TOKEN = token
        _ACCESS_TOKEN_EXPIRES = time.monotonic() + max(30.0, min(ttl - 30.0, 600.0))
        return token


def _secret_url(name: str) -> str:
    _, _, project_id, environment, secret_path = _config()
    query = urllib.parse.urlencode({
        "workspaceId": project_id,
        "environment": environment,
        "secretPath": secret_path,
    })
    return f"{_SECRET_URL}/{urllib.parse.quote(name, safe='')}?{query}"


def read_secret(name: str) -> str | None:
    cached = _SECRET_CACHE.get(name)
    if cached and time.monotonic() < cached[1]:
        return cached[0]
    request = urllib.request.Request(
        _secret_url(name),
        headers={"Authorization": f"Bearer {_access_token()}", "Accept": "application/json"},
        method="GET",
    )
    try:
        payload = _json_request(request)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            _SECRET_CACHE[name] = (None, time.monotonic() + 15.0)
            return None
        raise
    data = payload.get("secret", payload.get("data", payload))
    if isinstance(data, dict) and isinstance(data.get("secret"), dict):
        data = data["secret"]
    value = data.get("secretValue") if isinstance(data, dict) else None
    if not isinstance(value, str) or not value:
        _SECRET_CACHE[name] = (None, time.monotonic() + 15.0)
        return None
    _SECRET_CACHE[name] = (value, time.monotonic() + 15.0)
    return value


def write_secret(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise InfisicalError("Refusing to store an empty secret.")
    _, _, project_id, environment, secret_path = _config()
    token = _access_token()
    base = f"{_SECRET_URL}/{urllib.parse.quote(name, safe='')}"
    params = {
        "workspaceId": project_id,
        "environment": environment,
        "secretPath": secret_path,
    }
    body = {
        "workspaceId": project_id,
        "environment": environment,
        "secretPath": secret_path,
        "secretValue": value.strip(),
        "type": "shared",
        "secretComment": "TradeScanner managed Schwab OAuth token",
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    exists = read_secret(name) is not None
    url = f"{base}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers=headers,
        method="PATCH" if exists else "POST",
    )
    _json_request(request)
    _SECRET_CACHE[name] = (value.strip(), time.monotonic() + 15.0)


def _reset_cache_for_tests() -> None:
    global _ACCESS_TOKEN, _ACCESS_TOKEN_EXPIRES
    with _LOCK:
        _ACCESS_TOKEN = None
        _ACCESS_TOKEN_EXPIRES = 0.0
    _SECRET_CACHE.clear()
