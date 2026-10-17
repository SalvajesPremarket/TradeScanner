"""Minimal server-side Schwab API adapter for the new TradeScanner core."""
from __future__ import annotations

import base64
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
QUOTES_URL = "https://api.schwabapi.com/marketdata/v1/quotes"
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")

_TOKEN_LOCK = threading.Lock()
_CACHED_TOKEN: str | None = None
_CACHED_TOKEN_EXPIRES_AT = 0.0
_CACHED_CREDENTIALS: tuple[str, str, str] | None = None


class SchwabConfigurationError(RuntimeError):
    pass


class SchwabAPIError(RuntimeError):
    pass


def _credentials() -> tuple[str, str, str]:
    client_id = os.getenv("SCHWAB_CLIENT_ID", "").strip()
    client_secret = os.getenv("SCHWAB_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("SCHWAB_REFRESH_TOKEN", "").strip()
    if not all((client_id, client_secret, refresh_token)):
        raise SchwabConfigurationError(
            "Configure SCHWAB_CLIENT_ID, SCHWAB_CLIENT_SECRET and SCHWAB_REFRESH_TOKEN."
        )
    return client_id, client_secret, refresh_token


def _request_json(request: urllib.request.Request, timeout: int = 15) -> Any:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        # Do not surface request headers or credentials in errors.
        # Do not expose upstream response bodies to API clients.
        raise SchwabAPIError(f"Schwab API returned HTTP {exc.code}.") from None
    except (urllib.error.URLError, TimeoutError) as exc:
        raise SchwabAPIError(f"Could not reach Schwab API: {type(exc).__name__}") from None
    except json.JSONDecodeError:
        raise SchwabAPIError("Schwab API returned an invalid JSON response.") from None


def _access_token() -> str:
    global _CACHED_TOKEN, _CACHED_TOKEN_EXPIRES_AT, _CACHED_CREDENTIALS
    credentials = _credentials()
    now = time.monotonic()
    with _TOKEN_LOCK:
        if (
            _CACHED_TOKEN
            and _CACHED_CREDENTIALS == credentials
            and now < _CACHED_TOKEN_EXPIRES_AT
        ):
            return _CACHED_TOKEN

        client_id, client_secret, refresh_token = credentials
        basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }).encode()
        request = urllib.request.Request(
            TOKEN_URL,
            data=body,
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )
        payload = _request_json(request)
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not token:
            raise SchwabAPIError("Schwab token response did not contain an access token.")
        try:
            lifetime = max(60.0, float(payload.get("expires_in", 1800)))
        except (TypeError, ValueError):
            lifetime = 1800.0
        _CACHED_TOKEN = str(token)
        _CACHED_CREDENTIALS = credentials
        # Refresh one minute before provider expiry to avoid edge-of-expiry calls.
        _CACHED_TOKEN_EXPIRES_AT = time.monotonic() + max(1.0, lifetime - 60.0)
        return _CACHED_TOKEN


def _clear_token_cache() -> None:
    """Clear in-memory token state; intended for deterministic tests."""
    global _CACHED_TOKEN, _CACHED_TOKEN_EXPIRES_AT, _CACHED_CREDENTIALS
    with _TOKEN_LOCK:
        _CACHED_TOKEN = None
        _CACHED_TOKEN_EXPIRES_AT = 0.0
        _CACHED_CREDENTIALS = None

def get_quotes(symbols: list[str]) -> dict[str, Any]:
    """Fetch real quotes for a bounded list of validated symbols."""
    normalized = list(dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip()))
    if not normalized:
        raise ValueError("Provide at least one ticker symbol.")
    if len(normalized) > 100:
        raise ValueError("A single request is limited to 100 ticker symbols.")
    invalid = [symbol for symbol in normalized if not _SYMBOL_RE.fullmatch(symbol)]
    if invalid:
        raise ValueError(f"Invalid ticker symbol: {invalid[0]}")

    query = urllib.parse.urlencode({"symbols": ",".join(normalized), "fields": "quote,reference"})
    request = urllib.request.Request(
        f"{QUOTES_URL}?{query}",
        headers={
            "Authorization": f"Bearer {_access_token()}",
            "Accept": "application/json",
        },
        method="GET",
    )
    payload = _request_json(request)
    if not isinstance(payload, dict):
        raise SchwabAPIError("Schwab quotes response was not an object.")
    # Return provider data unchanged; do not synthesize missing quotes.
    return payload
