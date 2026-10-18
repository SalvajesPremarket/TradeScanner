"""Secure local persistence for Schwab's rotating refresh token.

Set TRADESCANNER_TOKEN_STORE_PATH to a file on a durable, private Render disk
(e.g. /var/data/schwab_tokens.json). The token is never logged or returned by
this module.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .schwab_errors import TokenStoreError


def _path() -> Path:
    raw = os.getenv("TRADESCANNER_TOKEN_STORE_PATH", "").strip()
    if not raw:
        raise TokenStoreError(
            "Persistent token storage is not configured; set TRADESCANNER_TOKEN_STORE_PATH on a durable disk."
        )
    return Path(raw)


def load_refresh_token() -> str | None:
    # Until a durable path is configured, allow the current environment token
    # to bootstrap existing deployments. OAuth writes and token rotation still
    # fail closed unless persistent storage is configured.
    if not os.getenv("TRADESCANNER_TOKEN_STORE_PATH", "").strip():
        return None
    path = _path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        raise TokenStoreError("Could not read the persisted Schwab token.") from None
    token = data.get("refresh_token") if isinstance(data, dict) else None
    return token.strip() if isinstance(token, str) and token.strip() else None


def save_refresh_token(token: str) -> None:
    if not isinstance(token, str) or not token.strip():
        raise TokenStoreError("Refusing to persist an empty Schwab refresh token.")
    path = _path()
    parent = path.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".schwab-token-", dir=str(parent))
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({"refresh_token": token.strip()}, handle)
                handle.write("\\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            os.chmod(path, 0o600)
        finally:
            try:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            except OSError:
                pass
    except OSError:
        raise TokenStoreError("Could not securely persist the Schwab refresh token.") from None
