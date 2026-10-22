"""Schwab OAuth callback that persists refresh tokens to Infisical."""
from __future__ import annotations

import base64
import json
import os
import secrets
import urllib.error
import urllib.parse
import urllib.request

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from .infisical_vault import InfisicalError, is_configured as infisical_is_configured
from .infisical_vault import write_secret

router = APIRouter()
TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
AUTHORIZE_URL = "https://api.schwabapi.com/v1/oauth/authorize"


def _redirect_uri() -> str:
    value = os.getenv("SCHWAB_REDIRECT_URI", "").strip()
    if not value.startswith("https://") or not value.endswith("/oauth/callback"):
        raise HTTPException(
            status_code=503,
            detail="Set SCHWAB_REDIRECT_URI to the HTTPS Render URL ending in /oauth/callback.",
        )
    return value


def _private_html(page: str) -> HTMLResponse:
    return HTMLResponse(
        page,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/oauth/start", include_in_schema=False)
def oauth_start():
    client_id = os.getenv("SCHWAB_CLIENT_ID", "").strip()
    if not client_id or not os.getenv("SCHWAB_CLIENT_SECRET", "").strip():
        raise HTTPException(status_code=503, detail="Schwab client credentials are not configured.")
    if not infisical_is_configured():
        raise HTTPException(status_code=503, detail="Secure token storage is not configured.")
    redirect_uri = _redirect_uri()
    state = secrets.token_urlsafe(32)
    params = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
    })
    response = RedirectResponse(f"{AUTHORIZE_URL}?{params}", status_code=302)
    response.set_cookie(
        "tradescanner_oauth_state", state, max_age=600, httponly=True,
        secure=True, samesite="lax", path="/oauth",
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


@router.get("/oauth/callback", include_in_schema=False, response_class=HTMLResponse)
def oauth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
):
    expected_state = request.cookies.get("tradescanner_oauth_state")
    if error:
        raise HTTPException(status_code=400, detail="Schwab authorization was denied or cancelled.")
    if not code or not state or not expected_state or not secrets.compare_digest(state, expected_state):
        raise HTTPException(status_code=400, detail="OAuth state validation failed. Start again at /oauth/start.")
    if not infisical_is_configured():
        raise HTTPException(status_code=503, detail="Secure token storage is not configured.")

    client_id = os.getenv("SCHWAB_CLIENT_ID", "").strip()
    client_secret = os.getenv("SCHWAB_CLIENT_SECRET", "").strip()
    redirect_uri = _redirect_uri()
    if not client_id or not client_secret:
        raise HTTPException(status_code=503, detail="Schwab client credentials are not configured.")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }).encode()
    token_request = urllib.request.Request(
        TOKEN_URL,
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(token_request, timeout=20) as upstream:
            payload = json.loads(upstream.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Schwab token exchange failed (HTTP {exc.code}).") from None
    except (urllib.error.URLError, TimeoutError):
        raise HTTPException(status_code=502, detail="Could not reach Schwab token service.") from None
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(status_code=502, detail="Schwab returned an invalid token response.") from None

    refresh_token = payload.get("refresh_token") if isinstance(payload, dict) else None
    if not isinstance(refresh_token, str) or not refresh_token:
        raise HTTPException(status_code=502, detail="Schwab response did not include a refresh token.")

    try:
        write_secret("SCHWAB_REFRESH_TOKEN", refresh_token)
    except InfisicalError:
        raise HTTPException(
            status_code=503,
            detail="Schwab authorized the app, but Infisical could not securely store the token. Restart OAuth after fixing vault access.",
        ) from None

    response = _private_html("""<!doctype html><html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1"><title>TradeScanner OAuth</title>
    <meta http-equiv="refresh" content="3;url=https://tradescanner-new-core.onrender.com/"></head>
    <body style="font-family:system-ui;max-width:760px;margin:40px auto;padding:0 18px">
    <h2>Schwab conectado correctamente</h2>
    <p>TradeScanner guardó la autorización en el almacén seguro. No necesitas copiar ni pegar tokens.</p>
    <p>Los tokens de acceso se renovarán automáticamente y los cambios de refresh token se guardarán en el almacén.</p>
    <p>Regresando a TradeScanner en unos segundos…</p>
    <p><a href="https://tradescanner-new-core.onrender.com/">Volver a TradeScanner ahora</a></p>
    <script>window.setTimeout(function(){window.location.replace("https://tradescanner-new-core.onrender.com/");},1500);</script>
    </body></html>""")
    response.delete_cookie("tradescanner_oauth_state", path="/oauth")
    return response
