"""One-time Schwab OAuth helper for securely obtaining the initial refresh token.

The refresh token is returned directly in the validated OAuth callback response.
It is never logged or stored in process memory between HTTP requests.
"""
from __future__ import annotations

import base64
import html
import json
import os
import secrets
import urllib.error
import urllib.parse
import urllib.request

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

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
        # Never pass Schwab's response body or request headers to the client/log.
        raise HTTPException(status_code=502, detail=f"Schwab token exchange failed (HTTP {exc.code}).") from None
    except (urllib.error.URLError, TimeoutError):
        raise HTTPException(status_code=502, detail="Could not reach Schwab token service.") from None
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(status_code=502, detail="Schwab returned an invalid token response.") from None

    refresh_token = payload.get("refresh_token") if isinstance(payload, dict) else None
    if not isinstance(refresh_token, str) or not refresh_token:
        raise HTTPException(status_code=502, detail="Schwab response did not include a refresh token.")

    # Return the value in this response rather than redirecting to an in-memory
    # handoff. This avoids prefetch consumption and loss across restarts/instances.
    safe_token = html.escape(refresh_token)
    page = f"""<!doctype html><html><head><meta charset="utf-8"><meta name="referrer" content="no-referrer">
    <meta name="viewport" content="width=device-width,initial-scale=1"><title>TradeScanner OAuth</title></head>
    <body style="font-family:system-ui;max-width:760px;margin:40px auto;padding:0 18px">
    <h2>Autorización Schwab completada</h2>
    <p>Copia el valor que aparece abajo y guárdalo en Render como <b>SCHWAB_REFRESH_TOKEN</b>.
    Este valor se muestra en esta respuesta privada. No lo compartas ni lo envíes por chat.</p>
    <textarea readonly rows="5" style="width:100%;box-sizing:border-box">{safe_token}</textarea>
    <p>Después de guardarlo en Render, elimina esta pestaña. Mantén el escáner automático desactivado hasta verificar la conexión.</p>
    </body></html>"""
    response = _private_html(page)
    response.delete_cookie("tradescanner_oauth_state", path="/oauth")
    return response
