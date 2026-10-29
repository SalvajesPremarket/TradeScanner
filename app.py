import asyncio
import hmac
import jwt
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import FileResponse, JSONResponse

from tradescanner.candles import CandleStore
from tradescanner.db import DB_ERRORS, is_postgres
from tradescanner.core import apply_filters, normalize_schwab_quotes
import urllib.error

from tradescanner.float_provider import apply_cached_float, refresh_symbol_float
from tradescanner.infisical_vault import is_configured as infisical_is_configured
from tradescanner.engine import get_engine_status, record_engine_status, run_scan_cycle
from tradescanner.snapshots import SnapshotStore, collect_snapshots
from tradescanner.strategies import (active_strategy, active_strategies, delete_strategy, strategy_matches, list_strategies, save_strategy, set_strategy_enabled)
from tradescanner.universe import UniverseUnavailableError, load_symbol_universe
from tradescanner.schwab_stream import start_schwab_stream, stop_schwab_stream
from tradescanner.oauth import router as oauth_router
from tradescanner.schwab import (
    SchwabAPIError,
    SchwabConfigurationError,
    get_quotes,
    get_chart_history,
)

ROOT = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


SCAN_LEASE_KEY = "tradescanner_automatic_scan_worker_v1"
SCAN_LEASE_TTL_SECONDS = 180.0


async def _automatic_scan_loop(stop: asyncio.Event) -> None:
    enabled = os.getenv("TRADESCANNER_AUTO_SCAN_ENABLED", "false").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        logger.warning("Automatic scan loop disabled: TRADESCANNER_AUTO_SCAN_ENABLED is false")
        return
    try:
        interval = max(10, int(os.getenv("TRADESCANNER_SCAN_INTERVAL_SECONDS", "30")))
    except ValueError:
        interval = 30
    worker_id = uuid.uuid4().hex
    logger.warning("Automatic scan loop coroutine entered; interval_seconds=%s", interval)

    while not stop.is_set():
        started = time.monotonic()
        lease_store = None
        lease_acquired = False
        try:
            lease_store = SnapshotStore()
            lease_acquired = await asyncio.to_thread(
                lease_store.acquire_lease, SCAN_LEASE_KEY, worker_id, SCAN_LEASE_TTL_SECONDS
            )
            if not lease_acquired:
                logger.info("Automatic scan skipped; another instance owns the scanner lease")
            else:
                result = await asyncio.to_thread(run_scan_cycle)
                logger.info(
                "Automatic scan cycle finished; status=%s cycles_completed=%s snapshots_saved=%s duration_seconds=%.3f",
                result.get("status", "unknown"),
                result.get("cycles_completed", "unknown"),
                result.get("snapshots_saved", 0),
                time.monotonic() - started,
                )
        except Exception as exc:
            # Store and log only the exception class; never leak provider responses or secrets.
            logger.error("Automatic scan cycle failed; error_type=%s", type(exc).__name__)
            try:
                record_engine_status(SnapshotStore(), status="error", last_error=type(exc).__name__)
            except (OSError, RuntimeError) + DB_ERRORS:
                pass
        finally:
            if lease_acquired and lease_store is not None:
                try:
                    await asyncio.to_thread(lease_store.release_lease, SCAN_LEASE_KEY, worker_id)
                except Exception as lease_error:
                    logger.warning("Automatic scan lease release failed; error_type=%s", type(lease_error).__name__)
        # Treat the configured interval as start-to-start cadence, not an
        # additional sleep after a slow scan. Otherwise history calls add the
        # full interval on top of every cycle and needlessly slow universe coverage.
        remaining = max(0.0, interval - (time.monotonic() - started))
        if remaining > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                pass


@asynccontextmanager
async def lifespan(application: FastAPI):
    stop = asyncio.Event()
    task = asyncio.create_task(_automatic_scan_loop(stop))
    application.state.scan_task = task
    start_schwab_stream()
    await asyncio.sleep(0)  # Give the scan coroutine a scheduling turn before startup completes.
    logger.warning(
        "TradeScanner lifespan started; automatic_scan_enabled=%s",
        os.getenv("TRADESCANNER_AUTO_SCAN_ENABLED", "false").strip().lower()
        in {"1", "true", "yes", "on"},
    )
    try:
        yield
    finally:
        stop.set()
        stop_schwab_stream()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


app = FastAPI(title="TradeScanner", version="0.3.1", lifespan=lifespan)
app.include_router(oauth_router)

_DEVICE_SESSION_KEY = "tradescanner_single_active_device_v1"
_DEVICE_COOKIE = "tradescanner_device_id"

@app.middleware("http")
async def enforce_single_active_device(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api/") or path in {"/api/device-session/claim", "/api/device-session/release"}:
        return await call_next(request)
    try:
        active = SnapshotStore().get_state(_DEVICE_SESSION_KEY, None)
    except (OSError, RuntimeError, ValueError) + DB_ERRORS:
        return JSONResponse(status_code=503, content={"detail": "No se pudo verificar el dispositivo activo."})
    device_id = request.cookies.get(_DEVICE_COOKIE, "")
    if not isinstance(active, dict) or not active.get("device_id") or not device_id or not hmac.compare_digest(str(active["device_id"]), device_id):
        return JSONResponse(status_code=423, content={"detail": "TradeScanner fue activado en otro dispositivo. Inicia sesión aquí para activar este dispositivo."})
    return await call_next(request)


def _scanner_indicators(candles: list[dict]) -> dict[str, float | None]:
    """Return EMA20 and MACD line from stored chronological candle closes."""
    closes: list[float] = []
    for candle in candles:
        try:
            value = float(candle.get("close"))
            if value > 0 and value == value and value != float("inf"):
                closes.append(value)
        except (TypeError, ValueError):
            continue
    if len(closes) < 26:
        return {"ema20": None, "macd": None}
    def ema_series(values: list[float], period: int) -> list[float]:
        alpha = 2.0 / (period + 1.0)
        result = [values[0]]
        for value in values[1:]:
            result.append(alpha * value + (1.0 - alpha) * result[-1])
        return result
    ema20_series = ema_series(closes, 20)
    ema12 = ema_series(closes, 12)
    ema26 = ema_series(closes, 26)
    return {
        "ema20": round(ema20_series[-1], 4),
        "macd": round(ema12[-1] - ema26[-1], 6),
    }



@app.get("/", include_in_schema=False)
def index():
    return FileResponse(ROOT / "index.html")


@app.get("/health")
def health():
    return {"status": "ok", "service": "TradeScanner"}


@app.get("/api/status")
def status():
    configured = all(os.getenv(name, "").strip() for name in (
        "SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET"
    )) and (
        bool(os.getenv("SCHWAB_REFRESH_TOKEN", "").strip()) or infisical_is_configured()
    )
    engine = get_engine_status()
    last_quote_at = engine.get("last_quote_received_at")
    connected = False
    quote_age_seconds = None
    if isinstance(last_quote_at, str):
        try:
            parsed_quote_at = datetime.fromisoformat(last_quote_at)
            if parsed_quote_at.tzinfo is None:
                parsed_quote_at = parsed_quote_at.replace(tzinfo=timezone.utc)
            quote_age_seconds = (datetime.now(timezone.utc) - parsed_quote_at.astimezone(timezone.utc)).total_seconds()
            connected = -30 <= quote_age_seconds <= 300
        except (TypeError, ValueError, OverflowError):
            quote_age_seconds = None
            connected = False
    try:
        latest_rows = SnapshotStore().latest(limit=1000)
        results_published = sum(1 for row in latest_rows if row.get("quote_is_fresh") is True)
    except (OSError, RuntimeError, ValueError) + DB_ERRORS:
        results_published = 0
    return {
        "service": "TradeScanner",
        "provider": "Schwab",
        "provider_configured": configured,
        "infisical_configured": infisical_is_configured(),
        "float_provider_configured": bool(os.getenv("MASSIVE_API_KEY", "").strip()),
        "market_data_connected": connected,
        "quote_age_seconds": round(quote_age_seconds, 1) if quote_age_seconds is not None else None,
        "last_quote_received_at": last_quote_at if isinstance(last_quote_at, str) else None,
        "last_error_type": engine.get("last_error") if isinstance(engine.get("last_error"), str) else None,
        "cycles_completed": engine.get("cycles_completed"),
        "received_count": engine.get("received_count"),
        "results_published": results_published,
        "engine_status": engine.get("status", "starting"),
        "message": (
            "Schwab quotes were received within the last five minutes."
            if connected
            else "No recent successful Schwab quote batch has been verified."
            if configured
            else "Schwab credentials are not configured."
        ),
    }


@app.post("/api/diagnostics/float/fmp")
@app.post("/api/diagnostics/float/fmp/resolve-legacy-cooldown")
@app.post("/api/diagnostics/float/fmp/refresh-cache")
def disabled_fmp_float_routes(request: Request):
    """Legacy FMP endpoints are intentionally disabled; no outbound FMP calls are made."""
    _authorize_strategy_access(request)
    raise HTTPException(status_code=410, detail="FMP está deshabilitado en TradeScanner. FLOAT utiliza Massive y su caché local.")


@app.post("/api/diagnostics/float/massive")
def test_massive_float(request: Request, symbol: str = Query("AAPL", min_length=1, max_length=12)):
    """Make one authenticated, bounded FLOAT request to Massive for verification."""
    _authorize_strategy_access(request)
    normalized = symbol.strip().upper()
    if not normalized.replace(".", "").replace("-", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker symbol.")
    result = refresh_symbol_float(SnapshotStore(), normalized, force=True)
    if result.get("status") == "not_configured":
        raise HTTPException(status_code=503, detail="MASSIVE_API_KEY is not configured in the service environment.")
    if result.get("status") == "unavailable":
        raise HTTPException(status_code=502, detail={
            "provider": "Massive", "status": result.get("status"),
            "error": result.get("error", "provider_error"),
        })
    return {
        "provider": "Massive",
        "quote_provider": "Schwab",
        "status": result.get("status"),
        "symbol": result.get("symbol"),
        "float_shares": result.get("float_shares"),
        "effective_date": result.get("effective_date"),
        "rows_saved": result.get("rows_saved", 0),
        "note": "No FMP fallback is used; this test requests FLOAT only.",
    }


@app.get("/api/engine/status")
def engine_status():
    state = get_engine_status()
    enabled = os.getenv("TRADESCANNER_AUTO_SCAN_ENABLED", "false").strip().lower() in {
        "1", "true", "yes", "on"
    }
    try:
        interval = max(10, int(os.getenv("TRADESCANNER_SCAN_INTERVAL_SECONDS", "30")))
    except ValueError:
        interval = 30
    task = getattr(app.state, "scan_task", None)
    task_done = task.done() if task is not None else None
    return {
        "automatic_scan_enabled": enabled,
        "scan_loop_task_created": task is not None,
        "scan_loop_task_done": task_done,
        "scan_loop_task_running": task is not None and not task_done,
        "cycle_interval_seconds": interval,
        "batch_size": 100,
        "provider": "Schwab",
        "float_provider": "Massive FLOAT + local cache" if os.getenv("MASSIVE_API_KEY", "").strip() else "Massive FLOAT not configured; local cache only",
        "state": state or {"status": "starting"},
    }


@app.get("/api/universe/status")
def universe_status():
    try:
        universe = load_symbol_universe()
        return {
            "source": universe["source"],
            "generated_at": universe["generated_at"],
            "universe_count": universe["count"],
            "sample_symbols": universe["symbols"][:20],
            "includes_etfs": True,
            "includes_test_issues": False,
            "note": "Current listed symbols from Nasdaq Trader; unsupported ticker formats are skipped.",
        }
    except UniverseUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None


@app.get("/api/snapshots/status")
def snapshots_status():
    try:
        store = SnapshotStore()
        return {
            "storage": "postgresql" if is_postgres(store.path) else "sqlite",
            "storage_path_configured": bool(os.getenv("DATABASE_URL", "").strip() or os.getenv("TRADESCANNER_DB_PATH", "").strip()),
            "durability_status": "managed_postgresql" if is_postgres(store.path) else "not_verified",
            "durability_note": (
                "PostgreSQL is managed by the configured provider; verify the DATABASE_URL and "
                "provider retention/backup settings. SQLite needs a durable mounted volume."
            ),
            **store.stats(),
        }
    except (OSError, RuntimeError) + DB_ERRORS:
        raise HTTPException(status_code=503, detail="Snapshot storage is unavailable.") from None


@app.get("/api/snapshots/latest")
def snapshots_latest(limit: int = Query(10000, ge=1, le=10000)):
    try:
        store = SnapshotStore()
        rows = store.latest(limit)
        candle_store = CandleStore(store.path)
        for row in rows:
            # RVOL is only needed for fresh quotes shown in the live scanner.
            # Avoid thousands of expensive candle-history reads for stale snapshots.
            if row.get("quote_is_fresh") is True:
                metrics = candle_store.rvol(row["symbol"])
                row["rvol"] = metrics["rvol"]
                row["rvol_sessions_used"] = metrics["sessions_used"]
                row["rvol_as_of"] = metrics.get("as_of")
            else:
                row["rvol"] = None
                row["rvol_sessions_used"] = 0
                row["rvol_as_of"] = None
            try:
                indicator_candles = candle_store.latest(row["symbol"], limit=100)
                indicators = _scanner_indicators(indicator_candles)
                row["ema20"] = indicators["ema20"]
                row["macd"] = indicators["macd"]
            except (OSError, RuntimeError, ValueError) + DB_ERRORS:
                row["ema20"] = None
                row["macd"] = None
            row.setdefault("float_shares", None)
            row.setdefault("float_as_of", None)
            row.setdefault("float_source", None)
        # Fill missing FLOAT from the existing local cache only; no provider calls.
        apply_cached_float(rows, store)
        # The main radar must show available snapshots independently of strategy matches.
        # Strategy evaluation remains available through the dedicated strategy endpoints.
        selected_strategy = active_strategy()
        return {
            "results": rows,
            "limit": limit,
            "active_strategy": (
                {"id": selected_strategy["id"], "name": selected_strategy["name"]}
                if selected_strategy else None
            ),
        }
    except (OSError, RuntimeError, ValueError) + DB_ERRORS:
        raise HTTPException(status_code=503, detail="Snapshot storage is unavailable.") from None


def _authorize_strategy_access(request: Request) -> dict:
    """Require a valid Neon Auth JWT for strategy editor operations."""
    jwks_url = os.getenv("NEON_AUTH_JWKS_URL", "").strip()
    issuer = os.getenv("NEON_AUTH_ISSUER", "").strip()
    if not jwks_url or not issuer:
        raise HTTPException(status_code=503, detail="Autenticación Neon no está configurada en el servidor.")

    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=401, detail="Inicia sesión con tu cuenta Neon para acceder al editor.")

    try:
        signing_key = jwt.PyJWKClient(jwks_url, cache_jwk_set=True).get_signing_key_from_jwt(token.strip()).key
    except jwt.PyJWKClientError:
        logger.exception("No se pudo consultar la clave pública de Neon Auth.")
        raise HTTPException(status_code=503, detail="No se pudo verificar Neon Auth temporalmente. Inténtalo de nuevo.") from None

    try:
        # Neon Auth issues tokens whose audience may be deployment-specific. Verify
        # the signature against Neon JWKS, expiration, subject, and issuer below; do not
        # assume a hard-coded audience that rejects valid Neon sessions.
        # Verify signature with Neon JWKS, expiration, subject, and issuer.
        # Neon deployment audience is not stable across this setup, so do not hard-code it.
        claims = jwt.decode(
            token.strip(),
            signing_key,
            algorithms=["EdDSA", "ES256", "RS256"],
            options={"verify_aud": False, "require": ["exp", "sub", "iss"]},
        )
        token_issuer = claims.get("iss")
        if not isinstance(token_issuer, str) or token_issuer.rstrip("/") != issuer.rstrip("/"):
            raise jwt.InvalidIssuerError("Issuer does not match NEON_AUTH_ISSUER")
        if not isinstance(claims.get("sub"), str) or not claims["sub"].strip():
            raise jwt.InvalidTokenError("Missing subject")
    except jwt.InvalidTokenError as exc:
        logger.warning("Neon Auth JWT rejected: %s", type(exc).__name__)
        raise HTTPException(status_code=401, detail="La sesión Neon no es válida o ha expirado. Inicia sesión de nuevo.") from None
    return claims


@app.post("/api/device-session/claim")
def claim_device_session(request: Request, payload: dict = Body(...)):
    claims = _authorize_strategy_access(request)
    device_id = payload.get("device_id")
    if not isinstance(device_id, str) or len(device_id) < 16 or len(device_id) > 128 or not all(ch.isalnum() or ch in "-_" for ch in device_id):
        raise HTTPException(status_code=400, detail="Identificador de dispositivo inválido.")
    store = SnapshotStore()
    store.set_state(_DEVICE_SESSION_KEY, {
        "device_id": device_id,
        "user_id": claims["sub"],
        "claimed_at": datetime.now(timezone.utc).isoformat(),
    })
    response = JSONResponse({"active": True, "message": "TradeScanner está activo en este dispositivo."})
    response.set_cookie(
        key=_DEVICE_COOKIE, value=device_id, httponly=True, secure=request.url.scheme == "https",
        samesite="lax", max_age=60 * 60 * 24 * 60, path="/",
    )
    return response


@app.post("/api/device-session/release")
def release_device_session(request: Request):
    device_id = request.cookies.get(_DEVICE_COOKIE, "")
    store = SnapshotStore()
    active = store.get_state(_DEVICE_SESSION_KEY, None)
    if isinstance(active, dict) and device_id and hmac.compare_digest(str(active.get("device_id", "")), device_id):
        store.set_state(_DEVICE_SESSION_KEY, {})
    response = JSONResponse({"released": True})
    response.delete_cookie(key=_DEVICE_COOKIE, path="/", httponly=True, secure=request.url.scheme == "https", samesite="lax")
    return response


@app.get("/api/strategies")
def strategies_list(request: Request):
    _authorize_strategy_access(request)
    """List saved strategies without exposing any execution environment."""
    return {"strategies": list_strategies()}


@app.post("/api/strategies")
def strategies_save(request: Request, payload: dict = Body(...)):
    _authorize_strategy_access(request)
    try:
        strategy = save_strategy(
            name=payload.get("name", ""),
            language=payload.get("language", ""),
            source=payload.get("source", ""),
            strategy_id=payload.get("id"),
        )
        return {"strategy": strategy}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@app.post("/api/strategies/{strategy_id}/activate")
def strategies_activate(strategy_id: str, request: Request, payload: dict = Body(...)):
    _authorize_strategy_access(request)
    enabled = payload.get("enabled")
    if not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="enabled debe ser true o false.")
    try:
        return {"strategy": set_strategy_enabled(strategy_id, enabled)}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@app.delete("/api/strategies/{strategy_id}")
def strategies_delete(strategy_id: str, request: Request):
    _authorize_strategy_access(request)
    try:
        delete_strategy(strategy_id)
        return {"deleted": True, "id": strategy_id}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc).strip("'")) from None


@app.get("/api/strategies/active-matches")
def strategies_active_matches(request: Request):
    _authorize_strategy_access(request)
    strategies = active_strategies()
    if not strategies:
        return {"strategy": None, "strategies": [], "symbols": [], "momentum": {"status": "inactive", "evaluations": [], "matches": []}}
    store = SnapshotStore()
    states = []
    all_symbols = []
    momentum_state = {"status": "inactive", "evaluations": [], "matches": []}
    for strategy in strategies:
        state = store.get_state(
            "strategy_scan_status:" + str(strategy.get("id")),
            {"status": "waiting_for_cycle", "evaluations": [], "matches": []},
        )
        states.append({
            "id": strategy.get("id"),
            "name": strategy.get("name"),
            "language": strategy.get("language"),
            "scan": state,
        })
        all_symbols.extend(state.get("matches", []))
        if "momentum" in str(strategy.get("name", "")).casefold() and momentum_state.get("status") == "inactive":
            momentum_state = state
    primary = strategies[0]
    return {
        "strategy": {"id": primary.get("id"), "name": primary.get("name")},
        "strategies": states,
        "symbols": list(dict.fromkeys(all_symbols)),
        "momentum": momentum_state,
    }

@app.get("/api/candles/{symbol}")
def candles_status(
    symbol: str,
    limit: int = Query(500, ge=1, le=5000),
    timeframe: str | None = Query(None, pattern="^(1m|5m|15m|1h|day|week)$"),
):
    normalized = symbol.strip().upper()
    if not normalized or len(normalized) > 15:
        raise HTTPException(status_code=400, detail="Invalid ticker symbol.")
    try:
        store = SnapshotStore()
        candle_store = CandleStore(store.path)
        if timeframe is None:
            return {
                "symbol": normalized,
                "provider": "Schwab",
                "timeframe_label": "Velas guardadas",
                "source": "stored",
                "candles": candle_store.latest(normalized, limit),
                "rvol": candle_store.rvol(normalized),
            }
        from tradescanner.candles import normalize_price_history
        payload = get_chart_history(normalized, timeframe)
        candles = normalize_price_history(normalized, payload)
        if timeframe == "1h":
            hourly = {}
            for candle in candles:
                bucket = (candle["candle_time_ms"] // 3_600_000) * 3_600_000
                current = hourly.get(bucket)
                if current is None:
                    hourly[bucket] = {**candle, "candle_time_ms": bucket}
                else:
                    current["high"] = max(current["high"], candle["high"])
                    current["low"] = min(current["low"], candle["low"])
                    current["close"] = candle["close"]
                    current["volume"] += candle["volume"]
            candles = list(hourly.values())
        candles = candles[-limit:]
        labels = {"1m":"1 minuto","5m":"5 minutos","15m":"15 minutos","1h":"1 hora","day":"Diaria","week":"Semanal"}
        return {
            "symbol": normalized,
            "provider": "Schwab",
            "timeframe": timeframe,
            "timeframe_label": labels[timeframe],
            "source": "schwab_live_history",
            "candles": candles,
            "rvol": candle_store.rvol(normalized),
        }
    except (SchwabAPIError, SchwabConfigurationError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    except (OSError, RuntimeError, ValueError) + DB_ERRORS:
        raise HTTPException(status_code=503, detail="Candle storage or price history is unavailable.") from None


@app.post("/api/scan")
def scan(symbols: str = Query(..., description="Comma-separated ticker symbols; up to 100 per provider request")):
    requested = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]
    if not requested:
        raise HTTPException(status_code=400, detail="Provide at least one ticker symbol.")
    unique_count = len(set(symbol.upper() for symbol in requested))
    if unique_count > 100:
        raise HTTPException(status_code=400, detail="A manual scan is limited to 100 unique symbols per request.")

    started = time.monotonic()
    try:
        result = collect_snapshots(requested, SnapshotStore())
    except SchwabConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except SchwabAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from None
    except (OSError, RuntimeError) + DB_ERRORS:
        raise HTTPException(status_code=503, detail="Snapshot storage is unavailable.") from None

    return {
        "status": "completed",
        "provider": "Schwab",
        **result,
        "duration_seconds": round(time.monotonic() - started, 3),
        "note": "Only quotes returned by Schwab were saved; missing values were not synthesized.",
    }


@app.get("/api/quotes")
def quotes(
    symbols: str = Query(..., description="Comma-separated ticker symbols"),
    min_price: float | None = Query(None, ge=0),
    max_price: float | None = Query(None, ge=0),
    min_gap_pct: float | None = None,
    max_gap_pct: float | None = None,
    min_rvol: float | None = Query(None, ge=0),
    min_volume: int | None = Query(None, ge=0),
):
    requested = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]
    try:
        raw = get_quotes(requested)
        rows = normalize_schwab_quotes(raw)
        filtered = apply_filters(
            rows,
            min_price=min_price,
            max_price=max_price,
            min_gap_pct=min_gap_pct,
            max_gap_pct=max_gap_pct,
            min_rvol=min_rvol,
            min_volume=min_volume,
        )
        return {
            "provider": "Schwab",
            "requested_count": len(set(symbol.upper() for symbol in requested)),
            "received_count": len(rows),
            "result_count": len(filtered),
            "filters": {
                "min_price": min_price,
                "max_price": max_price,
                "min_gap_pct": min_gap_pct,
                "max_gap_pct": max_gap_pct,
                "min_rvol": min_rvol,
                "min_volume": min_volume,
            },
            "results": filtered,
        }
    except SchwabConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except SchwabAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from None