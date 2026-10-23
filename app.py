import asyncio
import hmac
import jwt
import logging
import os
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import FileResponse

from tradescanner.candles import CandleStore
from tradescanner.db import DB_ERRORS, is_postgres
from tradescanner.core import apply_filters, normalize_schwab_quotes
import urllib.error

from tradescanner.float_provider import apply_cached_float, _fetch_page, _fetch_symbol_float
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
)

ROOT = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def _automatic_scan_loop(stop: asyncio.Event) -> None:
    enabled = os.getenv("TRADESCANNER_AUTO_SCAN_ENABLED", "false").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        logger.warning("Automatic scan loop disabled: TRADESCANNER_AUTO_SCAN_ENABLED is false")
        return
    try:
        interval = max(10, int(os.getenv("TRADESCANNER_SCAN_INTERVAL_SECONDS", "30")))
    except ValueError:
        interval = 30
    logger.warning("Automatic scan loop coroutine entered; interval_seconds=%s", interval)

    while not stop.is_set():
        started = time.monotonic()
        try:
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
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
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
        "float_provider_configured": bool(os.getenv("FMP_API_KEY", "").strip()),
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
def diagnose_fmp_float(request: Request):
    """One-shot, read-only check of FMP bulk and per-symbol FLOAT endpoints."""
    _authorize_strategy_access(request)
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return {
            "provider": "FMP",
            "status": "not_configured",
            "key_configured": False,
            "provider_http_status": None,
            "usable_records": 0,
            "cache_modified": False,
        }

    result = {
        "provider": "FMP",
        "key_configured": True,
        "cache_modified": False,
        "bulk_status": "not_tested",
        "bulk_http_status": None,
        "records_received": 0,
        "usable_records": 0,
        "targeted_symbol": "NVDA",
        "targeted_status": "not_tested",
        "targeted_http_status": None,
        "targeted_records_received": 0,
        "targeted_usable_records": 0,
    }
    try:
        records = _fetch_page(0, api_key)
        usable = sum(
            1 for item in records
            if isinstance(item.get("symbol"), str)
            and item.get("symbol", "").strip()
            and isinstance(item.get("floatShares"), (int, float))
            and not isinstance(item.get("floatShares"), bool)
            and item.get("floatShares", 0) > 0
        )
        result.update({
            "bulk_status": "ok" if usable else "no_usable_data",
            "bulk_http_status": 200,
            "records_received": len(records),
            "usable_records": usable,
        })
    except urllib.error.HTTPError as exc:
        result.update({"bulk_status": "unavailable", "bulk_http_status": exc.code})
    except Exception as exc:
        logger.warning("FMP bulk FLOAT diagnostic failed; error_type=%s", type(exc).__name__)
        result.update({"bulk_status": "unavailable", "bulk_error_type": type(exc).__name__})

    try:
        targeted = _fetch_symbol_float("NVDA", api_key)
        usable_targeted = any(
            isinstance(item, dict)
            and str(item.get("symbol", "")).strip().upper() == "NVDA"
            and isinstance(item.get("floatShares"), (int, float))
            and not isinstance(item.get("floatShares"), bool)
            and item.get("floatShares", 0) > 0
            for item in targeted
        )
        result.update({
            "targeted_status": "ok" if usable_targeted else "no_usable_data",
            "targeted_http_status": 200,
            "targeted_records_received": len(targeted),
            "targeted_usable_records": int(usable_targeted),
        })
    except urllib.error.HTTPError as exc:
        result.update({"targeted_status": "unavailable", "targeted_http_status": exc.code})
    except Exception as exc:
        logger.warning("FMP targeted FLOAT diagnostic failed; error_type=%s", type(exc).__name__)
        result.update({"targeted_status": "unavailable", "targeted_error_type": type(exc).__name__})

    result["status"] = (
        "ok" if result["bulk_status"] == "ok" and result["targeted_status"] == "ok"
        else "partial" if result["bulk_status"] == "ok" or result["targeted_status"] == "ok"
        else "unavailable"
    )
    # Legacy fields remain available for older UI clients.
    result["provider_http_status"] = result["bulk_http_status"]
    return result


@app.post("/api/diagnostics/float/fmp/resolve-legacy-cooldown")
def resolve_legacy_fmp_float_cooldown(request: Request):
    """Clear only an unclassified legacy cooldown after live FMP checks pass."""
    _authorize_strategy_access(request)
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="FMP_API_KEY no está configurada.")

    store = SnapshotStore()
    now = time.time()
    try:
        until = float(store.get_state("fmp_float_global_cooldown_until", 0) or 0)
        reason = store.get_state("fmp_float_global_cooldown_reason", "provider_cooldown")
        http_status = store.get_state("fmp_float_global_cooldown_http_status")
    except Exception as exc:
        logger.warning("Unable to read FMP cooldown state; error_type=%s", type(exc).__name__)
        raise HTTPException(status_code=503, detail="No se pudo leer el estado del cooldown.") from None

    if until <= now:
        return {"status": "already_expired", "cooldown_cleared": False}
    if reason != "provider_cooldown" or http_status is not None:
        raise HTTPException(
            status_code=409,
            detail="Cooldown conservado: tiene una causa explícita. No se eliminará automáticamente.",
        )

    # Verify both FMP routes now; never clear a known billing/rate-limit cooldown.
    try:
        bulk = _fetch_page(0, api_key)
        bulk_usable = any(
            isinstance(item.get("symbol"), str) and item.get("symbol", "").strip()
            and isinstance(item.get("floatShares"), (int, float))
            and not isinstance(item.get("floatShares"), bool)
            and item.get("floatShares", 0) > 0
            for item in bulk
        )
        targeted = _fetch_symbol_float("NVDA", api_key)
        targeted_usable = any(
            isinstance(item, dict)
            and str(item.get("symbol", "")).strip().upper() == "NVDA"
            and isinstance(item.get("floatShares"), (int, float))
            and not isinstance(item.get("floatShares"), bool)
            and item.get("floatShares", 0) > 0
            for item in targeted
        )
    except urllib.error.HTTPError as exc:
        logger.warning("FMP cooldown recovery check failed; provider_http_status=%s", exc.code)
        raise HTTPException(
            status_code=502,
            detail=f"FMP no superó la verificación actual (HTTP {exc.code}); cooldown conservado.",
        ) from None
    except Exception as exc:
        logger.warning("FMP cooldown recovery check failed; error_type=%s", type(exc).__name__)
        raise HTTPException(
            status_code=502,
            detail="FMP no superó la verificación actual; cooldown conservado.",
        ) from None

    if not bulk_usable or not targeted_usable:
        raise HTTPException(
            status_code=502,
            detail="FMP respondió, pero faltan datos FLOAT utilizables; cooldown conservado.",
        )

    # Expire the old global guard only after successful current checks.
    store.set_state("fmp_float_global_cooldown_until", 0)
    store.set_state("fmp_float_global_cooldown_reason", "cleared_after_verified_endpoints")
    store.set_state("fmp_float_global_cooldown_http_status", 0)
    logger.info("Cleared legacy FMP FLOAT cooldown after successful bulk and targeted checks.")
    return {
        "status": "ok",
        "cooldown_cleared": True,
        "verified_bulk_records": len(bulk),
        "verified_targeted_records": len(targeted),
        "cache_modified": False,
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
        "float_provider": "Schwab first; FMP cache for FLOAT only when FMP_API_KEY is configured",
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
            row.setdefault("float_shares", None)
            row.setdefault("float_as_of", None)
            row.setdefault("float_source", None)
        # Fill missing FLOAT from the local FMP cache; never call FMP from UI requests.
        apply_cached_float(rows, store)
        selected_strategy = active_strategy()
        if selected_strategy:
            rows = [
                row for row in rows
                if row.get("quote_is_fresh") is True
                and strategy_matches(selected_strategy, row)
            ]
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


def _authorize_strategy_access(request: Request) -> None:
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
def candles_status(symbol: str, limit: int = Query(500, ge=1, le=5000)):
    normalized = symbol.strip().upper()
    if not normalized or len(normalized) > 15:
        raise HTTPException(status_code=400, detail="Invalid ticker symbol.")
    try:
        store = SnapshotStore()
        candle_store = CandleStore(store.path)
        return {
            "symbol": normalized,
            "provider": "Schwab",
            "candles": candle_store.latest(normalized, limit),
            "rvol": candle_store.rvol(normalized),
        }
    except (OSError, RuntimeError, ValueError) + DB_ERRORS:
        raise HTTPException(status_code=503, detail="Candle storage is unavailable.") from None


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