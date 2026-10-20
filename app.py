import asyncio
import hmac
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.responses import FileResponse

from tradescanner.candles import CandleStore
from tradescanner.core import apply_filters, normalize_schwab_quotes
from tradescanner.float_provider import apply_cached_float
from tradescanner.infisical_vault import is_configured as infisical_is_configured
from tradescanner.engine import get_engine_status, record_engine_status, run_scan_cycle
from tradescanner.snapshots import SnapshotStore, collect_snapshots
from tradescanner.strategies import (active_strategy, delete_strategy, strategy_matches, list_strategies, save_strategy, set_strategy_enabled)
from tradescanner.universe import UniverseUnavailableError, load_symbol_universe
from tradescanner.oauth import router as oauth_router
from tradescanner.schwab import (
    SchwabAPIError,
    SchwabConfigurationError,
    get_quotes,
)

ROOT = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)


async def _automatic_scan_loop(stop: asyncio.Event) -> None:
    enabled = os.getenv("TRADESCANNER_AUTO_SCAN_ENABLED", "false").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        logger.warning("Automatic scan loop disabled: TRADESCANNER_AUTO_SCAN_ENABLED is false")
        return
    try:
        interval = max(10, int(os.getenv("TRADESCANNER_SCAN_INTERVAL_SECONDS", "30")))
    except ValueError:
        interval = 30
    logger.info("Automatic scan loop enabled; interval_seconds=%s", interval)

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
            except (OSError, RuntimeError, sqlite3.Error):
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
    try:
        yield
    finally:
        stop.set()
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
    if isinstance(last_quote_at, str):
        try:
            parsed_quote_at = datetime.fromisoformat(last_quote_at)
            if parsed_quote_at.tzinfo is None:
                parsed_quote_at = parsed_quote_at.replace(tzinfo=timezone.utc)
            quote_age_seconds = (datetime.now(timezone.utc) - parsed_quote_at.astimezone(timezone.utc)).total_seconds()
            connected = -30 <= quote_age_seconds <= 300
        except (TypeError, ValueError, OverflowError):
            connected = False
    try:
        latest_rows = SnapshotStore().latest(limit=1000)
        results_published = sum(1 for row in latest_rows if row.get("quote_is_fresh") is True)
    except (OSError, RuntimeError, sqlite3.Error, ValueError):
        results_published = 0
    return {
        "service": "TradeScanner",
        "provider": "Schwab",
        "provider_configured": configured,
        "infisical_configured": infisical_is_configured(),
        "float_provider_configured": bool(os.getenv("FMP_API_KEY", "").strip()),
        "market_data_connected": connected,
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
    return {
        "automatic_scan_enabled": enabled,
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
            "storage": "sqlite",
            "storage_path_configured": bool(os.getenv("TRADESCANNER_DB_PATH")),
            "durability_status": "not_verified",
            "durability_note": (
                "SQLite persistence is not guaranteed by setting a path alone; "
                "verify that TRADESCANNER_DB_PATH points to a durable mounted volume."
            ),
            **store.stats(),
        }
    except (OSError, RuntimeError, sqlite3.Error):
        raise HTTPException(status_code=503, detail="Snapshot storage is unavailable.") from None


@app.get("/api/snapshots/latest")
def snapshots_latest(limit: int = Query(100, ge=1, le=1000)):
    try:
        store = SnapshotStore()
        rows = store.latest(limit)
        candle_store = CandleStore(store.path)
        for row in rows:
            metrics = candle_store.rvol(row["symbol"])
            row["rvol"] = metrics["rvol"]
            row["rvol_sessions_used"] = metrics["sessions_used"]
            row["rvol_as_of"] = metrics.get("as_of")
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
    except (OSError, RuntimeError, sqlite3.Error, ValueError):
        raise HTTPException(status_code=503, detail="Snapshot storage is unavailable.") from None


def _authorize_strategy_access(request: Request) -> None:
    expected = os.getenv("TRADESCANNER_STRATEGY_ADMIN_TOKEN", "").strip()
    if len(expected) < 32:
        raise HTTPException(
            status_code=503,
            detail=(
                "Editor de estrategias bloqueado: configura TRADESCANNER_STRATEGY_ADMIN_TOKEN "
                "con al menos 32 caracteres aleatorios en el entorno privado del servicio."
            ),
        )
    supplied = request.headers.get("x-strategy-admin-token", "")
    if not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Clave del editor incorrecta o ausente.")


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
    strategy = active_strategy()
    if not strategy:
        return {"strategy": None, "symbols": []}
    store = SnapshotStore()
    rows = store.latest(1000)
    candle_store = CandleStore(store.path)
    for row in rows:
        row["rvol"] = candle_store.rvol(row["symbol"])["rvol"]
    matches = [
        row.get("symbol") for row in rows
        if row.get("symbol") and row.get("quote_is_fresh") is True
        and strategy_matches(strategy, row)
    ]
    return {"strategy": {"id": strategy["id"], "name": strategy["name"]}, "symbols": matches}


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
    except (OSError, RuntimeError, sqlite3.Error, ValueError):
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
    except (OSError, RuntimeError, sqlite3.Error):
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
