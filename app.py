import asyncio
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from tradescanner.candles import CandleStore
from tradescanner.core import apply_filters, normalize_schwab_quotes
from tradescanner.engine import get_engine_status, record_engine_status, run_scan_cycle
from tradescanner.snapshots import SnapshotStore, collect_snapshots
from tradescanner.universe import UniverseUnavailableError, load_symbol_universe
from tradescanner.schwab import (
    SchwabAPIError,
    SchwabConfigurationError,
    get_quotes,
)

ROOT = Path(__file__).resolve().parent


async def _automatic_scan_loop(stop: asyncio.Event) -> None:
    enabled = os.getenv("TRADESCANNER_AUTO_SCAN_ENABLED", "true").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return
    try:
        interval = max(10, int(os.getenv("TRADESCANNER_SCAN_INTERVAL_SECONDS", "30")))
    except ValueError:
        interval = 30

    while not stop.is_set():
        try:
            await asyncio.to_thread(run_scan_cycle)
        except Exception as exc:
            # Store only the exception class; never leak provider responses or secrets.
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


app = FastAPI(title="TradeScanner", version="0.3.0", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(ROOT / "index.html")


@app.get("/health")
def health():
    return {"status": "ok", "service": "TradeScanner"}


@app.get("/api/status")
def status():
    configured = all(
        os.getenv(name)
        for name in ("SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET", "SCHWAB_REFRESH_TOKEN")
    )
    engine = get_engine_status()
    connected = bool(engine.get("last_success_at"))
    return {
        "service": "TradeScanner",
        "provider": "Schwab",
        "provider_configured": configured,
        "market_data_connected": connected,
        "results_published": 0,
        "engine_status": engine.get("status", "starting"),
        "message": (
            "Schwab quotes have been received by the scanner."
            if connected
            else "Connection is not verified until a Schwab quote batch succeeds."
            if configured
            else "Schwab credentials are not configured."
        ),
    }


@app.get("/api/engine/status")
def engine_status():
    state = get_engine_status()
    enabled = os.getenv("TRADESCANNER_AUTO_SCAN_ENABLED", "true").strip().lower() in {
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
        return {"results": rows, "limit": limit}
    except (OSError, RuntimeError, sqlite3.Error, ValueError):
        raise HTTPException(status_code=503, detail="Snapshot storage is unavailable.") from None


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
    if len(set(symbol.upper() for symbol in requested)) > 10000:
        raise HTTPException(status_code=400, detail="A scan is limited to 10000 unique symbols.")

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
