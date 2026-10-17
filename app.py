import os
import sqlite3
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from tradescanner.core import apply_filters, normalize_schwab_quotes
from tradescanner.snapshots import SnapshotStore, collect_snapshots
from tradescanner.universe import UniverseUnavailableError, load_symbol_universe\nfrom tradescanner.schwab import (
    SchwabAPIError,
    SchwabConfigurationError,
    get_quotes,
)

ROOT = Path(__file__).resolve().parent
app = FastAPI(title="TradeScanner", version="0.2.0")


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
    return {
        "service": "TradeScanner",
        "provider": "Schwab",
        "provider_configured": configured,
        "market_data_connected": False,
        "results_published": 0,
        "message": (
            "Credentials configured; connection is verified only after a successful quote request."
            if configured
            else "Schwab credentials are not configured."
        ),
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
        return {"results": SnapshotStore().latest(limit), "limit": limit}
    except (OSError, RuntimeError, sqlite3.Error, ValueError):
        raise HTTPException(status_code=503, detail="Snapshot storage is unavailable.") from None


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
