import os
import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from tradescanner.core import apply_filters, normalize_schwab_quotes
from tradescanner.snapshots import SnapshotStore
from tradescanner.schwab import (
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
