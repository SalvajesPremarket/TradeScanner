import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from tradescanner.schwab import (
    SchwabAPIError,
    SchwabConfigurationError,
    get_quotes,
)

ROOT = Path(__file__).resolve().parent
app = FastAPI(title="TradeScanner", version="0.1.0")


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
            "Credentials configured; live data is verified only when a quote request succeeds."
            if configured
            else "Schwab credentials are not configured."
        ),
    }


@app.get("/api/quotes")
def quotes(symbols: str = Query(..., description="Comma-separated ticker symbols")):
    requested = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]
    try:
        return {
            "provider": "Schwab",
            "count": len(requested),
            "quotes": get_quotes(requested),
        }
    except SchwabConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except SchwabAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from None
