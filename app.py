import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent
app = FastAPI(title="TradeScanner", version="0.1.0")

app.mount("/static", StaticFiles(directory=ROOT), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(ROOT / "index.html")


@app.get("/health")
def health():
    return {"status": "ok", "service": "TradeScanner"}


@app.get("/api/status")
def status():
    # Report configuration state only; never expose credentials or token values.
    schwab_configured = all(
        os.getenv(name)
        for name in ("SCHWAB_CLIENT_ID", "SCHWAB_CLIENT_SECRET", "SCHWAB_REFRESH_TOKEN")
    )
    return {
        "service": "TradeScanner",
        "provider": "Schwab",
        "provider_configured": schwab_configured,
        "market_data_connected": False,
        "results_published": 0,
        "message": (
            "Schwab credentials configured; live connection not yet initialized."
            if schwab_configured
            else "Schwab credentials are not configured."
        ),
    }
