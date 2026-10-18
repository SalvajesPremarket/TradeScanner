"""Live listed-symbol universe from Nasdaq Trader's public symbol directory."""
from __future__ import annotations

import csv
import io
import re
import urllib.error
import urllib.request
from typing import Any

SYMBOL_DIRECTORY_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")


class UniverseUnavailableError(RuntimeError):
    """Raised when the official symbol directory cannot be fetched or parsed."""


def parse_symbol_directory(content: str) -> dict[str, Any]:
    """Parse the official pipe-delimited file without fabricating symbols."""
    reader = csv.reader(io.StringIO(content), delimiter="|")
    try:
        header = next(reader)
    except StopIteration:
        raise UniverseUnavailableError("Nasdaq Trader symbol directory is empty.") from None

    expected = ["Nasdaq Traded", "Symbol", "Security Name"]
    if header[:3] != expected or len(header) < 11:
        raise UniverseUnavailableError("Nasdaq Trader symbol directory format was not recognized.")

    symbols: list[str] = []
    seen: set[str] = set()
    generated_at: str | None = None
    for fields in reader:
        if not fields:
            continue
        first = fields[0].strip()
        if first.lower().startswith("file creation time"):
            generated_at = first.partition(":")[2].strip() or None
            continue
        if len(fields) < 11 or fields[0].strip().upper() != "Y":
            continue
        if fields[5].strip().upper() == "Y":  # Exclude ETFs from the common-stock universe.
            continue
        if fields[7].strip().upper() != "N":
            continue

        # The NASDAQ Symbol column is the directory's feed-oriented symbol;
        # fall back to Symbol when it is empty. Unsupported forms are skipped.
        symbol = (fields[10].strip() or fields[1].strip()).upper()
        if not _SYMBOL_RE.fullmatch(symbol) or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)

    if not symbols:
        raise UniverseUnavailableError("Nasdaq Trader directory contained no supported symbols.")
    return {
        "source": SYMBOL_DIRECTORY_URL,
        "generated_at": generated_at,
        "symbols": symbols,
        "count": len(symbols),
    }


def load_symbol_universe(timeout: int = 20) -> dict[str, Any]:
    """Fetch the current public symbol directory over HTTPS."""
    request = urllib.request.Request(
        SYMBOL_DIRECTORY_URL,
        headers={"User-Agent": "TradeScanner/1.0", "Accept": "text/plain"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            content = response.read().decode("utf-8-sig")
    except (urllib.error.URLError, TimeoutError, UnicodeDecodeError, OSError) as exc:
        raise UniverseUnavailableError(
            f"Could not retrieve Nasdaq Trader symbol directory ({type(exc).__name__})."
        ) from None
    return parse_symbol_directory(content)
