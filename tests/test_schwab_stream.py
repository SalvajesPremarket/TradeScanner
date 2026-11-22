import time

from tradescanner import schwab_stream


def test_reconnect_backoff_increases_after_short_disconnects_and_resets_after_stability():
    assert schwab_stream._next_retry_delay(2, 10) == 4
    assert schwab_stream._next_retry_delay(32, 10) == 60
    assert schwab_stream._next_retry_delay(32, 90) == 2


def _item(*, age_seconds=0, price=12.5, close=11.0):
    stamp = int((time.time() - age_seconds) * 1000)
    return {
        "key": "aapl",
        "1": {
            "3": price,
            "8": 125000,
            "15": close,
            "28": 1.5,
            "47": stamp,
        },
    }


def test_normalise_stream_item_accepts_fresh_level_one_quote():
    row = schwab_stream._normalise_stream_item(_item())

    assert row is not None
    assert row["symbol"] == "AAPL"
    assert row["price"] == 12.5
    assert row["previous_close"] == 11.0
    assert row["volume"] == 125000
    assert row["quote_is_fresh"] is True
    assert row["data_source"] == "schwab_stream"


def test_normalise_stream_item_rejects_stale_quote():
    assert schwab_stream._normalise_stream_item(_item(age_seconds=301)) is None


def test_normalise_stream_item_rejects_invalid_prices():
    assert schwab_stream._normalise_stream_item(_item(price=0)) is None
    assert schwab_stream._normalise_stream_item(_item(close=0)) is None


def test_symbol_batches_respects_configured_maximum(monkeypatch):
    monkeypatch.setattr(
        schwab_stream,
        "load_symbol_universe",
        lambda: {"symbols": ["aapl", "msft", "AAPL", "nvda", " " ]},
    )
    monkeypatch.setenv("TRADESCANNER_STREAM_MAX_SYMBOLS", "2")

    assert schwab_stream._symbol_batches() == [["AAPL", "MSFT"], ["NVDA"]]
