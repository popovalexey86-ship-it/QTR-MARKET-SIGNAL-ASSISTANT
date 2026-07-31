from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from market_signal_assistant.models import AssetClass, Instrument
from market_signal_assistant.providers import (
    BybitPublicProvider,
    CsvMarketDataProvider,
    MarketDataError,
)


def test_csv_provider_loads_canonical_history(tmp_path: Path) -> None:
    path = tmp_path / "candles.csv"
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-01-01T00:00:00Z,100,102,99,101,10\n"
        "2026-01-01T01:00:00Z,101,103,100,102,11\n",
        encoding="utf-8",
    )
    instrument = Instrument("BTCUSDT", AssetClass.CRYPTO)

    result = CsvMarketDataProvider({"BTCUSDT": path}).load(
        instrument,
        "1h",
        100,
    )

    assert result.instrument is instrument
    assert len(result.candles) == 2
    assert result.candles[0].timestamp.tzinfo is UTC


def test_csv_provider_rejects_unsorted_or_duplicate_rows(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-01-01T01:00:00Z,100,102,99,101,10\n"
        "2026-01-01T01:00:00Z,101,103,100,102,11\n",
        encoding="utf-8",
    )

    with pytest.raises(MarketDataError, match="Invalid CSV"):
        CsvMarketDataProvider({"BTCUSDT": path}).load(
            Instrument("BTCUSDT", AssetClass.CRYPTO),
            "1h",
            100,
        )


def test_bybit_provider_normalizes_reverse_order_and_excludes_open_candle() -> None:
    now = datetime.now(UTC)
    completed = now - timedelta(hours=2)
    forming = now - timedelta(minutes=10)

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        assert "api.bybit.com" in url
        assert timeout == 3.0
        return {
            "retCode": 0,
            "result": {
                "list": [
                    [
                        str(int(forming.timestamp() * 1000)),
                        "101",
                        "103",
                        "100",
                        "102",
                        "10",
                    ],
                    [
                        str(int(completed.timestamp() * 1000)),
                        "100",
                        "102",
                        "99",
                        "101",
                        "11",
                    ],
                ]
            },
        }

    result = BybitPublicProvider(getter=getter, timeout=3.0).load(
        Instrument("BTCUSDT", AssetClass.CRYPTO),
        "1h",
        100,
    )

    assert len(result.candles) == 1
    assert result.candles[0].close == 101.0


def test_bybit_provider_retries_without_real_sleep() -> None:
    attempts = 0
    delays: list[float] = []

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        nonlocal attempts
        del url, timeout
        attempts += 1
        if attempts < 3:
            raise MarketDataError("timeout")
        now = datetime.now(UTC) - timedelta(hours=2)
        return {
            "retCode": 0,
            "result": {
                "list": [
                    [
                        str(int(now.timestamp() * 1000)),
                        "100",
                        "102",
                        "99",
                        "101",
                        "10",
                    ]
                ]
            },
        }

    provider = BybitPublicProvider(
        getter=getter,
        sleep=delays.append,
    )
    provider.load(
        Instrument("BTCUSDT", AssetClass.CRYPTO),
        "1h",
        100,
    )

    assert attempts == 3
    assert delays == [0.5, 1.5]
