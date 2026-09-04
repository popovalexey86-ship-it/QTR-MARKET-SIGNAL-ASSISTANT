from __future__ import annotations

from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import pytest

from market_signal_assistant.qtr_signal_outcome.bybit_provider import (
    BybitKlineProvider,
    CachingMarketDataProvider,
    MarketDataProviderError,
)
from market_signal_assistant.qtr_signal_outcome.models import MarketCandle
from qtr_signal_outcome.helpers import NOW


def row(minute: int) -> list[str]:
    opened = NOW + timedelta(minutes=minute)
    return [
        str(int(opened.timestamp() * 1000)),
        "100",
        "101",
        "99",
        "100.5",
        "10",
        "1000",
    ]


def test_bybit_reverse_response_is_sorted_ascending() -> None:
    seen: list[str] = []

    def getter(url: str, timeout: float) -> dict[str, object]:
        del timeout
        seen.append(url)
        return {"retCode": 0, "result": {"list": [row(2), row(1)]}}

    candles = BybitKlineProvider(getter=getter).fetch(
        "BTCUSDT", NOW, NOW + timedelta(minutes=3)
    )
    assert candles[0].opened_at < candles[1].opened_at
    query = parse_qs(urlsplit(seen[0]).query)
    assert query["category"] == ["linear"]
    assert query["interval"] == ["1"]


@pytest.mark.parametrize(
    "payload",
    (
        {"retCode": 10001, "result": {"list": []}},
        {"retCode": 0, "result": {}},
        {"retCode": 0, "result": {"list": [["bad"]]}},
    ),
)
def test_bybit_bad_response_is_rejected(payload: dict[str, object]) -> None:
    def getter(url: str, timeout: float) -> dict[str, object]:
        del url, timeout
        return payload

    with pytest.raises(MarketDataProviderError):
        BybitKlineProvider(getter=getter).fetch(
            "BTCUSDT", NOW, NOW + timedelta(minutes=5)
        )


def test_provider_failure_is_normalized() -> None:
    def getter(url: str, timeout: float) -> dict[str, object]:
        del url, timeout
        raise OSError("offline")

    with pytest.raises(MarketDataProviderError):
        BybitKlineProvider(getter=getter).fetch(
            "BTCUSDT", NOW, NOW + timedelta(minutes=5)
        )


def test_exact_range_cache_avoids_duplicate_request() -> None:
    class Provider:
        calls = 0

        def fetch(
            self, symbol: str, start: object, end: object
        ) -> tuple[MarketCandle, ...]:
            del symbol, start, end
            self.calls += 1
            return ()

    provider = Provider()
    cached = CachingMarketDataProvider(provider)

    cached.fetch("BTCUSDT", NOW, NOW + timedelta(minutes=5))
    cached.fetch("BTCUSDT", NOW, NOW + timedelta(minutes=5))

    assert provider.calls == 1
