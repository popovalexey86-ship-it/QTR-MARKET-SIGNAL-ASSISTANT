from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from market_signal_assistant.derivatives.provider import DerivativesDataError
from market_signal_assistant.providers.bybit_derivatives import (
    BybitDerivativesProvider,
)
from market_signal_assistant.providers.bybit_liquidations import (
    BybitLiquidationAccumulator,
)

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)


def payloads() -> dict[str, Mapping[str, Any]]:
    return {
        "funding/history": {
            "retCode": 0,
            "result": {"list": [{"fundingRate": "0.00025"}]},
        },
        "open-interest": {
            "retCode": 0,
            "result": {"list": [
                {"timestamp": "1000", "openInterest": "100"},
                {"timestamp": "2000", "openInterest": "110"},
            ]},
        },
        "kline": {
            "retCode": 0,
            "result": {"list": [
                ["1000", "0", "0", "0", "100", "100"],
                ["2000", "0", "0", "0", "105", "125"],
            ]},
        },
    }


def test_collect_maps_funding_oi_price_and_volume_without_ticker() -> None:
    responses = payloads()
    urls: list[str] = []

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        urls.append(url)
        assert timeout == 3.0
        return next(value for key, value in responses.items() if key in url)

    result = BybitDerivativesProvider(
        BybitLiquidationAccumulator(clock=lambda: NOW),
        getter=getter,
        timeout=3.0,
        clock=lambda: NOW,
    ).collect("btcusdt")

    assert result.symbol == "BTCUSDT"
    assert result.funding_rate == pytest.approx(0.00025)
    assert result.open_interest == 110.0
    assert result.open_interest_change == pytest.approx(0.10)
    assert result.price_change == pytest.approx(0.05)
    assert result.volume_change == pytest.approx(0.25)
    assert len(urls) == 3
    assert not any("ticker" in url for url in urls)


@pytest.mark.parametrize(
    ("endpoint", "replacement"),
    [
        ("funding/history", {"retCode": 0, "result": {}}),
        ("open-interest", {"retCode": 0, "result": {"list": []}}),
        ("kline", {"retCode": 0, "result": {"list": [["bad"]]}}),
        ("funding/history", {"retCode": 10001, "result": {"list": []}}),
    ],
)
def test_malformed_rest_payload_is_controlled(
    endpoint: str, replacement: Mapping[str, Any]
) -> None:
    responses = payloads()
    responses[endpoint] = replacement

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        del timeout
        return next(value for key, value in responses.items() if key in url)

    with pytest.raises(DerivativesDataError):
        BybitDerivativesProvider(
            BybitLiquidationAccumulator(clock=lambda: NOW),
            getter=getter,
            clock=lambda: NOW,
        ).collect("BTCUSDT")
