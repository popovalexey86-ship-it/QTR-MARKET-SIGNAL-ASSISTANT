from collections.abc import Mapping
from typing import Any

import pytest

from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.providers import BybitPublicProvider, MarketDataError


@pytest.mark.parametrize("symbol", ["JNJUSDT", "XOMUSDT", "SPOTUSDT"])
def test_stock_symbol_type_is_not_crypto_inplay(symbol: str) -> None:
    instrument = CatalogInstrument(
        symbol=symbol,
        quote_coin="USDT",
        status="Trading",
        turnover_24h=10_000_000,
        bid=100,
        ask=100.1,
        base_coin=symbol.removesuffix("USDT"),
        settle_coin="USDT",
        contract_type="LinearPerpetual",
        symbol_type="stock",
        is_pre_listing=False,
    )

    assert instrument.is_crypto_linear_usdt is False


def test_bybit_existing_provider_lists_instruments_with_liquidity_metadata() -> None:
    calls: list[str] = []

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        calls.append(url)
        assert timeout == 4.0
        if "instruments-info" in url:
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "symbol": "BTCUSDT",
                            "baseCoin": "BTC",
                            "quoteCoin": "USDT",
                            "settleCoin": "USDT",
                            "contractType": "LinearPerpetual",
                            "symbolType": "",
                            "status": "Trading",
                            "isPreListing": False,
                        },
                        {
                            "symbol": "ETHUSDC",
                            "baseCoin": "ETH",
                            "quoteCoin": "USDC",
                            "settleCoin": "USDC",
                            "contractType": "LinearPerpetual",
                            "symbolType": "innovation",
                            "status": "Trading",
                            "isPreListing": False,
                        },
                    ]
                },
            }
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "turnover24h": "125000000",
                        "bid1Price": "60000",
                        "ask1Price": "60006",
                    },
                    {
                        "symbol": "ETHUSDC",
                        "turnover24h": "20000000",
                        "bid1Price": "3000",
                        "ask1Price": "3001",
                    },
                ]
            },
        }

    instruments = BybitPublicProvider(getter=getter, timeout=4.0).list_instruments()

    assert len(calls) == 2
    assert instruments[0].symbol == "BTCUSDT"
    assert instruments[0].quote_coin == "USDT"
    assert instruments[0].status == "Trading"
    assert instruments[0].base_coin == "BTC"
    assert instruments[0].contract_type == "LinearPerpetual"
    assert instruments[0].symbol_type == ""
    assert instruments[0].settle_coin == "USDT"
    assert instruments[0].is_pre_listing is False
    assert instruments[0].turnover_24h == 125_000_000
    assert instruments[0].spread_ratio == pytest.approx(6 / 60_003)


def test_bybit_catalog_rejects_malformed_payload() -> None:
    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        del url, timeout
        return {"retCode": 0, "result": {"list": "invalid"}}

    with pytest.raises(MarketDataError, match="catalog"):
        BybitPublicProvider(getter=getter).list_instruments()
