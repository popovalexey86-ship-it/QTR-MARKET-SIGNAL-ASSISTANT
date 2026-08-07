from datetime import UTC, datetime

from market_signal_assistant.catalog import MARKET_INSTRUMENTS, MARKETS_PRESET
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
)
from market_signal_assistant.providers import RoutingMarketDataProvider
from market_signal_assistant.telegram.parsing import parse_command
from market_signal_assistant.web.app import INSTRUMENTS

NOW = datetime(2026, 8, 1, tzinfo=UTC)


class RecordingProvider:
    def __init__(self) -> None:
        self.loaded: list[Instrument] = []

    def load(
        self, instrument: Instrument, interval: str, limit: int
    ) -> MarketSeries:
        del limit
        self.loaded.append(instrument)
        return MarketSeries(
            instrument,
            interval,
            (Candle(NOW, 100, 101, 99, 100, 10),),
        )


def test_markets_command_and_web_catalog_use_shared_catalog() -> None:
    command = parse_command("/markets")
    assert command.request is not None
    assert command.request.instruments == MARKETS_PRESET
    assert tuple(
        (item["symbol"], item["asset_class"]) for item in INSTRUMENTS
    ) == tuple(
        (item.symbol, item.asset_class.value) for item in MARKET_INSTRUMENTS
    )


def test_every_catalog_instrument_has_an_explicit_provider_route() -> None:
    crypto = RecordingProvider()
    traditional = RecordingProvider()
    routing = RoutingMarketDataProvider(
        crypto_provider=crypto,
        traditional_provider=traditional,
    )
    for instrument in MARKET_INSTRUMENTS:
        routing.load(instrument, "1h", 1)

    assert tuple(item.asset_class for item in crypto.loaded) == (
        AssetClass.CRYPTO,
        AssetClass.CRYPTO,
        AssetClass.CRYPTO,
    )
    assert tuple(item.asset_class for item in traditional.loaded) == (
        AssetClass.STOCK,
        AssetClass.FUND,
        AssetClass.FOREX,
    )
