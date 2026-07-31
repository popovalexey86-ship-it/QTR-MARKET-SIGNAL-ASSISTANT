from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from market_signal_assistant.derivatives.provider import DerivativesDataError
from market_signal_assistant.providers.bybit_liquidations import (
    BybitLiquidationAccumulator,
    BybitLiquidationStream,
)

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)


def row(
    side: str,
    size: str = "2",
    price: str = "100",
    timestamp: datetime = NOW,
) -> dict[str, object]:
    return {
        "T": int(timestamp.timestamp() * 1000),
        "s": "BTCUSDT",
        "S": side,
        "v": size,
        "p": price,
    }


def test_side_mapping_and_explicit_quote_notional() -> None:
    accumulator = BybitLiquidationAccumulator(clock=lambda: NOW)
    accumulator.ingest({"data": [row("Buy"), row("Sell", "3")]})
    assert accumulator.totals("BTCUSDT") == (200.0, 300.0)


def test_fifteen_minute_window_includes_boundary_and_expires_older() -> None:
    accumulator = BybitLiquidationAccumulator(clock=lambda: NOW)
    accumulator.ingest({"data": [
        row("Buy", timestamp=NOW - timedelta(minutes=15)),
        row("Sell", timestamp=NOW - timedelta(minutes=15, milliseconds=1)),
    ]})
    assert accumulator.totals("BTCUSDT") == (200.0, 0.0)


def test_malformed_message_is_atomic_and_controlled() -> None:
    accumulator = BybitLiquidationAccumulator(clock=lambda: NOW)
    with pytest.raises(DerivativesDataError, match="Malformed"):
        accumulator.ingest({"data": [row("Buy"), {"bad": "row"}]})
    assert accumulator.totals("BTCUSDT") == (0.0, 0.0)


def test_accumulator_is_thread_safe() -> None:
    accumulator = BybitLiquidationAccumulator(clock=lambda: NOW)

    def ingest(_: int) -> None:
        accumulator.ingest({"data": [row("Buy", "1", "10")]})

    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(executor.map(ingest, range(200)))
    assert accumulator.totals("BTCUSDT") == (2_000.0, 0.0)


def test_websocket_lifecycle_is_lazy_and_stop_is_idempotent() -> None:
    socket = Mock()
    factory = Mock(return_value=socket)
    stream = BybitLiquidationStream(
        BybitLiquidationAccumulator(clock=lambda: NOW),
        websocket_factory=factory,
    )
    factory.assert_not_called()
    assert stream.running is False

    stream.start("btcusdt")
    factory.assert_called_once_with(testnet=False, channel_type="linear")
    socket.all_liquidation_stream.assert_called_once()
    assert stream.running is True

    stream.stop()
    stream.stop()
    socket.exit.assert_called_once_with()
    assert stream.running is False
