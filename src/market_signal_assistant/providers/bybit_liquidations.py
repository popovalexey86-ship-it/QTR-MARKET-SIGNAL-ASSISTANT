from __future__ import annotations

import math
from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any, Protocol

from market_signal_assistant.derivatives.provider import DerivativesDataError


class LiquidationSocket(Protocol):
    def all_liquidation_stream(
        self, symbol: str, callback: Callable[[object], None]
    ) -> object: ...

    def exit(self) -> object: ...


WebSocketFactory = Callable[..., LiquidationSocket]


@dataclass(frozen=True, slots=True)
class _Liquidation:
    timestamp: datetime
    long_notional: float
    short_notional: float


class BybitLiquidationAccumulator:
    """Thread-safe rolling liquidation quote-notional by symbol."""

    def __init__(
        self,
        window: timedelta = timedelta(minutes=15),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if window <= timedelta(0):
            raise ValueError("Liquidation window must be positive.")
        self._window = window
        self._clock = clock or (lambda: datetime.now(UTC))
        self._events: dict[str, deque[_Liquidation]] = defaultdict(deque)
        self._lock = Lock()

    def ingest(self, message: object) -> None:
        """Parse one message atomically or raise a controlled data error."""
        rows = _liquidation_rows(message)
        parsed = tuple(_parse_liquidation(row) for row in rows)
        with self._lock:
            for symbol, event in parsed:
                self._events[symbol].append(event)
            self._prune_locked(self._now())

    def totals(self, symbol: str) -> tuple[float, float]:
        with self._lock:
            self._prune_locked(self._now())
            events = self._events.get(symbol.strip().upper(), ())
            return (
                sum(event.long_notional for event in events),
                sum(event.short_notional for event in events),
            )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise DerivativesDataError(
                "Liquidation clock must be timezone-aware."
            )
        return value

    def _prune_locked(self, now: datetime) -> None:
        cutoff = now - self._window
        for symbol in tuple(self._events):
            retained = deque(
                event
                for event in self._events[symbol]
                if event.timestamp >= cutoff
            )
            if retained:
                self._events[symbol] = retained
            else:
                del self._events[symbol]


class BybitLiquidationStream:
    """Explicit lifecycle wrapper for Bybit's public liquidation stream."""

    def __init__(
        self,
        accumulator: BybitLiquidationAccumulator,
        *,
        testnet: bool = False,
        channel_type: str = "linear",
        websocket_factory: WebSocketFactory | None = None,
    ) -> None:
        self._accumulator = accumulator
        self._testnet = testnet
        self._channel_type = channel_type
        self._factory = websocket_factory or _pybit_websocket_factory
        self._socket: LiquidationSocket | None = None

    @property
    def running(self) -> bool:
        return self._socket is not None

    def start(self, symbol: str) -> None:
        if self._socket is not None:
            raise RuntimeError("Bybit liquidation stream is already running.")
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("Liquidation symbol cannot be empty.")
        try:
            socket = self._factory(
                testnet=self._testnet,
                channel_type=self._channel_type,
            )
            socket.all_liquidation_stream(
                normalized_symbol, self._accumulator.ingest
            )
        except DerivativesDataError:
            raise
        except Exception as error:
            if "socket" in locals():
                socket.exit()
            raise DerivativesDataError(
                f"Bybit liquidation stream failed: {type(error).__name__}."
            ) from None
        self._socket = socket

    def stop(self) -> None:
        socket, self._socket = self._socket, None
        if socket is not None:
            try:
                socket.exit()
            except Exception as error:
                raise DerivativesDataError(
                    f"Bybit liquidation stream stop failed: {type(error).__name__}."
                ) from None


def _pybit_websocket_factory(**kwargs: object) -> LiquidationSocket:
    try:
        from pybit.unified_trading import WebSocket  # type: ignore[import-untyped]
    except ImportError:
        raise DerivativesDataError(
            "WebSocket support requires the optional 'websocket' dependency."
        ) from None
    return WebSocket(**kwargs)  # type: ignore[no-any-return]


def _liquidation_rows(message: object) -> list[Any]:
    if not isinstance(message, Mapping):
        raise DerivativesDataError("Malformed Bybit liquidation message.")
    data = message.get("data")
    if not isinstance(data, list):
        raise DerivativesDataError("Malformed Bybit liquidation message.")
    return data


def _parse_liquidation(row: object) -> tuple[str, _Liquidation]:
    if not isinstance(row, Mapping):
        raise DerivativesDataError("Malformed Bybit liquidation row.")
    try:
        symbol = str(row["s"]).strip().upper()
        side = row["S"]
        timestamp = datetime.fromtimestamp(float(row["T"]) / 1000, tz=UTC)
        size = float(row["v"])
        bankruptcy_price = float(row["p"])
    except (KeyError, TypeError, ValueError, OSError):
        raise DerivativesDataError("Malformed Bybit liquidation row.") from None
    if (
        not symbol
        or side not in ("Buy", "Sell")
        or not math.isfinite(size)
        or not math.isfinite(bankruptcy_price)
        or size < 0
        or bankruptcy_price < 0
    ):
        raise DerivativesDataError("Malformed Bybit liquidation row.")
    quote_notional = size * bankruptcy_price
    # Bybit: Buy means a liquidated long; Sell means a liquidated short.
    return symbol, _Liquidation(
        timestamp=timestamp,
        long_notional=quote_notional if side == "Buy" else 0.0,
        short_notional=quote_notional if side == "Sell" else 0.0,
    )
