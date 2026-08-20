from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookState
from market_signal_assistant.qtr_micro_scalper.live.collector import (
    BybitPublicStream,
    WebSocketFactory,
    default_websocket_factory,
)


class OrderBookCollector(BybitPublicStream):
    def __init__(
        self,
        symbols: Iterable[str],
        *,
        depth: int = 50,
        websocket_factory: WebSocketFactory = default_websocket_factory,
        event_sink: Callable[[OrderBookEvent], None] | None = None,
        heartbeat_seconds: float = 20.0,
        reconnect_seconds: float = 0.25,
    ) -> None:
        normalized = _symbols(symbols)
        self._depth = depth
        self._states = {
            symbol: OrderBookState(
                symbol, depth=depth, require_contiguous_update_ids=False
            )
            for symbol in normalized
        }
        self._event_sink = event_sink
        super().__init__(
            topics=(f"orderbook.{depth}.{symbol}" for symbol in normalized),
            websocket_factory=websocket_factory,
            heartbeat_seconds=heartbeat_seconds,
            reconnect_seconds=reconnect_seconds,
        )

    async def update_symbols(self, symbols: Iterable[str]) -> None:
        normalized = _symbols(symbols)
        current = set(self._states)
        desired = set(normalized)
        for symbol in sorted(desired - current):
            self._states[symbol] = OrderBookState(
                symbol,
                depth=self._depth,
                require_contiguous_update_ids=False,
            )
        try:
            await self.set_topics(
                f"orderbook.{self._depth}.{symbol}" for symbol in normalized
            )
        except Exception:
            for symbol in desired - current:
                self._states.pop(symbol, None)
            raise
        for symbol in current - desired:
            self._states.pop(symbol, None)
    def state(self, symbol: str) -> OrderBookState:

        return self._states[symbol.strip().upper()]

    def handle_payload(self, payload: dict[str, Any]) -> int:
        event = parse_orderbook_message(payload)
        if event is None or event.symbol not in self._states:
            return 0
        self._states[event.symbol].process(event)
        if self._event_sink is not None:
            self._event_sink(event)
        return 1


def parse_orderbook_message(
    payload: dict[str, Any], *, received_at: datetime | None = None
) -> OrderBookEvent | None:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    try:
        update_id = int(data["u"])
        event_type = (
            OrderBookEventType.SNAPSHOT
            if payload.get("type") == "snapshot" or update_id == 1
            else OrderBookEventType.DELTA
        )
        return OrderBookEvent(
            symbol=str(data["s"]),
            event_type=event_type,
            exchange_at=_timestamp(payload.get("cts", payload.get("ts"))),
            received_at=received_at or datetime.now(UTC),
            update_id=update_id,
            bids=_levels(data.get("b")),
            asks=_levels(data.get("a")),
            cross_sequence=_optional_int(data.get("seq")),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _levels(value: object) -> tuple[OrderBookLevel, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(OrderBookLevel(float(item[0]), float(item[1])) for item in value)


def _symbols(values: Iterable[str]) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(v.strip().upper() for v in values if v.strip()))
    return result


def _timestamp(value: object) -> datetime:
    return datetime.fromtimestamp(int(str(value)) / 1000, tz=UTC)


def _optional_int(value: object) -> int | None:
    return None if value is None else int(str(value))
