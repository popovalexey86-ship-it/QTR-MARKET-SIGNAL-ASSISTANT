from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from market_signal_assistant.qtr_micro_scalper.data.models import (
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowAccumulator
from market_signal_assistant.qtr_micro_scalper.live.collector import (
    BybitPublicStream,
    WebSocketFactory,
    default_websocket_factory,
)


class PublicTradeCollector(BybitPublicStream):
    def __init__(
        self,
        symbols: Iterable[str],
        accumulator: TradeFlowAccumulator,
        *,
        websocket_factory: WebSocketFactory = default_websocket_factory,
        event_sink: Callable[[PublicTradeEvent], None] | None = None,
        heartbeat_seconds: float = 20.0,
        reconnect_seconds: float = 0.25,
    ) -> None:
        normalized = _symbols(symbols)
        self._accumulator = accumulator
        self._event_sink = event_sink
        super().__init__(
            topics=(f"publicTrade.{symbol}" for symbol in normalized),
            websocket_factory=websocket_factory,
            heartbeat_seconds=heartbeat_seconds,
            reconnect_seconds=reconnect_seconds,
        )

    async def update_symbols(self, symbols: Iterable[str]) -> None:
        normalized = _symbols(symbols)
        await self.set_topics(
            f"publicTrade.{symbol}" for symbol in normalized
        )

    def handle_payload(self, payload: dict[str, Any]) -> int:
        accepted = 0
        for event in parse_public_trade_message(payload):
            accepted += int(self._accumulator.ingest(event).accepted)
            if self._event_sink is not None:
                self._event_sink(event)
        return accepted


def parse_public_trade_message(
    payload: dict[str, Any], *, received_at: datetime | None = None
) -> tuple[PublicTradeEvent, ...]:
    now = received_at or datetime.now(UTC)
    data = payload.get("data")
    if not isinstance(data, list):
        return ()
    events: list[PublicTradeEvent] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        try:
            wire_side = row["S"]
            if wire_side not in {"Buy", "Sell"}:
                raise ValueError("Unknown public trade side.")
            price, quantity = float(row["p"]), float(row["v"])
            events.append(
                PublicTradeEvent(
                    symbol=str(row["s"]),
                    trade_id=str(row["i"]),
                    exchange_at=_timestamp(row["T"]),
                    received_at=now,
                    side=TradeSide.BUY if wire_side == "Buy" else TradeSide.SELL,
                    price=price,
                    quantity=quantity,
                    quote_notional=price * quantity,
                    sequence=_optional_int(row.get("seq")),
                    is_block_trade=bool(row.get("BT", False)),
                    is_rpi_trade=bool(row.get("RPI", False)),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return tuple(events)


def _symbols(values: Iterable[str]) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(v.strip().upper() for v in values if v.strip()))
    return result


def _timestamp(value: object) -> datetime:
    return datetime.fromtimestamp(int(str(value)) / 1000, tz=UTC)


def _optional_int(value: object) -> int | None:
    return None if value is None else int(str(value))
