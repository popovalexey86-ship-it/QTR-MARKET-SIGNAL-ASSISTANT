from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

from market_signal_assistant.qtr_micro_scalper.data.models import (
    LiquidationEvent,
    LiquidationSide,
)
from market_signal_assistant.qtr_micro_scalper.live.collector import (
    BybitPublicStream,
    WebSocketFactory,
    default_websocket_factory,
)

LiquidationSink = Callable[[LiquidationEvent], None]


class LiquidationCollector(BybitPublicStream):
    def __init__(
        self,
        symbols: Iterable[str],
        sink: LiquidationSink,
        *,
        websocket_factory: WebSocketFactory = default_websocket_factory,
        heartbeat_seconds: float = 20.0,
        reconnect_seconds: float = 0.25,
    ) -> None:
        normalized = _symbols(symbols)
        self._sink = sink
        super().__init__(
            topics=(f"allLiquidation.{symbol}" for symbol in normalized),
            websocket_factory=websocket_factory,
            heartbeat_seconds=heartbeat_seconds,
            reconnect_seconds=reconnect_seconds,
        )

    async def update_symbols(self, symbols: Iterable[str]) -> None:
        normalized = _symbols(symbols)
        await self.set_topics(
            f"allLiquidation.{symbol}" for symbol in normalized
        )

    def handle_payload(self, payload: dict[str, Any]) -> int:
        events = parse_liquidation_message(payload)
        for event in events:
            self._sink(event)
        return len(events)


def parse_liquidation_message(
    payload: dict[str, Any], *, received_at: datetime | None = None
) -> tuple[LiquidationEvent, ...]:
    now = received_at or datetime.now(UTC)
    data = payload.get("data")
    if not isinstance(data, list):
        return ()
    events: list[LiquidationEvent] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        try:
            wire_side = row["S"]
            if wire_side not in {"Buy", "Sell"}:
                raise ValueError("Unknown liquidation side.")
            price, quantity = float(row["p"]), float(row["v"])
            events.append(
                LiquidationEvent(
                    symbol=str(row["s"]),
                    exchange_at=_timestamp(row["T"]),
                    received_at=now,
                    side=LiquidationSide.LONG
                    if wire_side == "Buy"
                    else LiquidationSide.SHORT,
                    bankruptcy_price=price,
                    quantity=quantity,
                    quote_notional=price * quantity,
                    liquidation_id=_optional_text(row.get("i")),
                    sequence=_optional_int(row.get("seq")),
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


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)
