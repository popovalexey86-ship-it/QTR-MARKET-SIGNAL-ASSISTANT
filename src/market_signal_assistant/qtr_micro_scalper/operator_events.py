"""Bounded observational projection of existing runtime transitions."""

from __future__ import annotations

import hashlib
from collections import deque
from threading import Lock
from typing import TYPE_CHECKING

from market_signal_assistant.qtr_micro_scalper.control_models import (
    MAX_RECENT_EVENTS,
    MAX_TEXT,
    RecentEvent,
)

if TYPE_CHECKING:
    from market_signal_assistant.qtr_micro_scalper.shadow_decision import ShadowTrade
    from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
        ShadowRuntimeEvent,
    )


class RecentEventFeed:
    def __init__(self) -> None:
        self._events: deque[RecentEvent] = deque(maxlen=MAX_RECENT_EVENTS)
        self._lock = Lock()

    def capture(
        self, trade: ShadowTrade, events: tuple[ShadowRuntimeEvent, ...]
    ) -> None:
        for source in events:
            kind = source.event_type.value
            if kind == "ENTRY_CREATED":
                event_type, reason, status = "ENTRY_CANDIDATE", "ENTRY_CREATED", None
            elif kind == "OPEN":
                event_type, reason, status = "OPEN", "ENTRY", None
            elif kind == "EXPIRED":
                event_type, reason, status = "EXPIRED", "EXPIRED", "NOT_TRIGGERED"
            elif source.stage.value == "CLOSED":
                event_type = "CLOSED"
                reason = {
                    "STOPPED": "STOP",
                    "TP2_REACHED": "TP2",
                    "TIME_EXIT": "TIME_EXIT",
                }.get(kind, kind)
                # Existing runtime outcome classification; never recompute R.
                from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
                    shadow_outcome,
                )

                status = shadow_outcome(trade).status.value
                if status == "BREAKEVEN":
                    status = "FLAT"
            else:
                continue
            identity = hashlib.sha256(
                (source.event_id + "|" + event_type).encode()
            ).hexdigest()
            record = RecentEvent(
                event_id="operator-" + identity,
                event_type=event_type,
                occurred_at=source.occurred_at,
                symbol=source.symbol[:MAX_TEXT],
                trade_id=trade.trade_id[:MAX_TEXT],
                side=trade.direction.value,
                entry_price=trade.entry_price
                if kind == "ENTRY_CREATED" or trade.entry_at is not None
                else None,
                exit_price=source.price if event_type == "CLOSED" else None,
                result_r=trade.realized_r if event_type == "CLOSED" else None,
                reason=reason,
                status=status,
            )
            with self._lock:
                if all(event.event_id != record.event_id for event in self._events):
                    self._events.append(record)

    def snapshot(self) -> tuple[RecentEvent, ...]:
        with self._lock:
            return tuple(sorted(self._events, key=lambda event: event.order_key))
