from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    PublicTradeEvent,
)


class ReplayEventType(StrEnum):
    ORDERBOOK = "orderbook"
    TRADE = "trade"


ReplayPayload = OrderBookEvent | PublicTradeEvent


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    event_type: ReplayEventType
    exchange_at: datetime
    payload: ReplayPayload


def merge_replay_streams(
    orderbook_events: Iterable[OrderBookEvent],
    trade_events: Iterable[PublicTradeEvent],
) -> Iterator[ReplayEvent]:
    """Merge ordered historical feeds without look-ahead.

    If timestamps are equal, orderbook is processed first.
    """

    books = iter(orderbook_events)
    trades = iter(trade_events)

    book = next(books, None)
    trade = next(trades, None)

    while book is not None or trade is not None:
        if trade is None:
            assert book is not None
            yield ReplayEvent(
                event_type=ReplayEventType.ORDERBOOK,
                exchange_at=book.exchange_at,
                payload=book,
            )
            book = next(books, None)
            continue

        if book is None:
            yield ReplayEvent(
                event_type=ReplayEventType.TRADE,
                exchange_at=trade.exchange_at,
                payload=trade,
            )
            trade = next(trades, None)
            continue

        if book.exchange_at <= trade.exchange_at:
            yield ReplayEvent(
                event_type=ReplayEventType.ORDERBOOK,
                exchange_at=book.exchange_at,
                payload=book,
            )
            book = next(books, None)
        else:
            yield ReplayEvent(
                event_type=ReplayEventType.TRADE,
                exchange_at=trade.exchange_at,
                payload=trade,
            )
            trade = next(trades, None)
