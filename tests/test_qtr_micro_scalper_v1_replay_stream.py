from __future__ import annotations

from datetime import UTC, datetime

from market_signal_assistant.qtr_micro.micro_scalper_v1.replay.stream import (
    ReplayEventType,
    merge_replay_streams,
)
from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
    PublicTradeEvent,
    TradeSide,
)


def _time(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


def _book(milliseconds: int, update_id: int) -> OrderBookEvent:
    at = _time(milliseconds)
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=(
            OrderBookEventType.SNAPSHOT
            if update_id == 1
            else OrderBookEventType.DELTA
        ),
        exchange_at=at,
        received_at=at,
        update_id=update_id,
        bids=(OrderBookLevel(100.0, 1.0),),
        asks=(OrderBookLevel(101.0, 1.0),),
        cross_sequence=update_id,
    )


def _trade(milliseconds: int, trade_id: str) -> PublicTradeEvent:
    at = _time(milliseconds)
    return PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id=trade_id,
        exchange_at=at,
        received_at=at,
        side=TradeSide.BUY,
        price=100.5,
        quantity=1.0,
        quote_notional=100.5,
    )


def test_merge_replay_streams_is_chronological() -> None:
    merged = list(
        merge_replay_streams(
            [_book(1000, 1), _book(1200, 2)],
            [_trade(1100, "t1"), _trade(1300, "t2")],
        )
    )

    assert [event.exchange_at for event in merged] == [
        _time(1000),
        _time(1100),
        _time(1200),
        _time(1300),
    ]


def test_equal_timestamp_processes_orderbook_before_trade() -> None:
    merged = list(
        merge_replay_streams(
            [_book(1000, 1)],
            [_trade(1000, "t1")],
        )
    )

    assert [event.event_type for event in merged] == [
        ReplayEventType.ORDERBOOK,
        ReplayEventType.TRADE,
    ]


def test_merge_handles_empty_orderbook_stream() -> None:
    merged = list(
        merge_replay_streams(
            [],
            [_trade(1000, "t1")],
        )
    )

    assert len(merged) == 1
    assert merged[0].event_type is ReplayEventType.TRADE


def test_merge_handles_empty_trade_stream() -> None:
    merged = list(
        merge_replay_streams(
            [_book(1000, 1)],
            [],
        )
    )

    assert len(merged) == 1
    assert merged[0].event_type is ReplayEventType.ORDERBOOK
