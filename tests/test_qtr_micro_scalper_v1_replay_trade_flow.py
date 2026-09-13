from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro.micro_scalper_v1.replay.trade_flow import (
    ReplayTradeFlowAccumulator,
)
from market_signal_assistant.qtr_micro_scalper.data.models import (
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import (
    TradeFlowAccumulator,
)


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def make_trade(
    *,
    trade_id: str,
    at: datetime,
    side: TradeSide,
    notional: float,
) -> PublicTradeEvent:
    return PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id=trade_id,
        side=side,
        price=100.0,
        quantity=notional / 100.0,
        quote_notional=notional,
        exchange_at=at,
        received_at=at,
        is_block_trade=False,
        is_rpi_trade=False,
    )


def test_replay_trade_flow_matches_production_accumulator() -> None:
    start = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
    clock = Clock(start)

    production = TradeFlowAccumulator(
        clock=clock,
        prune_on_ingest=False,
    )
    replay = ReplayTradeFlowAccumulator(clock=clock)

    events = [
        make_trade(
            trade_id="t1",
            at=start,
            side=TradeSide.BUY,
            notional=100.0,
        ),
        make_trade(
            trade_id="t2",
            at=start + timedelta(milliseconds=500),
            side=TradeSide.SELL,
            notional=40.0,
        ),
        make_trade(
            trade_id="t3",
            at=start + timedelta(seconds=2),
            side=TradeSide.BUY,
            notional=250.0,
        ),
        make_trade(
            trade_id="t4",
            at=start + timedelta(seconds=6),
            side=TradeSide.SELL,
            notional=80.0,
        ),
        make_trade(
            trade_id="t5",
            at=start + timedelta(seconds=16),
            side=TradeSide.BUY,
            notional=300.0,
        ),
        make_trade(
            trade_id="t6",
            at=start + timedelta(seconds=61),
            side=TradeSide.SELL,
            notional=120.0,
        ),
    ]

    checkpoints = {
        start,
        start + timedelta(seconds=1),
        start + timedelta(seconds=5),
        start + timedelta(seconds=15),
        start + timedelta(seconds=60),
        start + timedelta(seconds=61),
    }

    for event in events:
        clock.now = event.exchange_at
        production.ingest(event)
        replay.ingest(event)

        if event.exchange_at in checkpoints:
            expected = production.metrics("BTCUSDT", as_of=clock.now)
            actual = replay.metrics("BTCUSDT", as_of=clock.now)

            assert actual.symbol == expected.symbol
            assert actual.as_of == expected.as_of
            assert actual.buy_notional_1s == pytest.approx(
                expected.buy_notional_1s
            )
            assert actual.sell_notional_1s == pytest.approx(
                expected.sell_notional_1s
            )
            assert actual.delta_1s == pytest.approx(expected.delta_1s)
            assert actual.delta_5s == pytest.approx(expected.delta_5s)
            assert actual.delta_15s == pytest.approx(expected.delta_15s)
            assert actual.delta_60s == pytest.approx(expected.delta_60s)
            assert actual.cvd_process == pytest.approx(expected.cvd_process)
            assert actual.cvd_utc_day == pytest.approx(expected.cvd_utc_day)
            assert actual.trade_count_5s == expected.trade_count_5s
            assert actual.largest_trade_5s == pytest.approx(
                expected.largest_trade_5s
            )
            assert actual.block_delta_60s == pytest.approx(
                expected.block_delta_60s
            )
            assert actual.rpi_delta_60s == pytest.approx(
                expected.rpi_delta_60s
            )
            assert actual.last_trade_at == expected.last_trade_at
