from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper.data.models import (
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import (
    IngestStatus,
    TradeFlowAccumulator,
    simulate_trade_flow,
)

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)


class MutableClock:
    def __init__(self, current: datetime = NOW) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current


def event(
    trade_id: str,
    *,
    seconds_ago: float = 0.0,
    side: TradeSide = TradeSide.BUY,
    notional: float = 100.0,
    symbol: str = "BTCUSDT",
    sequence: int | None = None,
    block: bool = False,
    rpi: bool = False,
) -> PublicTradeEvent:
    exchange_at = NOW - timedelta(seconds=seconds_ago)
    return PublicTradeEvent(
        symbol=symbol,
        trade_id=trade_id,
        exchange_at=exchange_at,
        received_at=exchange_at + timedelta(milliseconds=5),
        side=side,
        price=notional,
        quantity=1.0,
        quote_notional=notional,
        sequence=sequence,
        is_block_trade=block,
        is_rpi_trade=rpi,
    )


def test_buy_and_sell_create_signed_delta_and_one_second_flow() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    accumulator.ingest(event("buy", side=TradeSide.BUY, notional=150.0))
    accumulator.ingest(event("sell", side=TradeSide.SELL, notional=40.0))

    metrics = accumulator.metrics("btcusdt", as_of=NOW)
    assert metrics.buy_notional_1s == 150.0
    assert metrics.sell_notional_1s == 40.0
    assert metrics.delta_1s == 110.0
    assert metrics.delta_60s == 110.0
    assert metrics.cvd_process == 110.0


def test_rolling_windows_include_exact_boundary() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    for trade in (
        event("at-1", seconds_ago=1, notional=1.0),
        event("at-5", seconds_ago=5, notional=5.0),
        event("at-15", seconds_ago=15, notional=15.0),
        event("at-60", seconds_ago=60, notional=60.0),
        event("outside", seconds_ago=60.001, notional=1_000.0),
    ):
        accumulator.ingest(trade)

    metrics = accumulator.metrics("BTCUSDT", as_of=NOW)
    assert metrics.delta_1s == 1.0
    assert metrics.delta_5s == 6.0
    assert metrics.delta_15s == 21.0
    assert metrics.delta_60s == 81.0


def test_duplicate_trade_is_suppressed_without_changing_cvd() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    first = accumulator.ingest(event("same", notional=100.0))
    duplicate = accumulator.ingest(event("same", notional=100.0))

    assert first.status is IngestStatus.ACCEPTED
    assert duplicate.status is IngestStatus.DUPLICATE
    assert accumulator.metrics("BTCUSDT", as_of=NOW).cvd_process == 100.0


def test_sequence_is_provenance_not_trade_identity() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    accumulator.ingest(event("one", sequence=7, notional=100.0))
    second = accumulator.ingest(event("two", sequence=7, notional=50.0))

    assert second.status is IngestStatus.ACCEPTED
    assert accumulator.metrics("BTCUSDT", as_of=NOW).cvd_process == 150.0


def test_block_flow_is_separate_from_primary_delta_and_cvd() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    accumulator.ingest(event("ordinary", notional=100.0))
    accumulator.ingest(event("block", notional=500.0, block=True))

    metrics = accumulator.metrics("BTCUSDT", as_of=NOW)
    assert metrics.delta_60s == 100.0
    assert metrics.cvd_process == 100.0
    assert metrics.block_delta_60s == 500.0
    assert metrics.trade_count_5s == 1


def test_rpi_flow_remains_in_primary_and_is_also_observable() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    accumulator.ingest(
        event(
            "rpi-sell",
            side=TradeSide.SELL,
            notional=75.0,
            rpi=True,
        )
    )

    metrics = accumulator.metrics("BTCUSDT", as_of=NOW)
    assert metrics.delta_60s == -75.0
    assert metrics.rpi_delta_60s == -75.0
    assert metrics.cvd_process == -75.0


def test_process_cvd_does_not_decay_when_rolling_event_is_pruned() -> None:
    clock = MutableClock()
    accumulator = TradeFlowAccumulator(clock=clock)
    accumulator.ingest(event("old", notional=125.0))
    clock.current = NOW + timedelta(seconds=80)

    metrics = accumulator.metrics("BTCUSDT", as_of=clock.current)
    assert metrics.delta_60s == 0.0
    assert metrics.cvd_process == 125.0
    assert accumulator.event_count("BTCUSDT") == 0


def test_utc_day_cvd_uses_trade_exchange_day() -> None:
    after_midnight = datetime(2026, 8, 16, 0, 0, 10, tzinfo=UTC)
    clock = MutableClock(after_midnight)
    accumulator = TradeFlowAccumulator(
        retention=timedelta(seconds=180),
        clock=clock,
    )
    before_at = after_midnight - timedelta(seconds=20)
    before = PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id="before",
        exchange_at=before_at,
        received_at=before_at,
        side=TradeSide.BUY,
        price=100.0,
        quantity=1.0,
        quote_notional=100.0,
    )
    after = PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id="after",
        exchange_at=after_midnight,
        received_at=after_midnight,
        side=TradeSide.SELL,
        price=25.0,
        quantity=1.0,
        quote_notional=25.0,
    )
    accumulator.ingest(before)
    accumulator.ingest(after)

    metrics = accumulator.metrics("BTCUSDT", as_of=after_midnight)
    assert metrics.cvd_process == 75.0
    assert metrics.cvd_utc_day == -25.0


def test_episode_cvd_starts_at_explicit_boundary() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    accumulator.ingest(event("before", seconds_ago=10, notional=100.0))
    accumulator.ingest(event("inside", seconds_ago=2, notional=40.0))
    accumulator.start_episode("BTCUSDT", at=NOW - timedelta(seconds=5))

    assert accumulator.metrics("BTCUSDT", as_of=NOW).cvd_episode == 40.0

    accumulator.ingest(
        event(
            "new-sell",
            side=TradeSide.SELL,
            notional=10.0,
        )
    )
    assert accumulator.metrics("BTCUSDT", as_of=NOW).cvd_episode == 30.0


def test_episode_cvd_is_none_until_episode_is_started() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    accumulator.ingest(event("one"))
    assert accumulator.metrics("BTCUSDT", as_of=NOW).cvd_episode is None


def test_event_older_than_retention_is_late_and_does_not_change_cvd() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    result = accumulator.ingest(event("late", seconds_ago=76))

    assert result.status is IngestStatus.LATE
    assert accumulator.metrics("BTCUSDT", as_of=NOW).cvd_process == 0.0
    assert accumulator.event_count("BTCUSDT") == 0


def test_symbols_are_isolated() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    accumulator.ingest(event("btc", notional=100.0))
    accumulator.ingest(event("eth", symbol="ETHUSDT", notional=20.0))

    assert accumulator.metrics("BTCUSDT", as_of=NOW).delta_60s == 100.0
    assert accumulator.metrics("ETHUSDT", as_of=NOW).delta_60s == 20.0


def test_metrics_are_immutable() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    metrics = accumulator.metrics("BTCUSDT", as_of=NOW)
    with pytest.raises(FrozenInstanceError):
        metrics.delta_60s = 1.0  # type: ignore[misc]


def test_offline_simulation_is_deterministic_and_reports_ingest_outcomes() -> None:
    trades = (
        event("sell", side=TradeSide.SELL, notional=25.0),
        event("buy", notional=100.0),
        event("buy", notional=100.0),
        event("late", seconds_ago=76, notional=1_000.0),
    )

    first = simulate_trade_flow(trades, symbol="BTCUSDT", as_of=NOW)
    second = simulate_trade_flow(reversed(trades), symbol="BTCUSDT", as_of=NOW)

    assert first == second
    assert first.metrics.delta_60s == 75.0
    assert first.metrics.cvd_process == 75.0
    assert first.accepted_events == 2
    assert first.duplicate_events == 1
    assert first.late_events == 1


def test_offline_simulation_can_start_episode_without_network() -> None:
    simulation = simulate_trade_flow(
        (
            event("before", seconds_ago=10, notional=100.0),
            event("inside", seconds_ago=1, notional=30.0),
        ),
        symbol="BTCUSDT",
        as_of=NOW,
        episode_started_at=NOW - timedelta(seconds=5),
    )
    assert simulation.metrics.cvd_process == 130.0
    assert simulation.metrics.cvd_episode == 30.0


def test_collector_rejects_naive_time_and_non_trade_input() -> None:
    accumulator = TradeFlowAccumulator(clock=MutableClock())
    with pytest.raises(ValueError, match="timezone-aware"):
        accumulator.metrics("BTCUSDT", as_of=datetime(2026, 8, 15, 12))
    with pytest.raises(TypeError, match="PublicTradeEvent"):
        accumulator.ingest(object())  # type: ignore[arg-type]
