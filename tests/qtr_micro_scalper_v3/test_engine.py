from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper_v3.engine import (
    CashScalperConfig,
    CashScalperEngine,
)
from market_signal_assistant.qtr_micro_scalper_v3.models import (
    ImpulseDirection,
    ImpulseSnapshot,
    V3ExitReason,
    V3PriceObservation,
    V3TradeStage,
)
from qtr_micro_scalper_v3.helpers import NOW, snapshot


def test_accepts_fresh_long_and_uses_native_cash_costs() -> None:
    result = CashScalperEngine().evaluate(snapshot(), notional=1_000.0)

    assert result.accepted is True
    assert result.direction is ImpulseDirection.LONG
    assert result.cost.total_round_trip_bps == pytest.approx(15.0)
    assert result.cost.total_round_trip_pct == pytest.approx(0.15)
    assert result.cost.expected_cash == pytest.approx(1.5)
    assert result.entry_price is not None
    assert result.target_price == pytest.approx(result.entry_price * 1.0035)
    assert result.stop_price == pytest.approx(result.entry_price * 0.998)


def test_accepts_fresh_short_with_flow_and_price_response() -> None:
    result = CashScalperEngine().evaluate(
        snapshot(ImpulseDirection.SHORT), notional=1_000.0
    )

    assert result.accepted is True
    assert result.direction is ImpulseDirection.SHORT
    assert result.entry_price is not None
    assert result.target_price is not None
    assert result.stop_price is not None
    assert result.entry_price == pytest.approx(99.99)
    assert result.target_price < result.entry_price < result.stop_price


@pytest.mark.parametrize(
    ("candidate", "reason"),
    [
        (replace(snapshot(), spread_bps=9.0), "spread_too_wide"),
        (
            replace(snapshot(), bid_depth_10bps=10_000.0),
            "insufficient_bid_liquidity",
        ),
        (
            replace(snapshot(), price_displacement_5s_bps=-8.0),
            "insufficient_directional_displacement",
        ),
        (
            replace(snapshot(), absorption_detected=True),
            "absorption_or_exhaustion",
        ),
        (
            replace(snapshot(), impulse_displacement_bps=25.0),
            "impulse_already_extended",
        ),
        (
            replace(snapshot(), trigger_progress_atr=0.9),
            "trigger_progress_too_far",
        ),
    ],
)
def test_hard_gates_reject_non_tradable_or_exhausted_inputs(
    candidate: ImpulseSnapshot, reason: str
) -> None:
    result = CashScalperEngine().evaluate(candidate)

    assert result.accepted is False
    assert reason in result.blocking_reasons


def test_stale_input_is_rejected() -> None:
    stale = replace(snapshot(), source_at=NOW - timedelta(seconds=3))
    assert "stale_market_data" in CashScalperEngine().evaluate(stale).blocking_reasons


def test_economic_gate_uses_planned_target_not_trailing_realized_range() -> None:
    candidate = replace(
        snapshot(),
        spread_bps=1.0,
        estimated_potential_bps=2.0,
        local_volatility_bps=2.0,
    )

    result = CashScalperEngine().evaluate(candidate)

    assert result.cost.total_round_trip_bps == pytest.approx(14.0)
    assert result.accepted is True
    assert "insufficient_net_potential" not in result.blocking_reasons


def test_economic_gate_blocks_when_planned_target_cannot_cover_costs() -> None:
    engine = CashScalperEngine(CashScalperConfig(target_bps=20.0))

    result = engine.evaluate(snapshot())

    assert result.accepted is False
    assert "insufficient_net_potential" in result.blocking_reasons


def test_large_notional_response_metric_is_telemetry_only() -> None:
    candidate = replace(snapshot(), price_response_bps_per_10k=0.000_001)

    result = CashScalperEngine().evaluate(candidate)

    assert result.accepted is True
    assert "flow_price_not_aligned" not in result.blocking_reasons
    assert "insufficient_directional_displacement" not in result.blocking_reasons


@pytest.mark.parametrize("displacement", (-1.0, 2.99))
def test_wrong_or_insufficient_directional_displacement_blocks(
    displacement: float,
) -> None:
    result = CashScalperEngine().evaluate(
        replace(snapshot(), price_displacement_5s_bps=displacement)
    )

    assert result.accepted is False
    assert "insufficient_directional_displacement" in result.blocking_reasons


def test_flow_acceleration_remains_an_independent_gate() -> None:
    result = CashScalperEngine().evaluate(replace(snapshot(), flow_acceleration=0.5))

    assert result.accepted is False
    assert "flow_not_accelerating" in result.blocking_reasons


def test_same_impulse_cannot_reenter_after_exit() -> None:
    engine = CashScalperEngine()
    decision = engine.evaluate(snapshot())
    trade = engine.open_shadow_trade(decision)
    closed = engine.manage(
        trade,
        V3PriceObservation("BTCUSDT", NOW + timedelta(seconds=10), 100.50),
    )
    assert closed.trade.stage is V3TradeStage.CLOSED

    engine.remember_terminal(closed.trade)
    repeated = engine.evaluate(
        replace(snapshot(), observed_at=NOW + timedelta(seconds=11))
    )
    assert repeated.accepted is False
    assert "impulse_already_traded" in repeated.blocking_reasons


def test_cash_target_stop_time_and_directional_failure_are_independent_of_v2() -> None:
    engine = CashScalperEngine()
    trade = engine.open_shadow_trade(engine.evaluate(snapshot()))

    target = engine.manage(
        trade,
        V3PriceObservation("BTCUSDT", NOW + timedelta(seconds=10), 100.50),
    )
    assert target.trade.exit_reason is V3ExitReason.CASH_TARGET
    assert target.trade.net_return_pct > 0

    stopped = engine.manage(
        trade,
        V3PriceObservation("BTCUSDT", NOW + timedelta(seconds=10), 99.70),
    )
    assert stopped.trade.exit_reason is V3ExitReason.CASH_STOP

    timed = engine.manage(
        trade,
        V3PriceObservation("BTCUSDT", NOW + timedelta(seconds=91), 100.05),
    )
    assert timed.trade.exit_reason is V3ExitReason.TIME_STOP

    failed = engine.manage(
        trade,
        V3PriceObservation(
            "BTCUSDT",
            NOW + timedelta(seconds=5),
            100.02,
            directional_failure=True,
        ),
    )
    assert failed.trade.exit_reason is V3ExitReason.DIRECTIONAL_FAILURE


def test_optional_runner_starts_only_after_primary_target_covers_costs() -> None:
    engine = CashScalperEngine(CashScalperConfig(runner_fraction=0.2))
    trade = engine.open_shadow_trade(engine.evaluate(snapshot()))
    update = engine.manage(
        trade,
        V3PriceObservation("BTCUSDT", NOW + timedelta(seconds=10), 100.40),
    )

    assert update.trade.stage is V3TradeStage.RUNNER
    assert update.trade.primary_exit_at is not None
    assert update.trade.runner_stop_price > update.trade.entry_price
