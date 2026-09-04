from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from market_signal_assistant.qtr_signal_outcome.engine import OutcomeEngine
from market_signal_assistant.qtr_signal_outcome.models import (
    BarrierOrder,
    Direction,
    HorizonOutcome,
    OutcomeStatus,
    SignalOutcome,
)
from qtr_signal_outcome.helpers import candle, signal


def horizon(outcome: SignalOutcome, minutes: int) -> HorizonOutcome:
    return next(item for item in outcome.horizons if item.horizon_minutes == minutes)


def test_long_mfe_mae_and_all_checkpoints() -> None:
    candles = tuple(
        candle(
            minute,
            high=100.0 + minute * 0.1,
            low=100.0 - minute * 0.05,
            close=100.0 + minute * 0.05,
        )
        for minute in range(1, 241)
    )
    outcome = OutcomeEngine().analyze(signal(), candles)
    assert outcome.status is OutcomeStatus.COMPLETE
    assert tuple(item.horizon_minutes for item in outcome.horizons) == (
        5,
        15,
        30,
        60,
        120,
        240,
    )
    final = horizon(outcome, 240)
    assert final.mfe_price == pytest.approx(24.0)
    assert final.mae_price == pytest.approx(12.0)
    assert final.mfe_atr == pytest.approx(12.0)
    assert final.mae_atr == pytest.approx(6.0)
    assert final.directional_close_return_pct == pytest.approx(12.0)


def test_short_mfe_mae() -> None:
    candles = (candle(1, high=101.0, low=97.0, close=98.0),) + tuple(
        candle(minute, high=100.5, low=98.0, close=99.0) for minute in range(2, 6)
    )
    outcome = OutcomeEngine().analyze(
        signal(Direction.SHORT),
        candles,
    )
    first = horizon(outcome, 5)
    assert first.mfe_price == pytest.approx(3.0)
    assert first.mae_price == pytest.approx(1.0)
    assert first.mfe_atr == pytest.approx(1.5)
    assert first.mae_atr == pytest.approx(0.5)
    assert first.directional_close_return_pct == pytest.approx(1.0)


@pytest.mark.parametrize(
    ("candles", "expected"),
    [
        (
            (candle(1, high=102.1, low=99.8), candle(2, high=102.2, low=97.9)),
            BarrierOrder.FAVORABLE_FIRST,
        ),
        (
            (candle(1, high=100.2, low=97.9), candle(2, high=102.1, low=97.8)),
            BarrierOrder.ADVERSE_FIRST,
        ),
        (
            (candle(1, high=102.1, low=97.9),),
            BarrierOrder.AMBIGUOUS_SAME_CANDLE,
        ),
        ((candle(1, high=100.2, low=99.8),), BarrierOrder.NEITHER),
    ],
)
def test_barrier_order_does_not_guess_inside_candle(
    candles: tuple[Any, ...], expected: BarrierOrder
) -> None:
    outcome = OutcomeEngine().analyze(signal(), candles)
    pair = next(
        item
        for item in outcome.barrier_orders
        if item.favorable_atr == 1.0 and item.adverse_atr == -1.0
    )
    assert pair.order is expected


def test_invalidation_long_and_short() -> None:
    long = OutcomeEngine().analyze(signal(), (candle(1, low=97.9),))
    short = OutcomeEngine().analyze(signal(Direction.SHORT), (candle(1, high=102.1),))
    assert long.invalidation_hit is True
    assert short.invalidation_hit is True
    assert long.invalidation_minutes == pytest.approx(1.0)
    assert short.invalidation_minutes == pytest.approx(1.0)


def test_barriers_store_first_causal_hit_time() -> None:
    result = OutcomeEngine().analyze(
        signal(),
        (
            candle(1, high=100.5, low=99.8),
            candle(2, high=102.1, low=99.8),
        ),
    )

    plus_one = next(
        item for item in result.favorable_barriers if item.threshold_atr == 1.0
    )
    assert plus_one.hit is True
    assert plus_one.first_hit_timestamp == candle(2).closed_at
    assert plus_one.first_hit_minutes_from_signal == pytest.approx(2.0)
    assert tuple(item.threshold_atr for item in result.adverse_barriers) == (
        -0.5,
        -1.0,
        -1.5,
    )


def test_partial_then_complete_horizon() -> None:
    engine = OutcomeEngine()
    partial = engine.analyze(signal(), tuple(candle(i) for i in range(1, 10)))
    complete = engine.analyze(signal(), tuple(candle(i) for i in range(1, 241)))
    assert partial.status is OutcomeStatus.PARTIAL
    assert complete.status is OutcomeStatus.COMPLETE
    assert horizon(partial, 15).close_price is None
    assert horizon(complete, 240).close_price is not None


def test_signal_candle_that_started_before_signal_is_excluded() -> None:
    item = replace(
        signal(),
        signal_timestamp=signal().signal_timestamp.replace(second=30),
    )
    result = OutcomeEngine().analyze(
        item,
        (candle(1, high=110.0, low=90.0),)
        + tuple(candle(minute, high=100.5, low=99.5) for minute in range(2, 6)),
    )
    first = horizon(result, 5)
    assert first.mfe_price == pytest.approx(0.5)
    assert first.mae_price == pytest.approx(0.5)
