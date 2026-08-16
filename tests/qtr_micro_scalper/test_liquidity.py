from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    BookSide,
    FlowSide,
    LiquidityBookFrame,
    LiquidityIntelligenceConfig,
    PressureDirection,
    SweepDirection,
    calculate_pressure,
    detect_absorption,
    detect_liquidity_walls,
    detect_sweep,
    simulate_liquidity_intelligence,
)
from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookState
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def level(price: float, quantity: float) -> OrderBookLevel:
    return OrderBookLevel(price=price, quantity=quantity)


def frame(
    *,
    bids: tuple[OrderBookLevel, ...],
    asks: tuple[OrderBookLevel, ...],
    at: datetime = NOW,
    symbol: str = "BTCUSDT",
) -> LiquidityBookFrame:
    state = OrderBookState(symbol)
    state.process(
        OrderBookEvent(
            symbol=symbol,
            event_type=OrderBookEventType.SNAPSHOT,
            exchange_at=at,
            received_at=at,
            update_id=1,
            bids=bids,
            asks=asks,
        )
    )
    return LiquidityBookFrame.from_state(state, as_of=at)


def trade_flow(
    *,
    delta_1s: float,
    delta_5s: float,
    symbol: str = "BTCUSDT",
) -> TradeFlowMetrics:
    return TradeFlowMetrics(
        symbol=symbol,
        as_of=NOW,
        buy_notional_1s=max(delta_1s, 0.0),
        sell_notional_1s=max(-delta_1s, 0.0),
        delta_1s=delta_1s,
        delta_5s=delta_5s,
        delta_15s=delta_5s,
        delta_60s=delta_5s,
        cvd_process=delta_5s,
        cvd_utc_day=delta_5s,
        cvd_episode=delta_5s,
        trade_count_5s=5,
        largest_trade_5s=abs(delta_5s),
        block_delta_60s=0.0,
        rpi_delta_60s=0.0,
        last_trade_at=NOW,
    )


def balanced_frame(*, at: datetime = NOW) -> LiquidityBookFrame:
    return frame(
        at=at,
        bids=(level(99.99, 10.0), level(99.98, 10.0), level(99.97, 10.0)),
        asks=(level(100.01, 10.0), level(100.02, 10.0), level(100.03, 10.0)),
    )


def test_wall_detector_uses_relative_level_notional() -> None:
    current = frame(
        bids=(level(99.99, 1.0), level(99.98, 1.0), level(99.97, 10.0)),
        asks=(level(100.01, 1.0), level(100.02, 1.0), level(100.03, 1.0)),
    )

    walls = detect_liquidity_walls(current)
    assert len(walls) == 1
    assert walls[0].side is BookSide.BID
    assert walls[0].price == 99.97
    assert walls[0].strength_ratio == pytest.approx(9.998)


def test_wall_detector_ignores_far_levels_and_small_samples() -> None:
    current = frame(
        bids=(level(99.99, 1.0), level(99.0, 100.0)),
        asks=(level(100.01, 1.0), level(101.0, 100.0)),
    )
    assert detect_liquidity_walls(current) == ()


def test_buy_absorption_requires_flow_small_move_and_retained_ask_depth() -> None:
    previous = balanced_frame()
    current = frame(
        at=NOW + timedelta(seconds=1),
        bids=(level(99.995, 10.0), level(99.985, 10.0), level(99.975, 10.0)),
        asks=(level(100.015, 10.0), level(100.025, 10.0), level(100.035, 10.0)),
    )
    result = detect_absorption(
        previous,
        current,
        trade_flow(delta_1s=100.0, delta_5s=200.0),
        aggressive_notional_baseline=100.0,
    )

    assert result.detected is True
    assert result.aggressive_side is FlowSide.BUY
    assert result.favorable_price_move_bps == pytest.approx(0.5)
    assert result.opposing_depth_retention is not None
    assert result.opposing_depth_retention >= 0.8


def test_sell_absorption_is_symmetric() -> None:
    previous = balanced_frame()
    current = frame(
        at=NOW + timedelta(seconds=1),
        bids=(level(99.985, 10.0), level(99.975, 10.0), level(99.965, 10.0)),
        asks=(level(100.005, 10.0), level(100.015, 10.0), level(100.025, 10.0)),
    )
    result = detect_absorption(
        previous,
        current,
        trade_flow(delta_1s=-100.0, delta_5s=-200.0),
        aggressive_notional_baseline=100.0,
    )
    assert result.detected is True
    assert result.aggressive_side is FlowSide.SELL


def test_absorption_rejects_real_price_displacement() -> None:
    previous = balanced_frame()
    current = frame(
        at=NOW + timedelta(seconds=1),
        bids=(level(100.09, 10.0),),
        asks=(level(100.11, 10.0),),
    )
    result = detect_absorption(
        previous,
        current,
        trade_flow(delta_1s=100.0, delta_5s=200.0),
        aggressive_notional_baseline=100.0,
    )
    assert result.detected is False
    assert "Цена сдвинулась" in " ".join(result.reasons)


def test_upward_sweep_requires_levels_move_flow_and_depletion() -> None:
    previous = balanced_frame()
    current = frame(
        at=NOW + timedelta(seconds=1),
        bids=(level(100.02, 2.0), level(100.01, 2.0)),
        asks=(level(100.04, 2.0), level(100.05, 2.0)),
    )
    result = detect_sweep(
        previous,
        current,
        trade_flow(delta_1s=150.0, delta_5s=300.0),
        aggressive_notional_baseline=100.0,
    )

    assert result.detected is True
    assert result.direction is SweepDirection.UP
    assert result.levels_consumed == 3
    assert result.swept_notional == pytest.approx(3_000.6)
    assert result.price_displacement_bps == pytest.approx(3.0)
    assert result.depth_depletion is not None and result.depth_depletion > 0


def test_downward_sweep_is_symmetric() -> None:
    previous = balanced_frame()
    current = frame(
        at=NOW + timedelta(seconds=1),
        bids=(level(99.95, 2.0), level(99.94, 2.0)),
        asks=(level(99.97, 2.0), level(99.98, 2.0)),
    )
    result = detect_sweep(
        previous,
        current,
        trade_flow(delta_1s=-150.0, delta_5s=-300.0),
        aggressive_notional_baseline=100.0,
    )
    assert result.detected is True
    assert result.direction is SweepDirection.DOWN
    assert result.levels_consumed == 3


def test_flow_without_consumed_levels_is_not_a_sweep() -> None:
    current = balanced_frame(at=NOW + timedelta(seconds=1))
    result = detect_sweep(
        balanced_frame(),
        current,
        trade_flow(delta_1s=150.0, delta_5s=300.0),
        aggressive_notional_baseline=100.0,
    )
    assert result.detected is False
    assert result.direction is SweepDirection.NONE
    assert "поглощённых уровней" in " ".join(result.reasons)


def test_pressure_combines_trade_book_and_depth_direction() -> None:
    current = frame(
        bids=(level(99.99, 30.0), level(99.98, 20.0), level(99.97, 10.0)),
        asks=(level(100.01, 2.0), level(100.02, 2.0), level(100.03, 2.0)),
    )
    pressure = calculate_pressure(
        current,
        trade_flow(delta_1s=100.0, delta_5s=200.0),
        aggressive_notional_baseline=100.0,
    )
    assert pressure.direction is PressureDirection.BUY
    assert pressure.book_pressure is not None and pressure.book_pressure > 0
    assert pressure.depth_pressure is not None and pressure.depth_pressure > 0
    assert 0 < pressure.confidence <= 100


def test_conflicting_pressure_can_remain_neutral() -> None:
    current = balanced_frame()
    pressure = calculate_pressure(
        current,
        trade_flow(delta_1s=0.0, delta_5s=0.0),
        aggressive_notional_baseline=100.0,
    )
    assert pressure.direction is PressureDirection.NEUTRAL
    assert abs(pressure.combined_pressure) < 0.001


def test_offline_layer_returns_all_explainable_components() -> None:
    previous = balanced_frame()
    current = frame(
        at=NOW + timedelta(seconds=1),
        bids=(level(100.02, 2.0), level(100.01, 2.0)),
        asks=(level(100.04, 2.0), level(100.05, 2.0)),
    )
    result = simulate_liquidity_intelligence(
        previous,
        current,
        trade_flow(delta_1s=150.0, delta_5s=300.0),
        aggressive_notional_baseline=100.0,
    )
    assert result.symbol == "BTCUSDT"
    assert result.sweep.detected is True
    assert result.absorption.detected is False
    assert result.pressure.direction is PressureDirection.BUY


def test_symbol_mismatch_and_invalid_baseline_are_rejected() -> None:
    previous = balanced_frame()
    current = balanced_frame(at=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="same symbol"):
        detect_sweep(
            previous,
            current,
            trade_flow(delta_1s=1.0, delta_5s=1.0, symbol="ETHUSDT"),
            aggressive_notional_baseline=100.0,
        )
    with pytest.raises(ValueError, match="baseline must be positive"):
        calculate_pressure(
            current,
            trade_flow(delta_1s=1.0, delta_5s=1.0),
            aggressive_notional_baseline=0.0,
        )


def test_threshold_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="thresholds must be positive"):
        LiquidityIntelligenceConfig(sweep_flow_ratio=0.0)
    with pytest.raises(ValueError, match="between 0 and 1"):
        LiquidityIntelligenceConfig(pressure_direction_threshold=1.1)
