from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    AbsorptionDetection,
    BookSide,
    FlowSide,
    LiquidityBookFrame,
    LiquidityIntelligence,
    LiquidityWall,
    PressureDirection,
    PressureMetrics,
    SweepDetection,
    SweepDirection,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    MarketBias,
    MarketState,
    MarketStateEngine,
    MarketStateEngineConfig,
    simulate_market_state,
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


def book_frame(
    *,
    symbol: str = "BTCUSDT",
    exchange_at: datetime = NOW,
    as_of: datetime = NOW,
    ready: bool = True,
) -> LiquidityBookFrame:
    state = OrderBookState(symbol)
    bids = (level(99.99, 10.0), level(99.98, 8.0)) if ready else ()
    asks = (level(100.01, 10.0), level(100.02, 8.0)) if ready else ()
    state.process(
        OrderBookEvent(
            symbol=symbol,
            event_type=OrderBookEventType.SNAPSHOT,
            exchange_at=exchange_at,
            received_at=exchange_at,
            update_id=1,
            bids=bids,
            asks=asks,
        )
    )
    return LiquidityBookFrame.from_state(state, as_of=as_of)


def flow(
    *,
    symbol: str = "BTCUSDT",
    delta: float = 0.0,
    last_trade_at: datetime | None = NOW,
    as_of: datetime = NOW,
) -> TradeFlowMetrics:
    return TradeFlowMetrics(
        symbol=symbol,
        as_of=as_of,
        buy_notional_1s=max(delta, 0.0),
        sell_notional_1s=max(-delta, 0.0),
        delta_1s=delta,
        delta_5s=delta,
        delta_15s=delta,
        delta_60s=delta,
        cvd_process=delta,
        cvd_utc_day=delta,
        cvd_episode=delta,
        trade_count_5s=1 if last_trade_at is not None else 0,
        largest_trade_5s=abs(delta),
        block_delta_60s=0.0,
        rpi_delta_60s=0.0,
        last_trade_at=last_trade_at,
    )


def wall(side: BookSide, strength: float = 4.0) -> LiquidityWall:
    return LiquidityWall(
        side=side,
        price=99.98 if side is BookSide.BID else 100.02,
        quote_notional=10_000.0,
        strength_ratio=strength,
        distance_bps=2.0,
    )


def intelligence(
    *,
    symbol: str = "BTCUSDT",
    pressure_direction: PressureDirection = PressureDirection.NEUTRAL,
    combined_pressure: float = 0.0,
    pressure_confidence: float = 0.0,
    sweep_direction: SweepDirection = SweepDirection.NONE,
    sweep_score: float = 0.0,
    absorption_side: FlowSide = FlowSide.NONE,
    absorption_score: float = 0.0,
    bid_walls: tuple[LiquidityWall, ...] = (),
    ask_walls: tuple[LiquidityWall, ...] = (),
) -> LiquidityIntelligence:
    return LiquidityIntelligence(
        symbol=symbol,
        bid_walls=bid_walls,
        ask_walls=ask_walls,
        absorption=AbsorptionDetection(
            detected=absorption_side is not FlowSide.NONE,
            aggressive_side=absorption_side,
            score=absorption_score,
            aggressive_notional=200.0,
            aggressive_flow_ratio=2.0,
            favorable_price_move_bps=0.5,
            opposing_depth_retention=1.0,
            reasons=("Встречная ликвидность удержала поток.",),
        ),
        sweep=SweepDetection(
            detected=sweep_direction is not SweepDirection.NONE,
            direction=sweep_direction,
            score=sweep_score,
            aggressive_notional=300.0,
            aggressive_flow_ratio=3.0,
            levels_consumed=3,
            swept_notional=1_000.0,
            price_displacement_bps=3.0,
            depth_depletion=0.5,
            reasons=("Поток поглотил уровни книги.",),
        ),
        pressure=PressureMetrics(
            book_pressure=combined_pressure,
            trade_pressure=combined_pressure,
            depth_pressure=combined_pressure,
            combined_pressure=combined_pressure,
            direction=pressure_direction,
            confidence=pressure_confidence,
            reasons=("Метрики давления согласованы.",),
        ),
    )


def assess(
    liquidity: LiquidityIntelligence,
    *,
    current: LiquidityBookFrame | None = None,
    trade_flow: TradeFlowMetrics | None = None,
) -> object:
    return MarketStateEngine().assess(
        current or book_frame(),
        trade_flow or flow(delta=liquidity.pressure.combined_pressure * 100),
        liquidity,
        assessed_at=NOW,
    )


@pytest.mark.parametrize(
    ("direction", "state", "bias", "score_sign"),
    [
        (SweepDirection.UP, MarketState.UPWARD_SWEEP, MarketBias.BULLISH, 1),
        (SweepDirection.DOWN, MarketState.DOWNWARD_SWEEP, MarketBias.BEARISH, -1),
    ],
)
def test_sweep_states(
    direction: SweepDirection,
    state: MarketState,
    bias: MarketBias,
    score_sign: int,
) -> None:
    result = assess(
        intelligence(
            sweep_direction=direction,
            sweep_score=80.0,
            pressure_confidence=60.0,
        )
    )
    assert result.state is state  # type: ignore[attr-defined]
    assert result.bias is bias  # type: ignore[attr-defined]
    assert result.directional_score * score_sign > 0  # type: ignore[attr-defined]
    assert result.reasons  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("side", "state", "bias", "score_sign"),
    [
        (FlowSide.BUY, MarketState.BUY_FLOW_ABSORBED, MarketBias.BEARISH, -1),
        (FlowSide.SELL, MarketState.SELL_FLOW_ABSORBED, MarketBias.BULLISH, 1),
    ],
)
def test_absorption_states_reverse_aggressive_flow_bias(
    side: FlowSide,
    state: MarketState,
    bias: MarketBias,
    score_sign: int,
) -> None:
    result = assess(intelligence(absorption_side=side, absorption_score=75.0))
    assert result.state is state  # type: ignore[attr-defined]
    assert result.bias is bias  # type: ignore[attr-defined]
    assert result.directional_score * score_sign > 0  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("direction", "pressure", "state", "bias"),
    [
        (PressureDirection.BUY, 0.6, MarketState.BUY_PRESSURE, MarketBias.BULLISH),
        (
            PressureDirection.SELL,
            -0.6,
            MarketState.SELL_PRESSURE,
            MarketBias.BEARISH,
        ),
    ],
)
def test_directional_pressure_states(
    direction: PressureDirection,
    pressure: float,
    state: MarketState,
    bias: MarketBias,
) -> None:
    result = assess(
        intelligence(
            pressure_direction=direction,
            combined_pressure=pressure,
            pressure_confidence=60.0,
        )
    )
    assert result.state is state  # type: ignore[attr-defined]
    assert result.bias is bias  # type: ignore[attr-defined]


def test_sweep_has_priority_over_absorption_and_pressure() -> None:
    result = assess(
        intelligence(
            sweep_direction=SweepDirection.UP,
            sweep_score=80.0,
            absorption_side=FlowSide.BUY,
            absorption_score=90.0,
            pressure_direction=PressureDirection.SELL,
            combined_pressure=-0.8,
            pressure_confidence=80.0,
        )
    )
    assert result.state is MarketState.UPWARD_SWEEP  # type: ignore[attr-defined]


def test_two_sided_walls_form_neutral_liquidity_state() -> None:
    result = assess(
        intelligence(
            bid_walls=(wall(BookSide.BID),),
            ask_walls=(wall(BookSide.ASK),),
        )
    )
    assert result.state is MarketState.TWO_SIDED_LIQUIDITY  # type: ignore[attr-defined]
    assert result.bias is MarketBias.NEUTRAL  # type: ignore[attr-defined]
    assert result.confirmations  # type: ignore[attr-defined]


def test_balanced_state_when_no_direction_is_confirmed() -> None:
    result = assess(intelligence())
    assert result.state is MarketState.BALANCED  # type: ignore[attr-defined]
    assert result.ready is True  # type: ignore[attr-defined]
    assert "не подтверждено" in result.reasons[0]  # type: ignore[attr-defined]


def test_unready_order_book_blocks_classification_with_reasons() -> None:
    result = assess(intelligence(), current=book_frame(ready=False))
    assert result.state is MarketState.NOT_READY  # type: ignore[attr-defined]
    assert result.bias is MarketBias.UNKNOWN  # type: ignore[attr-defined]
    assert result.ready is False  # type: ignore[attr-defined]
    assert "Order book не готов" in " ".join(result.reasons)  # type: ignore[attr-defined]


def test_stale_book_and_trade_are_reported_separately() -> None:
    stale_at = NOW - timedelta(seconds=2)
    result = assess(
        intelligence(),
        current=book_frame(exchange_at=stale_at, as_of=stale_at),
        trade_flow=flow(last_trade_at=stale_at, as_of=stale_at),
    )
    assert result.state is MarketState.NOT_READY  # type: ignore[attr-defined]
    assert "Order book устарел." in result.reasons  # type: ignore[attr-defined]
    assert "Public trades устарели." in result.reasons  # type: ignore[attr-defined]


def test_missing_trade_history_is_not_ready() -> None:
    result = assess(intelligence(), trade_flow=flow(last_trade_at=None))
    assert result.state is MarketState.NOT_READY  # type: ignore[attr-defined]
    assert "Отсутствует история public trades." in result.reasons  # type: ignore[attr-defined]


def test_opposing_wall_is_exposed_as_warning() -> None:
    result = assess(
        intelligence(
            pressure_direction=PressureDirection.BUY,
            combined_pressure=0.6,
            pressure_confidence=60.0,
            ask_walls=(wall(BookSide.ASK),),
        )
    )
    assert result.state is MarketState.BUY_PRESSURE  # type: ignore[attr-defined]
    assert result.warnings == (  # type: ignore[attr-defined]
        "Сильная ask wall ограничивает bullish-сценарий.",
    )


def test_combined_metrics_keep_each_source_separate() -> None:
    result = assess(
        intelligence(
            pressure_direction=PressureDirection.BUY,
            combined_pressure=0.4,
            pressure_confidence=40.0,
            bid_walls=(wall(BookSide.BID, 5.0),),
        ),
        trade_flow=flow(delta=125.0),
    )
    assert result.metrics.delta_5s == 125.0  # type: ignore[attr-defined]
    assert result.metrics.imbalance_l5 is not None  # type: ignore[attr-defined]
    assert result.metrics.combined_pressure == 0.4  # type: ignore[attr-defined]
    assert result.metrics.strongest_bid_wall_ratio == 5.0  # type: ignore[attr-defined]


def test_offline_entry_point_matches_direct_engine() -> None:
    current = book_frame()
    trades = flow(delta=50.0)
    liquidity = intelligence(
        pressure_direction=PressureDirection.BUY,
        combined_pressure=0.5,
        pressure_confidence=50.0,
    )
    direct = MarketStateEngine().assess(
        current,
        trades,
        liquidity,
        assessed_at=NOW,
    )
    offline = simulate_market_state(
        current,
        trades,
        liquidity,
        assessed_at=NOW,
    )
    assert offline == direct


def test_assessment_is_immutable() -> None:
    result = assess(intelligence())
    with pytest.raises(FrozenInstanceError):
        result.ready = False  # type: ignore[attr-defined]


def test_mismatched_symbol_and_future_source_are_rejected() -> None:
    current = book_frame()
    with pytest.raises(ValueError, match="same symbol"):
        MarketStateEngine().assess(
            current,
            flow(symbol="ETHUSDT"),
            intelligence(),
            assessed_at=NOW,
        )
    future_flow = replace(flow(), as_of=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="newer than assessed_at"):
        MarketStateEngine().assess(
            current,
            future_flow,
            intelligence(),
            assessed_at=NOW,
        )


def test_engine_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        MarketStateEngineConfig(max_book_age_ms=0.0)
    with pytest.raises(ValueError, match="between 0 and 1"):
        MarketStateEngineConfig(pressure_state_threshold=1.1)
