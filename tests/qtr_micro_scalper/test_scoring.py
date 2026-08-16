from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    AbsorptionDetection,
    BookSide,
    FlowSide,
    LiquidityIntelligence,
    LiquidityWall,
    PressureDirection,
    PressureMetrics,
    SweepDetection,
    SweepDirection,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    CombinedMarketMetrics,
    MarketBias,
    MarketState,
    MarketStateAssessment,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookMetrics
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics
from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperDecision,
    ScalperDirection,
    ScalperScore,
    ScalperScoringEngine,
    simulate_scalper_score,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    RiskContext,
    RiskLevel,
    ShadowDirection,
    ShadowOpportunity,
    ShadowOpportunityDecision,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def trade_flow(sign: float = 1.0) -> TradeFlowMetrics:
    return TradeFlowMetrics(
        symbol="BTCUSDT",
        as_of=NOW,
        buy_notional_1s=2_000.0 if sign > 0 else 500.0,
        sell_notional_1s=500.0 if sign > 0 else 2_000.0,
        delta_1s=100.0 * sign,
        delta_5s=300.0 * sign,
        delta_15s=600.0 * sign,
        delta_60s=1_000.0 * sign,
        cvd_process=2_000.0 * sign,
        cvd_utc_day=3_000.0 * sign,
        cvd_episode=1_500.0 * sign,
        trade_count_5s=20,
        largest_trade_5s=500.0,
        block_delta_60s=0.0,
        rpi_delta_60s=0.0,
        last_trade_at=NOW - timedelta(milliseconds=10),
    )


def orderbook(
    sign: float = 1.0,
    *,
    ready: bool = True,
    spread_bps: float | None = 1.0,
    book_age_ms: float | None = 10.0,
) -> OrderBookMetrics:
    bid_depth = 20_000.0 if sign > 0 else 5_000.0
    ask_depth = 5_000.0 if sign > 0 else 20_000.0
    return OrderBookMetrics(
        symbol="BTCUSDT",
        as_of=NOW,
        book_exchange_at=NOW - timedelta(milliseconds=10),
        book_age_ms=book_age_ms,
        update_id=10,
        cross_sequence=10,
        bid_levels=10,
        ask_levels=10,
        best_bid=99.99,
        best_ask=100.01,
        mid_price=100.0,
        microprice=100.0,
        spread_bps=spread_bps,
        bid_depth_5bps=bid_depth / 2,
        ask_depth_5bps=ask_depth / 2,
        bid_depth_10bps=bid_depth,
        ask_depth_10bps=ask_depth,
        bid_depth_25bps=bid_depth * 2,
        ask_depth_25bps=ask_depth * 2,
        imbalance_l1=0.8 * sign,
        imbalance_l5=0.8 * sign,
        imbalance_l10=0.7 * sign,
        imbalance_l25=0.6 * sign,
        imbalance_l50=0.5 * sign,
        ready=ready,
        health_reasons=() if ready else ("snapshot_not_ready",),
    )


def liquidity(sign: float = 1.0) -> LiquidityIntelligence:
    long = sign > 0
    support_side = BookSide.BID if long else BookSide.ASK
    return LiquidityIntelligence(
        symbol="BTCUSDT",
        bid_walls=(
            LiquidityWall(BookSide.BID, 99.8, 20_000.0, 5.0, 20.0),
        )
        if support_side is BookSide.BID
        else (),
        ask_walls=(
            LiquidityWall(BookSide.ASK, 100.2, 20_000.0, 5.0, 20.0),
        )
        if support_side is BookSide.ASK
        else (),
        absorption=AbsorptionDetection(
            detected=False,
            aggressive_side=FlowSide.NONE,
            score=0.0,
            aggressive_notional=0.0,
            aggressive_flow_ratio=0.0,
            favorable_price_move_bps=None,
            opposing_depth_retention=None,
            reasons=("No absorption.",),
        ),
        sweep=SweepDetection(
            detected=True,
            direction=SweepDirection.UP if long else SweepDirection.DOWN,
            score=90.0,
            aggressive_notional=10_000.0,
            aggressive_flow_ratio=3.0,
            levels_consumed=4,
            swept_notional=8_000.0,
            price_displacement_bps=5.0,
            depth_depletion=0.8,
            reasons=("Confirmed liquidity sweep.",),
        ),
        pressure=PressureMetrics(
            book_pressure=0.8 * sign,
            trade_pressure=0.8 * sign,
            depth_pressure=0.6 * sign,
            combined_pressure=0.8 * sign,
            direction=PressureDirection.BUY if long else PressureDirection.SELL,
            confidence=90.0,
            reasons=("Directional liquidity pressure.",),
        ),
    )


def combined_metrics(sign: float = 1.0) -> CombinedMarketMetrics:
    book = orderbook(sign)
    flow = trade_flow(sign)
    return CombinedMarketMetrics(
        spread_bps=book.spread_bps,
        book_age_ms=book.book_age_ms,
        trade_age_ms=10.0,
        delta_1s=flow.delta_1s,
        delta_5s=flow.delta_5s,
        delta_15s=flow.delta_15s,
        delta_60s=flow.delta_60s,
        cvd_process=flow.cvd_process,
        cvd_utc_day=flow.cvd_utc_day,
        cvd_episode=flow.cvd_episode,
        imbalance_l1=book.imbalance_l1,
        imbalance_l5=book.imbalance_l5,
        imbalance_l10=book.imbalance_l10,
        bid_depth_10bps=book.bid_depth_10bps,
        ask_depth_10bps=book.ask_depth_10bps,
        book_pressure=0.8 * sign,
        trade_pressure=0.8 * sign,
        depth_pressure=0.6 * sign,
        combined_pressure=0.8 * sign,
        sweep_score=90.0,
        absorption_score=0.0,
        strongest_bid_wall_ratio=5.0 if sign > 0 else None,
        strongest_ask_wall_ratio=5.0 if sign < 0 else None,
    )


def market_state(sign: float = 1.0, *, ready: bool = True) -> MarketStateAssessment:
    long = sign > 0
    return MarketStateAssessment(
        symbol="BTCUSDT",
        assessed_at=NOW,
        state=MarketState.BUY_PRESSURE if long else MarketState.SELL_PRESSURE,
        bias=MarketBias.BULLISH if long else MarketBias.BEARISH,
        directional_score=0.9 * sign,
        confidence=90.0 if ready else 0.0,
        ready=ready,
        metrics=combined_metrics(sign),
        reasons=("Directional market state.",),
        confirmations=("Market state confirms direction.",) if ready else (),
        warnings=(),
    )


def risk(*, blocked: bool = False, score: float = 0.0) -> RiskContext:
    return RiskContext(
        level=RiskLevel.BLOCKED if blocked else RiskLevel.LOW,
        score=100.0 if blocked else score,
        factors=(),
        reasons=("Risk context blocks the setup.",) if blocked else (),
    )


def opportunity(
    sign: float = 1.0,
    *,
    direction: ShadowDirection | None = None,
    invalid_structure: bool = False,
    risk_context: RiskContext | None = None,
) -> ShadowOpportunity:
    long = sign > 0
    selected_direction = direction or (
        ShadowDirection.LONG if long else ShadowDirection.SHORT
    )
    selected_risk = risk_context or risk()
    invalidation = 98.0 if long else 102.0
    if invalid_structure:
        invalidation = 102.0 if long else 98.0
    state = market_state(sign)
    return ShadowOpportunity(
        symbol="BTCUSDT",
        assessed_at=NOW,
        direction=selected_direction,
        decision=(
            ShadowOpportunityDecision.BLOCKED
            if selected_risk.level is RiskLevel.BLOCKED
            else ShadowOpportunityDecision.SHADOW_CANDIDATE
        ),
        opportunity_score=90.0,
        confidence=90.0,
        market_alignment=1.0,
        market_state=state,
        price_context=PriceContext(
            symbol="BTCUSDT",
            assessed_at=NOW,
            direction=selected_direction,
            market_price=100.0,
            atr=2.0,
            trigger_price=100.0,
            invalidation_price=invalidation,
        ),
        risk=selected_risk,
        reasons=("Strong offline setup context.",),
        confirmations=("Structure confirms the setup.",),
        warnings=selected_risk.reasons,
    )


def score(sign: float = 1.0) -> ScalperScore:
    selected_risk = risk()
    return simulate_scalper_score(
        market_state(sign),
        liquidity(sign),
        trade_flow(sign),
        orderbook(sign),
        opportunity(sign, risk_context=selected_risk),
        selected_risk,
    )


def test_strong_long_setup_is_explainable() -> None:
    result = score()
    assert result.direction is ScalperDirection.LONG
    assert result.decision is ScalperDecision.STRONG_SCALP
    assert result.total_score >= 80.0
    assert result.confidence >= 70.0
    assert any("Trade Flow" in reason for reason in result.reasons)
    assert any("Liquidity sweep" in reason for reason in result.reasons)


def test_strong_short_setup_is_symmetric() -> None:
    result = score(-1.0)
    assert result.direction is ScalperDirection.SHORT
    assert result.decision is ScalperDecision.STRONG_SCALP
    assert result.total_score == pytest.approx(score().total_score)


def test_weak_market_is_watch_not_a_directional_signal() -> None:
    state = replace(
        market_state(),
        state=MarketState.BALANCED,
        bias=MarketBias.NEUTRAL,
        directional_score=0.0,
        confidence=35.0,
    )
    flow = replace(
        trade_flow(),
        delta_1s=0.0,
        delta_5s=0.0,
        delta_15s=0.0,
        delta_60s=0.0,
        cvd_process=0.0,
        cvd_utc_day=0.0,
        cvd_episode=0.0,
    )
    neutral_liquidity = replace(
        liquidity(),
        bid_walls=(),
        sweep=replace(liquidity().sweep, detected=False, direction=SweepDirection.NONE),
        pressure=replace(
            liquidity().pressure,
            book_pressure=0.0,
            trade_pressure=0.0,
            depth_pressure=0.0,
            combined_pressure=0.0,
            direction=PressureDirection.NEUTRAL,
            confidence=25.0,
        ),
    )
    weak_opportunity = replace(
        opportunity(),
        decision=ShadowOpportunityDecision.WATCH,
        opportunity_score=20.0,
        confidence=30.0,
    )
    result = simulate_scalper_score(
        state,
        neutral_liquidity,
        flow,
        replace(
            orderbook(),
            imbalance_l1=0.0,
            imbalance_l5=0.0,
            imbalance_l10=0.0,
            bid_depth_10bps=10_000.0,
            ask_depth_10bps=10_000.0,
        ),
        weak_opportunity,
        risk(score=20.0),
    )
    assert result.decision is ScalperDecision.WATCH
    assert result.total_score < 65.0


def test_blocked_risk_is_a_hard_gate() -> None:
    blocked_risk = risk(blocked=True)
    result = simulate_scalper_score(
        market_state(),
        liquidity(),
        trade_flow(),
        orderbook(),
        opportunity(risk_context=blocked_risk),
        blocked_risk,
    )
    assert result.decision is ScalperDecision.BLOCKED
    assert result.direction is ScalperDirection.LONG
    assert result.component_scores.risk_score == -30.0
    assert any("Risk Context is blocked" in warning for warning in result.warnings)


@pytest.mark.parametrize(
    ("bad_book", "bad_state", "warning"),
    [
        (orderbook(ready=False), market_state(), "Order book is unhealthy"),
        (orderbook(book_age_ms=2_000.0), market_state(), "stale or missing"),
        (orderbook(spread_bps=9.0), market_state(), "Spread exceeds"),
        (orderbook(), market_state(ready=False), "Market State data are not ready"),
    ],
)
def test_missing_stale_or_unhealthy_data_are_blocked(
    bad_book: OrderBookMetrics,
    bad_state: MarketStateAssessment,
    warning: str,
) -> None:
    result = simulate_scalper_score(
        bad_state,
        liquidity(),
        trade_flow(),
        bad_book,
        opportunity(),
        risk(),
    )
    assert result.decision is ScalperDecision.BLOCKED
    assert any(warning in item for item in result.warnings)


def test_opposite_liquidity_pressure_is_blocked() -> None:
    result = simulate_scalper_score(
        market_state(),
        liquidity(-1.0),
        trade_flow(),
        orderbook(),
        opportunity(),
        risk(),
    )
    assert result.decision is ScalperDecision.BLOCKED
    assert any("opposite" in warning for warning in result.warnings)


def test_missing_structural_invalidation_is_blocked() -> None:
    result = simulate_scalper_score(
        market_state(),
        liquidity(),
        trade_flow(),
        orderbook(),
        opportunity(invalid_structure=True),
        risk(),
    )
    assert result.decision is ScalperDecision.BLOCKED
    assert any("Structural invalidation" in warning for warning in result.warnings)


def test_same_input_produces_same_output_offline() -> None:
    inputs = (
        market_state(),
        liquidity(),
        trade_flow(),
        orderbook(),
        opportunity(),
        risk(),
    )
    assert ScalperScoringEngine().score(*inputs) == simulate_scalper_score(*inputs)


def test_component_weights_sum_to_total_before_clamp() -> None:
    result = score()
    components = result.component_scores
    expected = (
        components.trade_flow_score
        + components.liquidity_score
        + components.orderbook_score
        + components.market_state_score
        + components.setup_score
        + components.risk_score
    )
    assert result.total_score == pytest.approx(expected)
    assert components.trade_flow_score <= 30.0
    assert components.liquidity_score <= 25.0
    assert components.orderbook_score <= 20.0
    assert components.market_state_score <= 15.0
    assert components.setup_score <= 10.0
    assert -30.0 <= components.risk_score <= 0.0


def test_score_is_immutable() -> None:
    result = score()
    with pytest.raises(FrozenInstanceError):
        result.total_score = 0.0  # type: ignore[misc]


def test_symbol_mismatch_is_rejected() -> None:
    with pytest.raises(ValueError, match="same symbol"):
        simulate_scalper_score(
            market_state(),
            liquidity(),
            replace(trade_flow(), symbol="ETHUSDT"),
            orderbook(),
            opportunity(),
            risk(),
        )
