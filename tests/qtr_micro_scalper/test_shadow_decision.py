from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    CombinedMarketMetrics,
    MarketBias,
    MarketState,
    MarketStateAssessment,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    RiskContext,
    RiskLevel,
    ShadowDirection,
    ShadowOpportunity,
    ShadowOpportunityDecision,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowDecisionConfig,
    ShadowDecisionEngine,
    ShadowOutcomeStatus,
    ShadowOutcomeTracker,
    ShadowPriceBar,
    ShadowTradeEventType,
    ShadowTradeStage,
    calculate_shadow_levels,
    shadow_outcome,
    simulate_shadow_trade,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def metrics() -> CombinedMarketMetrics:
    return CombinedMarketMetrics(
        spread_bps=2.0,
        book_age_ms=10.0,
        trade_age_ms=5.0,
        delta_1s=10.0,
        delta_5s=20.0,
        delta_15s=30.0,
        delta_60s=40.0,
        cvd_process=100.0,
        cvd_utc_day=80.0,
        cvd_episode=50.0,
        imbalance_l1=0.2,
        imbalance_l5=0.2,
        imbalance_l10=0.1,
        bid_depth_10bps=10_000.0,
        ask_depth_10bps=9_000.0,
        book_pressure=0.2,
        trade_pressure=0.3,
        depth_pressure=0.1,
        combined_pressure=0.25,
        sweep_score=0.0,
        absorption_score=0.0,
        strongest_bid_wall_ratio=None,
        strongest_ask_wall_ratio=None,
    )


def opportunity(
    *,
    direction: ShadowDirection = ShadowDirection.LONG,
    decision: ShadowOpportunityDecision = (
        ShadowOpportunityDecision.SHADOW_CANDIDATE
    ),
    market_price: float = 100.0,
    trigger_price: float = 100.0,
    invalidation_price: float | None = None,
    atr: float = 2.0,
) -> ShadowOpportunity:
    if invalidation_price is None:
        invalidation_price = 98.0 if direction is not ShadowDirection.SHORT else 102.0
    bias = (
        MarketBias.BEARISH
        if direction is ShadowDirection.SHORT
        else MarketBias.BULLISH
    )
    market_state = MarketStateAssessment(
        symbol="BTCUSDT",
        assessed_at=NOW,
        state=(
            MarketState.SELL_PRESSURE
            if direction is ShadowDirection.SHORT
            else MarketState.BUY_PRESSURE
        ),
        bias=bias,
        directional_score=-0.7 if bias is MarketBias.BEARISH else 0.7,
        confidence=80.0,
        ready=True,
        metrics=metrics(),
        reasons=("Market State подтверждён.",),
        confirmations=("Направленное давление подтверждено.",),
        warnings=(),
    )
    price_context = PriceContext(
        symbol="BTCUSDT",
        assessed_at=NOW,
        direction=direction,
        market_price=market_price,
        atr=atr,
        trigger_price=trigger_price,
        invalidation_price=invalidation_price,
    )
    return ShadowOpportunity(
        symbol="BTCUSDT",
        assessed_at=NOW,
        direction=direction,
        decision=decision,
        opportunity_score=80.0,
        confidence=75.0,
        market_alignment=1.0 if direction is not ShadowDirection.NEUTRAL else 0.0,
        market_state=market_state,
        price_context=price_context,
        risk=RiskContext(level=RiskLevel.LOW, score=0.0, factors=(), reasons=()),
        reasons=("Shadow opportunity подтверждён.",),
        confirmations=("Market State и Price Context согласованы.",),
        warnings=(),
    )


def bar(
    index: int,
    *,
    open_price: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.0,
    symbol: str = "BTCUSDT",
) -> ShadowPriceBar:
    opened_at = NOW + timedelta(seconds=index)
    return ShadowPriceBar(
        symbol=symbol,
        opened_at=opened_at,
        closed_at=opened_at + timedelta(seconds=1),
        open=open_price,
        high=high,
        low=low,
        close=close,
    )


def test_long_entry_stop_and_targets_are_calculated_in_r() -> None:
    levels = calculate_shadow_levels(opportunity())
    assert levels.entry_price == 100.0
    assert levels.initial_stop == 97.8
    assert levels.risk_per_unit == pytest.approx(2.2)
    assert levels.tp1_price == pytest.approx(102.2)
    assert levels.tp2_price == pytest.approx(104.4)


def test_short_levels_are_symmetric() -> None:
    levels = calculate_shadow_levels(opportunity(direction=ShadowDirection.SHORT))
    assert levels.entry_price == 100.0
    assert levels.initial_stop == 102.2
    assert levels.tp1_price == pytest.approx(97.8)
    assert levels.tp2_price == pytest.approx(95.6)


def test_entry_never_uses_better_historical_price_after_trigger() -> None:
    long_levels = calculate_shadow_levels(
        opportunity(market_price=101.0, trigger_price=100.0)
    )
    short_levels = calculate_shadow_levels(
        opportunity(
            direction=ShadowDirection.SHORT,
            market_price=99.0,
            trigger_price=100.0,
        )
    )
    assert long_levels.entry_price == 101.0
    assert short_levels.entry_price == 99.0


def test_non_candidate_and_neutral_opportunity_do_not_create_trade() -> None:
    rejected = ShadowDecisionEngine().create_trade(
        opportunity(decision=ShadowOpportunityDecision.WATCH)
    )
    assert rejected.accepted is False

    neutral = opportunity(direction=ShadowDirection.NEUTRAL)
    neutral_result = ShadowDecisionEngine().create_trade(neutral)
    assert neutral_result.accepted is False


def test_trade_plan_is_waiting_and_contains_no_external_order_data() -> None:
    decision = ShadowDecisionEngine().create_trade(opportunity())
    assert decision.trade is not None
    assert decision.trade.stage is ShadowTradeStage.WAITING_ENTRY
    assert decision.trade.trade_id.startswith("shadow-")
    assert decision.trade.events == ()


def test_full_long_tp1_tp2_lifecycle_realizes_weighted_r() -> None:
    result = simulate_shadow_trade(
        opportunity(),
        (bar(1, high=104.5, low=98.5, close=104.0),),
    )
    assert result.trade is not None
    assert result.outcome is not None
    assert result.trade.stage is ShadowTradeStage.CLOSED
    assert result.trade.tp1_hit is True
    assert result.trade.tp2_hit is True
    assert result.outcome.status is ShadowOutcomeStatus.WIN
    assert result.outcome.realized_r == pytest.approx(1.5)
    assert [event.event_type for event in result.trade.events] == [
        ShadowTradeEventType.ENTRY,
        ShadowTradeEventType.TP1,
        ShadowTradeEventType.TP2,
    ]


def test_short_tp_lifecycle_is_symmetric() -> None:
    result = simulate_shadow_trade(
        opportunity(direction=ShadowDirection.SHORT),
        (bar(1, high=101.0, low=95.5, close=96.0),),
    )
    assert result.outcome is not None
    assert result.outcome.status is ShadowOutcomeStatus.WIN
    assert result.outcome.realized_r == pytest.approx(1.5)


def test_initial_stop_realizes_minus_one_r() -> None:
    result = simulate_shadow_trade(
        opportunity(),
        (bar(1, high=101.0, low=97.5, close=98.0),),
    )
    assert result.outcome is not None
    assert result.outcome.status is ShadowOutcomeStatus.LOSS
    assert result.outcome.realized_r == pytest.approx(-1.0)
    assert result.outcome.events[-1].event_type is ShadowTradeEventType.STOP


def test_stop_first_policy_is_conservative_on_ambiguous_bar() -> None:
    result = simulate_shadow_trade(
        opportunity(),
        (bar(1, high=105.0, low=97.0, close=102.0),),
    )
    assert result.outcome is not None
    assert result.outcome.realized_r == pytest.approx(-1.0)
    assert result.outcome.tp1_hit is False
    assert "STOP_FIRST" in result.outcome.events[-1].reason


def test_tp1_moves_virtual_stop_to_breakeven() -> None:
    result = simulate_shadow_trade(
        opportunity(),
        (
            bar(1, high=102.3, low=98.5, close=102.0),
            bar(2, open_price=101.0, high=101.5, low=99.5, close=100.0),
        ),
    )
    assert result.trade is not None
    assert result.outcome is not None
    assert result.trade.current_stop == result.trade.entry_price
    assert result.outcome.realized_r == pytest.approx(0.5)
    assert result.outcome.status is ShadowOutcomeStatus.WIN


def test_untriggered_plan_expires_without_virtual_fill() -> None:
    result = simulate_shadow_trade(
        opportunity(market_price=100.0, trigger_price=101.0),
        (bar(61, high=100.5, low=99.0, close=100.0),),
    )
    assert result.trade is not None
    assert result.outcome is not None
    assert result.trade.stage is ShadowTradeStage.EXPIRED
    assert result.outcome.status is ShadowOutcomeStatus.NOT_TRIGGERED
    assert result.outcome.entry_at is None


def test_maximum_holding_bars_closes_at_bar_close() -> None:
    config = ShadowDecisionConfig(maximum_holding_bars=2)
    result = simulate_shadow_trade(
        opportunity(),
        (
            bar(1, high=101.0, low=99.0, close=100.5),
            bar(2, open_price=100.5, high=101.5, low=99.0, close=101.0),
        ),
        config=config,
    )
    assert result.outcome is not None
    assert result.outcome.events[-1].event_type is ShadowTradeEventType.TIME_EXIT
    assert result.outcome.realized_r == pytest.approx(1.0 / 2.2)


def test_excursion_tracking_uses_directional_r_units() -> None:
    result = simulate_shadow_trade(
        opportunity(),
        (bar(1, high=101.1, low=98.9, close=100.0),),
    )
    assert result.outcome is not None
    assert result.outcome.max_favorable_excursion_r == pytest.approx(0.5)
    assert result.outcome.max_adverse_excursion_r == pytest.approx(0.5)


def test_offline_simulation_sorts_bars_deterministically() -> None:
    bars = (
        bar(2, open_price=102.0, high=104.5, low=101.0, close=104.0),
        bar(1, high=102.3, low=98.5, close=102.0),
    )
    ordered = simulate_shadow_trade(opportunity(), tuple(reversed(bars)))
    shuffled = simulate_shadow_trade(opportunity(), bars)
    assert ordered == shuffled


def test_bar_symbol_and_order_are_validated() -> None:
    engine = ShadowDecisionEngine()
    trade = engine.create_trade(opportunity()).trade
    assert trade is not None
    with pytest.raises(ValueError, match="same symbol"):
        engine.process_bar(trade, bar(1, symbol="ETHUSDT"))
    processed = engine.process_bar(trade, bar(2))
    with pytest.raises(ValueError, match="chronological"):
        engine.process_bar(processed, bar(1))


def test_outcome_tracker_is_duplicate_safe_and_summarizes_r() -> None:
    win = simulate_shadow_trade(
        opportunity(),
        (bar(1, high=104.5, low=98.5, close=104.0),),
    ).outcome
    loss = simulate_shadow_trade(
        opportunity(market_price=101.0, trigger_price=101.0),
        (bar(1, open_price=101.0, high=101.5, low=97.0, close=98.0),),
    ).outcome
    assert win is not None and loss is not None

    tracker = ShadowOutcomeTracker()
    assert tracker.record(win) is True
    assert tracker.record(win) is False
    assert tracker.record(loss) is True
    summary = tracker.summary()
    assert summary.total_trades == 2
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.total_realized_r == pytest.approx(0.5)
    assert summary.average_realized_r == pytest.approx(0.25)
    assert summary.win_rate == 50.0


def test_open_outcome_cannot_be_recorded() -> None:
    trade = ShadowDecisionEngine().create_trade(opportunity()).trade
    assert trade is not None
    pending = shadow_outcome(trade)
    assert pending.status is ShadowOutcomeStatus.PENDING
    with pytest.raises(ValueError, match="terminal"):
        ShadowOutcomeTracker().record(pending)


def test_shadow_trade_is_immutable_and_config_is_validated() -> None:
    trade = ShadowDecisionEngine().create_trade(opportunity()).trade
    assert trade is not None
    with pytest.raises(FrozenInstanceError):
        trade.stage = ShadowTradeStage.OPEN  # type: ignore[misc]
    with pytest.raises(ValueError, match="TP1 must be below TP2"):
        ShadowDecisionConfig(tp1_r=2.0, tp2_r=1.0)
