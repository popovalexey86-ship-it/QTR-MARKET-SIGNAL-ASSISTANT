from dataclasses import FrozenInstanceError, dataclass, replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    AbsorptionDetection,
    FlowSide,
    LiquidityIntelligence,
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
    ScalperComponentScores,
    ScalperDecision,
    ScalperDirection,
    ScalperScore,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    RiskContext,
    RiskLevel,
    ShadowDirection,
    ShadowOpportunity,
    ShadowOpportunityDecision,
)
from market_signal_assistant.qtr_micro_scalper.snapshot import (
    MicrostructureSnapshotBuilder,
    MicrostructureSnapshotBundle,
    SnapshotReadiness,
    simulate_microstructure_snapshot,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


@dataclass(frozen=True)
class Components:
    trade_flow: TradeFlowMetrics
    orderbook: OrderBookMetrics
    liquidity: LiquidityIntelligence
    market_state: MarketStateAssessment
    setup_context: ShadowOpportunity
    scalper_score: ScalperScore


def market_metrics() -> CombinedMarketMetrics:
    return CombinedMarketMetrics(
        spread_bps=1.0,
        book_age_ms=10.0,
        trade_age_ms=10.0,
        delta_1s=100.0,
        delta_5s=200.0,
        delta_15s=300.0,
        delta_60s=500.0,
        cvd_process=1_000.0,
        cvd_utc_day=2_000.0,
        cvd_episode=500.0,
        imbalance_l1=0.5,
        imbalance_l5=0.5,
        imbalance_l10=0.4,
        bid_depth_10bps=20_000.0,
        ask_depth_10bps=10_000.0,
        book_pressure=0.5,
        trade_pressure=0.6,
        depth_pressure=0.3,
        combined_pressure=0.5,
        sweep_score=80.0,
        absorption_score=0.0,
        strongest_bid_wall_ratio=None,
        strongest_ask_wall_ratio=None,
    )


def components() -> Components:
    flow = TradeFlowMetrics(
        symbol="BTCUSDT",
        as_of=NOW,
        buy_notional_1s=2_000.0,
        sell_notional_1s=500.0,
        delta_1s=100.0,
        delta_5s=200.0,
        delta_15s=300.0,
        delta_60s=500.0,
        cvd_process=1_000.0,
        cvd_utc_day=2_000.0,
        cvd_episode=500.0,
        trade_count_5s=20,
        largest_trade_5s=500.0,
        block_delta_60s=0.0,
        rpi_delta_60s=0.0,
        last_trade_at=NOW - timedelta(milliseconds=10),
    )
    book = OrderBookMetrics(
        symbol="BTCUSDT",
        as_of=NOW,
        book_exchange_at=NOW - timedelta(milliseconds=10),
        book_age_ms=10.0,
        update_id=10,
        cross_sequence=10,
        bid_levels=10,
        ask_levels=10,
        best_bid=99.99,
        best_ask=100.01,
        mid_price=100.0,
        microprice=100.0,
        spread_bps=1.0,
        bid_depth_5bps=10_000.0,
        ask_depth_5bps=5_000.0,
        bid_depth_10bps=20_000.0,
        ask_depth_10bps=10_000.0,
        bid_depth_25bps=30_000.0,
        ask_depth_25bps=15_000.0,
        imbalance_l1=0.5,
        imbalance_l5=0.5,
        imbalance_l10=0.4,
        imbalance_l25=0.3,
        imbalance_l50=0.2,
        ready=True,
        health_reasons=(),
    )
    liquidity = LiquidityIntelligence(
        symbol="BTCUSDT",
        bid_walls=(),
        ask_walls=(),
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
            direction=SweepDirection.UP,
            score=80.0,
            aggressive_notional=10_000.0,
            aggressive_flow_ratio=2.0,
            levels_consumed=3,
            swept_notional=8_000.0,
            price_displacement_bps=4.0,
            depth_depletion=0.7,
            reasons=("Sweep confirmed.",),
        ),
        pressure=PressureMetrics(
            book_pressure=0.5,
            trade_pressure=0.6,
            depth_pressure=0.3,
            combined_pressure=0.5,
            direction=PressureDirection.BUY,
            confidence=80.0,
            reasons=("Buy pressure.",),
        ),
    )
    state = MarketStateAssessment(
        symbol="BTCUSDT",
        assessed_at=NOW,
        state=MarketState.BUY_PRESSURE,
        bias=MarketBias.BULLISH,
        directional_score=0.8,
        confidence=85.0,
        ready=True,
        metrics=market_metrics(),
        reasons=("Bullish microstructure.",),
        confirmations=("Trade Flow and OrderBook agree.",),
        warnings=(),
    )
    risk = RiskContext(level=RiskLevel.LOW, score=0.0, factors=(), reasons=())
    setup = ShadowOpportunity(
        symbol="BTCUSDT",
        assessed_at=NOW,
        direction=ShadowDirection.LONG,
        decision=ShadowOpportunityDecision.SHADOW_CANDIDATE,
        opportunity_score=85.0,
        confidence=80.0,
        market_alignment=1.0,
        market_state=state,
        price_context=PriceContext(
            symbol="BTCUSDT",
            assessed_at=NOW,
            direction=ShadowDirection.LONG,
            market_price=100.0,
            atr=2.0,
            trigger_price=100.0,
            invalidation_price=98.0,
        ),
        risk=risk,
        reasons=("Setup context is aligned.",),
        confirmations=("Structure confirms LONG.",),
        warnings=(),
    )
    score = ScalperScore(
        total_score=85.0,
        decision=ScalperDecision.STRONG_SCALP,
        direction=ScalperDirection.LONG,
        confidence=82.0,
        component_scores=ScalperComponentScores(
            liquidity_score=22.0,
            trade_flow_score=27.0,
            orderbook_score=17.0,
            market_state_score=13.0,
            setup_score=8.0,
            risk_score=-2.0,
        ),
        reasons=("All scoring components align.",),
        warnings=(),
    )
    return Components(flow, book, liquidity, state, setup, score)


def build_complete(
    values: Components | None = None,
    *,
    generated_at: datetime = NOW,
) -> MicrostructureSnapshotBundle:
    selected = values or components()
    return simulate_microstructure_snapshot(
        symbol="BTCUSDT",
        generated_at=generated_at,
        trade_flow=selected.trade_flow,
        orderbook=selected.orderbook,
        liquidity=selected.liquidity,
        market_state=selected.market_state,
        setup_context=selected.setup_context,
        scalper_score=selected.scalper_score,
    )


def test_complete_bundle_is_ready_and_preserves_components() -> None:
    values = components()
    bundle = build_complete(values)
    assert bundle.readiness is SnapshotReadiness.READY
    assert bundle.trade_flow is values.trade_flow
    assert bundle.orderbook is values.orderbook
    assert bundle.liquidity is values.liquidity
    assert bundle.market_state is values.market_state
    assert bundle.setup_context is values.setup_context
    assert bundle.scalper_score is values.scalper_score
    assert bundle.missing_components == ()
    assert bundle.warnings == ()


def test_missing_derived_component_is_partial_and_explainable() -> None:
    values = components()
    bundle = simulate_microstructure_snapshot(
        symbol="BTCUSDT",
        generated_at=NOW,
        trade_flow=values.trade_flow,
        orderbook=values.orderbook,
        liquidity=values.liquidity,
        market_state=values.market_state,
        setup_context=values.setup_context,
    )
    assert bundle.readiness is SnapshotReadiness.PARTIAL
    assert bundle.missing_components == ("scalper_score",)
    assert "Missing component: scalper_score." in bundle.warnings


def test_missing_core_component_is_not_ready() -> None:
    values = components()
    bundle = simulate_microstructure_snapshot(
        symbol="BTCUSDT",
        generated_at=NOW,
        orderbook=values.orderbook,
        liquidity=values.liquidity,
        market_state=values.market_state,
    )
    assert bundle.readiness is SnapshotReadiness.NOT_READY
    assert "trade_flow" in bundle.missing_components


def test_empty_bundle_is_not_ready_and_lists_every_component() -> None:
    bundle = simulate_microstructure_snapshot(symbol="BTCUSDT", generated_at=NOW)
    assert bundle.readiness is SnapshotReadiness.NOT_READY
    assert bundle.missing_components == (
        "trade_flow",
        "orderbook",
        "liquidity",
        "market_state",
        "setup_context",
        "scalper_score",
    )
    assert len(bundle.warnings) == 6


def test_unhealthy_orderbook_is_not_ready_with_source_warning() -> None:
    values = components()
    bad_book = replace(
        values.orderbook,
        ready=False,
        health_reasons=("snapshot_not_ready",),
    )
    bundle = build_complete(replace(values, orderbook=bad_book))
    assert bundle.readiness is SnapshotReadiness.NOT_READY
    assert any("snapshot_not_ready" in warning for warning in bundle.warnings)


def test_blocked_score_does_not_make_complete_data_not_ready() -> None:
    values = components()
    blocked_score = replace(
        values.scalper_score,
        decision=ScalperDecision.BLOCKED,
        warnings=("BLOCKED: spread is too wide.",),
    )
    bundle = build_complete(replace(values, scalper_score=blocked_score))
    assert bundle.readiness is SnapshotReadiness.READY
    assert "BLOCKED: spread is too wide." in bundle.warnings


def test_symbol_mismatch_is_rejected() -> None:
    values = components()
    with pytest.raises(ValueError, match="symbol mismatch: trade_flow"):
        mismatched_flow = replace(values.trade_flow, symbol="ETHUSDT")
        build_complete(replace(values, trade_flow=mismatched_flow))


def test_generated_at_must_be_timezone_aware() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        build_complete(generated_at=datetime(2026, 8, 16, 12))


def test_component_timestamp_cannot_be_in_the_future() -> None:
    values = components()
    future_flow = replace(values.trade_flow, as_of=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="cannot be in the future"):
        build_complete(replace(values, trade_flow=future_flow))


def test_component_timestamp_skew_is_rejected() -> None:
    values = components()
    old_flow = replace(values.trade_flow, as_of=NOW - timedelta(seconds=2))
    with pytest.raises(ValueError, match="differ by more than one second"):
        build_complete(
            replace(values, trade_flow=old_flow),
            generated_at=NOW + timedelta(seconds=1),
        )


def test_generated_at_is_normalized_to_utc() -> None:
    berlin = timezone(timedelta(hours=2))
    bundle = build_complete(generated_at=NOW.astimezone(berlin))
    assert bundle.generated_at == NOW
    assert bundle.generated_at.tzinfo is UTC


def test_offline_simulation_is_deterministic() -> None:
    values = components()
    direct = MicrostructureSnapshotBuilder().build(
        symbol="BTCUSDT",
        generated_at=NOW,
        trade_flow=values.trade_flow,
        orderbook=values.orderbook,
        liquidity=values.liquidity,
        market_state=values.market_state,
        setup_context=values.setup_context,
        scalper_score=values.scalper_score,
    )
    assert direct == build_complete(values)


def test_bundle_is_immutable() -> None:
    bundle = build_complete()
    with pytest.raises(FrozenInstanceError):
        bundle.readiness = SnapshotReadiness.NOT_READY  # type: ignore[misc]


def test_direct_model_cannot_claim_inconsistent_readiness() -> None:
    with pytest.raises(ValueError, match="readiness is inconsistent"):
        replace(build_complete(), readiness=SnapshotReadiness.PARTIAL)
