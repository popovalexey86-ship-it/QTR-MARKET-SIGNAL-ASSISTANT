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
    RiskFactor,
    RiskLevel,
    SetupContextEngine,
    SetupContextEngineConfig,
    ShadowDirection,
    ShadowOpportunityDecision,
    simulate_setup_context,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def combined_metrics(*, spread_bps: float | None = 2.0) -> CombinedMarketMetrics:
    return CombinedMarketMetrics(
        spread_bps=spread_bps,
        book_age_ms=10.0,
        trade_age_ms=5.0,
        delta_1s=50.0,
        delta_5s=100.0,
        delta_15s=150.0,
        delta_60s=200.0,
        cvd_process=1_000.0,
        cvd_utc_day=500.0,
        cvd_episode=250.0,
        imbalance_l1=0.4,
        imbalance_l5=0.3,
        imbalance_l10=0.2,
        bid_depth_10bps=10_000.0,
        ask_depth_10bps=8_000.0,
        book_pressure=0.3,
        trade_pressure=0.4,
        depth_pressure=0.2,
        combined_pressure=0.35,
        sweep_score=0.0,
        absorption_score=0.0,
        strongest_bid_wall_ratio=None,
        strongest_ask_wall_ratio=None,
    )


def market(
    *,
    bias: MarketBias = MarketBias.BULLISH,
    directional_score: float = 0.7,
    confidence: float = 80.0,
    ready: bool = True,
    spread_bps: float | None = 2.0,
    warnings: tuple[str, ...] = (),
    assessed_at: datetime = NOW,
    symbol: str = "BTCUSDT",
) -> MarketStateAssessment:
    if not ready:
        state = MarketState.NOT_READY
        actual_bias = MarketBias.UNKNOWN
        score = 0.0
        actual_confidence = 0.0
        reasons = ("Market data не готовы.",)
    elif bias is MarketBias.BULLISH:
        state = MarketState.BUY_PRESSURE
        actual_bias = bias
        score = abs(directional_score)
        actual_confidence = confidence
        reasons = ("Давление покупателей подтверждено.",)
    elif bias is MarketBias.BEARISH:
        state = MarketState.SELL_PRESSURE
        actual_bias = bias
        score = -abs(directional_score)
        actual_confidence = confidence
        reasons = ("Давление продавцов подтверждено.",)
    else:
        state = MarketState.BALANCED
        actual_bias = bias
        score = 0.0
        actual_confidence = confidence
        reasons = ("Рынок сбалансирован.",)
    return MarketStateAssessment(
        symbol=symbol,
        assessed_at=assessed_at,
        state=state,
        bias=actual_bias,
        directional_score=score,
        confidence=actual_confidence,
        ready=ready,
        metrics=combined_metrics(spread_bps=spread_bps),
        reasons=reasons,
        confirmations=("Market State подтверждён.",) if ready else (),
        warnings=warnings,
    )


def price(
    *,
    direction: ShadowDirection = ShadowDirection.LONG,
    market_price: float = 100.0,
    trigger_price: float = 100.0,
    invalidation_price: float | None = None,
    atr: float = 2.0,
    ready: bool = True,
    assessed_at: datetime = NOW,
    symbol: str = "BTCUSDT",
) -> PriceContext:
    if invalidation_price is None:
        invalidation_price = 98.0 if direction is not ShadowDirection.SHORT else 102.0
    return PriceContext(
        symbol=symbol,
        assessed_at=assessed_at,
        direction=direction,
        market_price=market_price,
        atr=atr,
        trigger_price=trigger_price,
        invalidation_price=invalidation_price,
        local_range_low=95.0,
        local_range_high=105.0,
        ready=ready,
        health_reasons=() if ready else ("Недостаточно истории цены.",),
        confirmations=("Цена находится у trigger.",),
    )


def test_aligned_long_creates_explainable_shadow_candidate() -> None:
    result = SetupContextEngine().analyze(market(), price())

    assert result.direction is ShadowDirection.LONG
    assert result.decision is ShadowOpportunityDecision.SHADOW_CANDIDATE
    assert result.market_alignment == 1.0
    assert result.opportunity_score >= 60.0
    assert result.risk.level is RiskLevel.LOW
    assert result.reasons
    assert "согласованно" in " ".join(result.confirmations)


def test_aligned_short_is_symmetric() -> None:
    result = SetupContextEngine().analyze(
        market(bias=MarketBias.BEARISH),
        price(direction=ShadowDirection.SHORT),
    )
    assert result.direction is ShadowDirection.SHORT
    assert result.decision is ShadowOpportunityDecision.SHADOW_CANDIDATE
    assert result.market_alignment == 1.0


def test_market_conflict_is_explicit_and_never_candidate() -> None:
    result = SetupContextEngine().analyze(
        market(bias=MarketBias.BEARISH),
        price(direction=ShadowDirection.LONG),
    )
    assert result.decision is ShadowOpportunityDecision.CONFLICTED
    assert result.market_alignment == -1.0
    assert RiskFactor.MARKET_CONFLICT in result.risk.factors
    assert "противоречит" in " ".join(result.warnings)


def test_neutral_price_context_remains_watch() -> None:
    result = SetupContextEngine().analyze(
        market(bias=MarketBias.NEUTRAL),
        price(direction=ShadowDirection.NEUTRAL),
    )
    assert result.decision is ShadowOpportunityDecision.WATCH
    assert result.direction is ShadowDirection.NEUTRAL
    assert result.market_alignment == 0.0


@pytest.mark.parametrize(
    ("market_ready", "price_ready"),
    [(False, True), (True, False)],
)
def test_unready_source_blocks_shadow_analysis(
    market_ready: bool,
    price_ready: bool,
) -> None:
    result = SetupContextEngine().analyze(
        market(ready=market_ready),
        price(ready=price_ready),
    )
    assert result.decision is ShadowOpportunityDecision.BLOCKED
    assert result.risk.level is RiskLevel.BLOCKED
    assert result.opportunity_score == 0.0
    assert RiskFactor.DATA_NOT_READY in result.risk.factors


def test_invalid_structural_invalidation_blocks_context() -> None:
    result = SetupContextEngine().analyze(
        market(),
        price(invalidation_price=101.0),
    )
    assert result.decision is ShadowOpportunityDecision.BLOCKED
    assert RiskFactor.STRUCTURE_INVALID in result.risk.factors


def test_excessive_extension_is_visible_risk_and_reduces_score() -> None:
    normal = SetupContextEngine().analyze(market(), price())
    extended = SetupContextEngine().analyze(
        market(),
        price(market_price=103.0, trigger_price=100.0),
    )
    assert RiskFactor.EXCESSIVE_EXTENSION in extended.risk.factors
    assert extended.opportunity_score < normal.opportunity_score
    assert "далеко ушла" in " ".join(extended.warnings)


@pytest.mark.parametrize(
    ("invalidation", "factor"),
    [
        (99.8, RiskFactor.STOP_TOO_TIGHT),
        (95.0, RiskFactor.STOP_TOO_WIDE),
    ],
)
def test_stop_distance_risk_is_classified(
    invalidation: float,
    factor: RiskFactor,
) -> None:
    result = SetupContextEngine().analyze(
        market(),
        price(invalidation_price=invalidation),
    )
    assert factor in result.risk.factors
    assert result.risk.level in {RiskLevel.MODERATE, RiskLevel.HIGH}


def test_wide_or_missing_spread_is_not_hidden() -> None:
    wide = SetupContextEngine().analyze(market(spread_bps=12.0), price())
    assert RiskFactor.WIDE_SPREAD in wide.risk.factors
    assert "Spread" in " ".join(wide.warnings)

    missing = SetupContextEngine().analyze(market(spread_bps=None), price())
    assert missing.decision is ShadowOpportunityDecision.BLOCKED
    assert RiskFactor.DATA_NOT_READY in missing.risk.factors


def test_market_warning_becomes_opposing_liquidity_risk() -> None:
    result = SetupContextEngine().analyze(
        market(warnings=("Сильная ask wall ограничивает bullish-сценарий.",)),
        price(),
    )
    assert RiskFactor.OPPOSING_LIQUIDITY in result.risk.factors
    assert "ask wall" in " ".join(result.warnings)


def test_price_context_derives_normalized_price_facts() -> None:
    context = price(market_price=101.0, trigger_price=100.0, invalidation_price=98.0)
    assert context.trigger_progress_atr == 0.5
    assert context.stop_distance_atr == 1.5
    assert context.structure_valid is True
    assert context.range_position == 0.6


def test_offline_entry_point_matches_direct_engine() -> None:
    market_state = market()
    price_context = price()
    direct = SetupContextEngine().analyze(market_state, price_context)
    offline = simulate_setup_context(market_state, price_context)
    assert offline == direct


def test_inputs_are_not_mutated_and_result_is_immutable() -> None:
    market_state = market()
    price_context = price()
    result = SetupContextEngine().analyze(market_state, price_context)
    assert result.market_state is market_state
    assert result.price_context is price_context
    with pytest.raises(FrozenInstanceError):
        result.opportunity_score = 0.0  # type: ignore[misc]


def test_symbol_and_timestamp_must_match() -> None:
    with pytest.raises(ValueError, match="same symbol"):
        SetupContextEngine().analyze(market(), price(symbol="ETHUSDT"))
    with pytest.raises(ValueError, match="same assessed_at"):
        SetupContextEngine().analyze(
            market(),
            price(assessed_at=NOW + timedelta(seconds=1)),
        )


def test_price_and_engine_configuration_are_validated() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        price(atr=0.0)
    with pytest.raises(ValueError, match="range is inverted"):
        PriceContext(
            symbol="BTCUSDT",
            assessed_at=NOW,
            direction=ShadowDirection.LONG,
            market_price=100.0,
            atr=2.0,
            trigger_price=100.0,
            invalidation_price=98.0,
            local_range_low=105.0,
            local_range_high=95.0,
        )
    with pytest.raises(ValueError, match="stop distance range is inverted"):
        SetupContextEngineConfig(
            minimum_stop_distance_atr=2.0,
            maximum_stop_distance_atr=1.0,
        )
