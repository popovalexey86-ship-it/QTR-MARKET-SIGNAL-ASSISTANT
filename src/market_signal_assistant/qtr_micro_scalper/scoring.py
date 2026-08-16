from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    BookSide,
    FlowSide,
    LiquidityIntelligence,
    PressureDirection,
    SweepDirection,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    MarketBias,
    MarketStateAssessment,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookMetrics
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    RiskContext,
    RiskLevel,
    ShadowDirection,
    ShadowOpportunity,
    ShadowOpportunityDecision,
)


class ScalperDecision(StrEnum):
    STRONG_SCALP = "STRONG_SCALP"
    SCALP = "SCALP"
    WATCH = "WATCH"
    BLOCKED = "BLOCKED"


class ScalperDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"


@dataclass(frozen=True, slots=True)
class ScalperComponentScores:
    """Weighted contributions, not independent 0..100 raw scores."""

    liquidity_score: float
    trade_flow_score: float
    orderbook_score: float
    market_state_score: float
    setup_score: float
    risk_score: float

    def __post_init__(self) -> None:
        ranges = (
            ("liquidity", self.liquidity_score, 0.0, 25.0),
            ("trade flow", self.trade_flow_score, 0.0, 30.0),
            ("orderbook", self.orderbook_score, 0.0, 20.0),
            ("market state", self.market_state_score, 0.0, 15.0),
            ("setup", self.setup_score, 0.0, 10.0),
            ("risk", self.risk_score, -30.0, 0.0),
        )
        for name, value, minimum, maximum in ranges:
            if not _finite(value) or not minimum <= value <= maximum:
                raise ValueError(f"Scalper {name} contribution is outside its range.")


@dataclass(frozen=True, slots=True)
class ScalperScore:
    total_score: float
    decision: ScalperDecision
    direction: ScalperDirection
    confidence: float
    component_scores: ScalperComponentScores
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _finite(self.total_score) or not 0.0 <= self.total_score <= 100.0:
            raise ValueError("Scalper total score must be between 0 and 100.")
        if not _finite(self.confidence) or not 0.0 <= self.confidence <= 100.0:
            raise ValueError("Scalper confidence must be between 0 and 100.")
        reasons = _texts("Scalper reason", self.reasons)
        warnings = _texts("Scalper warning", self.warnings)
        if not reasons:
            raise ValueError("Scalper score requires explainable reasons.")
        object.__setattr__(self, "reasons", reasons)
        object.__setattr__(self, "warnings", warnings)


@dataclass(frozen=True, slots=True)
class ScalperScoringConfig:
    maximum_book_age_ms: float = 1_000.0
    maximum_trade_age_ms: float = 750.0
    maximum_spread_bps: float = 8.0
    opposite_pressure_threshold: float = 0.25
    strong_scalp_score: float = 80.0
    scalp_score: float = 65.0
    strong_confidence: float = 70.0
    scalp_confidence: float = 55.0

    def __post_init__(self) -> None:
        for name, value in (
            ("maximum book age", self.maximum_book_age_ms),
            ("maximum trade age", self.maximum_trade_age_ms),
            ("maximum spread", self.maximum_spread_bps),
            ("strong score", self.strong_scalp_score),
            ("scalp score", self.scalp_score),
            ("strong confidence", self.strong_confidence),
            ("scalp confidence", self.scalp_confidence),
        ):
            if not _positive(value):
                raise ValueError(f"Scalper scoring {name} must be positive.")
        if not 0.0 < self.opposite_pressure_threshold <= 1.0:
            raise ValueError("Opposite pressure threshold must be between 0 and 1.")
        if not self.scalp_score < self.strong_scalp_score <= 100.0:
            raise ValueError("Scalper score thresholds are inverted.")
        if not self.scalp_confidence < self.strong_confidence <= 100.0:
            raise ValueError("Scalper confidence thresholds are inverted.")


class ScalperScoringEngine:
    """Explainable offline scoring; it has no authority to create an order."""

    def __init__(self, config: ScalperScoringConfig | None = None) -> None:
        self._config = config or ScalperScoringConfig()

    def score(
        self,
        market_state: MarketStateAssessment,
        liquidity: LiquidityIntelligence,
        trade_flow: TradeFlowMetrics,
        orderbook: OrderBookMetrics,
        opportunity: ShadowOpportunity,
        risk: RiskContext,
    ) -> ScalperScore:
        _validate_symbols(
            market_state,
            liquidity,
            trade_flow,
            orderbook,
            opportunity,
        )
        direction = _direction(opportunity.direction)
        sign = _direction_sign(direction)
        hard_gates = _hard_gates(
            market_state,
            liquidity,
            trade_flow,
            orderbook,
            opportunity,
            risk,
            direction,
            self._config,
        )

        trade_raw = _trade_flow_raw(trade_flow, sign)
        liquidity_raw = _liquidity_raw(liquidity, sign)
        orderbook_raw = _orderbook_raw(orderbook, sign, self._config)
        market_raw = _market_state_raw(market_state, direction)
        setup_raw = _setup_raw(opportunity)
        risk_penalty = -min(30.0, max(0.0, risk.score) * 0.30)
        components = ScalperComponentScores(
            trade_flow_score=trade_raw * 0.30,
            liquidity_score=liquidity_raw * 0.25,
            orderbook_score=orderbook_raw * 0.20,
            market_state_score=market_raw * 0.15,
            setup_score=setup_raw * 0.10,
            risk_score=risk_penalty,
        )
        total = _component_total(components)
        confidence = _confidence(market_state, liquidity, opportunity, risk)
        decision = _decision(total, confidence, direction, hard_gates, self._config)
        reasons = _component_reasons(components, direction, market_state, liquidity)
        warnings = _unique(
            (
                *opportunity.warnings,
                *risk.reasons,
                *(f"BLOCKED: {reason}" for reason in hard_gates),
            )
        )
        return ScalperScore(
            total_score=total,
            decision=decision,
            direction=direction,
            confidence=confidence,
            component_scores=components,
            reasons=reasons,
            warnings=warnings,
        )


def simulate_scalper_score(
    market_state: MarketStateAssessment,
    liquidity: LiquidityIntelligence,
    trade_flow: TradeFlowMetrics,
    orderbook: OrderBookMetrics,
    opportunity: ShadowOpportunity,
    risk: RiskContext,
    *,
    config: ScalperScoringConfig | None = None,
) -> ScalperScore:
    """Offline scoring entry point using the same implementation as the engine."""

    return ScalperScoringEngine(config).score(
        market_state,
        liquidity,
        trade_flow,
        orderbook,
        opportunity,
        risk,
    )


def _hard_gates(
    market_state: MarketStateAssessment,
    liquidity: LiquidityIntelligence,
    trade_flow: TradeFlowMetrics,
    orderbook: OrderBookMetrics,
    opportunity: ShadowOpportunity,
    risk: RiskContext,
    direction: ScalperDirection,
    config: ScalperScoringConfig,
) -> tuple[str, ...]:
    gates: list[str] = []
    if not market_state.ready:
        gates.append("Market State data are not ready.")
    if not orderbook.ready:
        gates.append("Order book is unhealthy.")
    if (
        orderbook.book_age_ms is None
        or orderbook.book_age_ms > config.maximum_book_age_ms
    ):
        gates.append("Order book data are stale or missing.")
    trade_age = _trade_age_ms(market_state, trade_flow)
    if trade_age is None or trade_age > config.maximum_trade_age_ms:
        gates.append("Trade flow data are stale or missing.")
    if orderbook.spread_bps is None:
        gates.append("Spread is unavailable.")
    elif orderbook.spread_bps > config.maximum_spread_bps:
        gates.append("Spread exceeds the scoring safety limit.")
    if (
        orderbook.best_bid is None
        or orderbook.best_ask is None
        or orderbook.mid_price is None
        or orderbook.imbalance_l5 is None
    ):
        gates.append("Required order book metrics are missing.")
    if not opportunity.price_context.structure_valid:
        gates.append("Structural invalidation is missing or invalid.")
    if risk.level is RiskLevel.BLOCKED:
        gates.append("Risk Context is blocked.")
    if _opposite_liquidity(liquidity, direction, config):
        gates.append("Liquidity pressure is opposite to setup direction.")
    return _unique(tuple(gates))


def _opposite_liquidity(
    liquidity: LiquidityIntelligence,
    direction: ScalperDirection,
    config: ScalperScoringConfig,
) -> bool:
    if direction is ScalperDirection.NONE:
        return False
    pressure = liquidity.pressure
    if direction is ScalperDirection.LONG:
        pressure_opposite = (
            pressure.direction is PressureDirection.SELL
            and pressure.combined_pressure <= -config.opposite_pressure_threshold
        )
        sweep_opposite = (
            liquidity.sweep.detected
            and liquidity.sweep.direction is SweepDirection.DOWN
        )
        absorption_opposite = (
            liquidity.absorption.detected
            and liquidity.absorption.aggressive_side is FlowSide.BUY
        )
    else:
        pressure_opposite = (
            pressure.direction is PressureDirection.BUY
            and pressure.combined_pressure >= config.opposite_pressure_threshold
        )
        sweep_opposite = (
            liquidity.sweep.detected
            and liquidity.sweep.direction is SweepDirection.UP
        )
        absorption_opposite = (
            liquidity.absorption.detected
            and liquidity.absorption.aggressive_side is FlowSide.SELL
        )
    return pressure_opposite or sweep_opposite or absorption_opposite


def _trade_flow_raw(metrics: TradeFlowMetrics, sign: float) -> float:
    weighted = (
        (metrics.delta_1s, 20.0),
        (metrics.delta_5s, 25.0),
        (metrics.delta_15s, 20.0),
        (metrics.delta_60s, 10.0),
        (metrics.cvd_process, 10.0),
        (metrics.cvd_utc_day, 10.0),
    )
    score = sum(_sign_alignment(value, sign) * weight for value, weight in weighted)
    if metrics.cvd_episode is None:
        score += 2.5
    else:
        score += _sign_alignment(metrics.cvd_episode, sign) * 5.0
    return score


def _liquidity_raw(liquidity: LiquidityIntelligence, sign: float) -> float:
    pressure_alignment = max(
        -1.0,
        min(1.0, liquidity.pressure.combined_pressure * sign),
    )
    pressure_score = (pressure_alignment + 1.0) / 2.0 * 35.0

    if not liquidity.sweep.detected:
        sweep_score = 17.5
    else:
        sweep_sign = 1.0 if liquidity.sweep.direction is SweepDirection.UP else -1.0
        sweep_score = 35.0 if sweep_sign == sign else 0.0

    if not liquidity.absorption.detected:
        absorption_score = 10.0
    else:
        bullish_absorption = liquidity.absorption.aggressive_side is FlowSide.SELL
        absorption_sign = 1.0 if bullish_absorption else -1.0
        absorption_score = 20.0 if absorption_sign == sign else 0.0

    wall_score = _wall_score(liquidity, sign)
    return pressure_score + sweep_score + absorption_score + wall_score


def _wall_score(liquidity: LiquidityIntelligence, sign: float) -> float:
    support_side = BookSide.BID if sign > 0 else BookSide.ASK
    opposing_side = BookSide.ASK if sign > 0 else BookSide.BID
    support = _strongest_wall(liquidity, support_side)
    opposing = _strongest_wall(liquidity, opposing_side)
    if support is None and opposing is None:
        return 5.0
    if support is not None and opposing is None:
        return 10.0
    if support is None:
        return 0.0
    if opposing is None or support + opposing == 0:
        return 5.0
    return support / (support + opposing) * 10.0


def _orderbook_raw(
    metrics: OrderBookMetrics,
    sign: float,
    config: ScalperScoringConfig,
) -> float:
    score = 0.0
    for value, weight in (
        (metrics.imbalance_l1, 25.0),
        (metrics.imbalance_l5, 30.0),
        (metrics.imbalance_l10, 25.0),
    ):
        score += _optional_alignment(value, sign) * weight
    depth_pressure = _depth_pressure(metrics)
    score += _optional_alignment(depth_pressure, sign) * 10.0
    if metrics.spread_bps is not None:
        spread_quality = max(0.0, 1 - metrics.spread_bps / config.maximum_spread_bps)
        score += spread_quality * 10.0
    return score


def _market_state_raw(
    market_state: MarketStateAssessment,
    direction: ScalperDirection,
) -> float:
    if direction is ScalperDirection.NONE or market_state.bias is MarketBias.UNKNOWN:
        return 0.0
    if market_state.bias is MarketBias.NEUTRAL:
        return 50.0
    aligned = (
        direction is ScalperDirection.LONG
        and market_state.bias is MarketBias.BULLISH
    ) or (
        direction is ScalperDirection.SHORT
        and market_state.bias is MarketBias.BEARISH
    )
    confidence = market_state.confidence / 100.0
    if aligned:
        return 70.0 + 30.0 * confidence
    return 30.0 * (1.0 - confidence)


def _setup_raw(opportunity: ShadowOpportunity) -> float:
    base = {
        ShadowOpportunityDecision.SHADOW_CANDIDATE: 40.0,
        ShadowOpportunityDecision.WATCH: 20.0,
        ShadowOpportunityDecision.CONFLICTED: 5.0,
        ShadowOpportunityDecision.BLOCKED: 0.0,
    }[opportunity.decision]
    return min(100.0, base + opportunity.opportunity_score * 0.60)


def _component_total(scores: ScalperComponentScores) -> float:
    total = (
        scores.trade_flow_score
        + scores.liquidity_score
        + scores.orderbook_score
        + scores.market_state_score
        + scores.setup_score
        + scores.risk_score
    )
    return max(0.0, min(100.0, total))


def _confidence(
    market_state: MarketStateAssessment,
    liquidity: LiquidityIntelligence,
    opportunity: ShadowOpportunity,
    risk: RiskContext,
) -> float:
    value = (
        market_state.confidence * 0.35
        + liquidity.pressure.confidence * 0.25
        + opportunity.confidence * 0.20
        + (100.0 - risk.score) * 0.20
    )
    return max(0.0, min(100.0, value))


def _decision(
    total: float,
    confidence: float,
    direction: ScalperDirection,
    hard_gates: tuple[str, ...],
    config: ScalperScoringConfig,
) -> ScalperDecision:
    if hard_gates:
        return ScalperDecision.BLOCKED
    if direction is ScalperDirection.NONE:
        return ScalperDecision.WATCH
    if total >= config.strong_scalp_score and confidence >= config.strong_confidence:
        return ScalperDecision.STRONG_SCALP
    if total >= config.scalp_score and confidence >= config.scalp_confidence:
        return ScalperDecision.SCALP
    return ScalperDecision.WATCH


def _component_reasons(
    scores: ScalperComponentScores,
    direction: ScalperDirection,
    market_state: MarketStateAssessment,
    liquidity: LiquidityIntelligence,
) -> tuple[str, ...]:
    reasons = (
        f"💹 Trade Flow ({direction.value}): +{scores.trade_flow_score:.1f}",
        f"💧 Liquidity: +{scores.liquidity_score:.1f}",
        f"📚 OrderBook: +{scores.orderbook_score:.1f}",
        (
            f"🧠 Market State ({market_state.state.value}): "
            f"+{scores.market_state_score:.1f}"
        ),
        f"🎯 Setup Context: +{scores.setup_score:.1f}",
        f"⚠️ Risk Context: {scores.risk_score:.1f}",
    )
    semantic: list[str] = []
    if liquidity.sweep.detected:
        semantic.append(
            f"Liquidity sweep: {liquidity.sweep.direction.value}, "
            f"score {liquidity.sweep.score:.1f}."
        )
    semantic.extend(market_state.reasons[:1])
    return _unique((*reasons, *semantic))


def _validate_symbols(
    market_state: MarketStateAssessment,
    liquidity: LiquidityIntelligence,
    trade_flow: TradeFlowMetrics,
    orderbook: OrderBookMetrics,
    opportunity: ShadowOpportunity,
) -> None:
    symbols = {
        market_state.symbol,
        liquidity.symbol,
        trade_flow.symbol,
        orderbook.symbol,
        opportunity.symbol,
    }
    if len(symbols) != 1:
        raise ValueError("Scalper scoring inputs must use the same symbol.")


def _trade_age_ms(
    market_state: MarketStateAssessment,
    trade_flow: TradeFlowMetrics,
) -> float | None:
    if market_state.metrics.trade_age_ms is not None:
        return market_state.metrics.trade_age_ms
    if trade_flow.last_trade_at is None:
        return None
    value = (
        market_state.assessed_at - trade_flow.last_trade_at
    ).total_seconds() * 1_000
    return max(0.0, value)


def _depth_pressure(metrics: OrderBookMetrics) -> float | None:
    bid = metrics.bid_depth_10bps
    ask = metrics.ask_depth_10bps
    if bid is None or ask is None or bid + ask == 0:
        return None
    return (bid - ask) / (bid + ask)


def _strongest_wall(
    liquidity: LiquidityIntelligence,
    side: BookSide,
) -> float | None:
    walls = liquidity.bid_walls if side is BookSide.BID else liquidity.ask_walls
    return max((wall.strength_ratio for wall in walls), default=None)


def _direction(value: ShadowDirection) -> ScalperDirection:
    if value is ShadowDirection.LONG:
        return ScalperDirection.LONG
    if value is ShadowDirection.SHORT:
        return ScalperDirection.SHORT
    return ScalperDirection.NONE


def _direction_sign(direction: ScalperDirection) -> float:
    if direction is ScalperDirection.LONG:
        return 1.0
    if direction is ScalperDirection.SHORT:
        return -1.0
    return 0.0


def _sign_alignment(value: float, sign: float) -> float:
    if sign == 0 or value == 0:
        return 0.5
    return 1.0 if value * sign > 0 else 0.0


def _optional_alignment(value: float | None, sign: float) -> float:
    if value is None:
        return 0.0
    return _sign_alignment(value, sign)


def _texts(name: str, values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(value.strip() for value in values)
    if any(not value for value in normalized):
        raise ValueError(f"{name} cannot be empty.")
    return tuple(dict.fromkeys(normalized))


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value.strip()))


def _finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _positive(value: float) -> bool:
    return _finite(value) and value > 0
