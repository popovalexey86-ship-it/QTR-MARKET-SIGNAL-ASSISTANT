from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    MarketBias,
    MarketStateAssessment,
)


class ShadowDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


class ShadowOpportunityDecision(StrEnum):
    SHADOW_CANDIDATE = "SHADOW_CANDIDATE"
    WATCH = "WATCH"
    CONFLICTED = "CONFLICTED"
    BLOCKED = "BLOCKED"


class RiskLevel(StrEnum):
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    BLOCKED = "BLOCKED"


class RiskFactor(StrEnum):
    DATA_NOT_READY = "DATA_NOT_READY"
    MARKET_CONFLICT = "MARKET_CONFLICT"
    EXCESSIVE_EXTENSION = "EXCESSIVE_EXTENSION"
    STOP_TOO_TIGHT = "STOP_TOO_TIGHT"
    STOP_TOO_WIDE = "STOP_TOO_WIDE"
    WIDE_SPREAD = "WIDE_SPREAD"
    OPPOSING_LIQUIDITY = "OPPOSING_LIQUIDITY"
    STRUCTURE_INVALID = "STRUCTURE_INVALID"


@dataclass(frozen=True, slots=True)
class PriceContext:
    symbol: str
    assessed_at: datetime
    direction: ShadowDirection
    market_price: float
    atr: float
    trigger_price: float
    invalidation_price: float
    local_range_low: float | None = None
    local_range_high: float | None = None
    ready: bool = True
    health_reasons: tuple[str, ...] = ()
    confirmations: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Price context symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "assessed_at", _utc(self.assessed_at))
        if not isinstance(self.direction, ShadowDirection):
            raise ValueError("Price context direction is invalid.")
        for name, value in (
            ("market_price", self.market_price),
            ("atr", self.atr),
            ("trigger_price", self.trigger_price),
            ("invalidation_price", self.invalidation_price),
        ):
            if not _positive(value):
                raise ValueError(f"Price context {name} must be positive.")
        if (self.local_range_low is None) != (self.local_range_high is None):
            raise ValueError("Price context local range requires both boundaries.")
        if self.local_range_low is not None and self.local_range_high is not None:
            if not _positive(self.local_range_low) or not _positive(
                self.local_range_high
            ):
                raise ValueError("Price context local range must be positive.")
            if self.local_range_low >= self.local_range_high:
                raise ValueError("Price context local range is inverted.")
        health = _texts("Price context health reason", self.health_reasons)
        confirmations = _texts("Price context confirmation", self.confirmations)
        warnings = _texts("Price context warning", self.warnings)
        object.__setattr__(self, "health_reasons", health)
        object.__setattr__(self, "confirmations", confirmations)
        object.__setattr__(self, "warnings", warnings)
        if self.ready and health:
            raise ValueError("Ready price context cannot contain health reasons.")
        if not self.ready and not health:
            raise ValueError("Not-ready price context must explain its state.")

    @property
    def trigger_progress_atr(self) -> float | None:
        """Positive after trigger in the intended direction; negative before it."""

        if self.direction is ShadowDirection.LONG:
            return (self.market_price - self.trigger_price) / self.atr
        if self.direction is ShadowDirection.SHORT:
            return (self.trigger_price - self.market_price) / self.atr
        return None

    @property
    def stop_distance_atr(self) -> float | None:
        if self.direction is ShadowDirection.NEUTRAL:
            return None
        return abs(self.market_price - self.invalidation_price) / self.atr

    @property
    def structure_valid(self) -> bool:
        if self.direction is ShadowDirection.LONG:
            return self.invalidation_price < self.market_price
        if self.direction is ShadowDirection.SHORT:
            return self.invalidation_price > self.market_price
        return True

    @property
    def range_position(self) -> float | None:
        if self.local_range_low is None or self.local_range_high is None:
            return None
        width = self.local_range_high - self.local_range_low
        return (self.market_price - self.local_range_low) / width


@dataclass(frozen=True, slots=True)
class SetupContextEngineConfig:
    maximum_spread_bps: float = 8.0
    maximum_extension_atr: float = 0.50
    minimum_stop_distance_atr: float = 0.25
    maximum_stop_distance_atr: float = 2.0
    shadow_candidate_score: float = 60.0

    def __post_init__(self) -> None:
        values = (
            self.maximum_spread_bps,
            self.maximum_extension_atr,
            self.minimum_stop_distance_atr,
            self.maximum_stop_distance_atr,
            self.shadow_candidate_score,
        )
        if any(not _positive(value) for value in values):
            raise ValueError("Setup context thresholds must be positive.")
        if self.minimum_stop_distance_atr >= self.maximum_stop_distance_atr:
            raise ValueError("Setup context stop distance range is inverted.")
        if self.shadow_candidate_score > 100:
            raise ValueError("Shadow candidate score cannot exceed 100.")


@dataclass(frozen=True, slots=True)
class RiskContext:
    level: RiskLevel
    score: float
    factors: tuple[RiskFactor, ...]
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not 0.0 <= self.score <= 100.0:
            raise ValueError("Risk context score must be between 0 and 100.")


@dataclass(frozen=True, slots=True)
class ShadowOpportunity:
    symbol: str
    assessed_at: datetime
    direction: ShadowDirection
    decision: ShadowOpportunityDecision
    opportunity_score: float
    confidence: float
    market_alignment: float | None
    market_state: MarketStateAssessment
    price_context: PriceContext
    risk: RiskContext
    reasons: tuple[str, ...]
    confirmations: tuple[str, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        if not 0.0 <= self.opportunity_score <= 100.0:
            raise ValueError("Shadow opportunity score must be between 0 and 100.")
        if not 0.0 <= self.confidence <= 100.0:
            raise ValueError("Shadow opportunity confidence must be between 0 and 100.")
        if self.market_alignment is not None and not (
            -1.0 <= self.market_alignment <= 1.0
        ):
            raise ValueError("Market alignment must be between -1 and 1.")
        if not self.reasons:
            raise ValueError("Shadow opportunity requires explainable reasons.")


class SetupContextEngine:
    """Combine market state and price context for shadow analysis only."""

    def __init__(self, config: SetupContextEngineConfig | None = None) -> None:
        self._config = config or SetupContextEngineConfig()

    def analyze(
        self,
        market_state: MarketStateAssessment,
        price_context: PriceContext,
    ) -> ShadowOpportunity:
        _validate_inputs(market_state, price_context)
        alignment = _alignment(price_context.direction, market_state.bias)
        risk = _risk_context(market_state, price_context, alignment, self._config)
        score = _opportunity_score(
            market_state,
            price_context,
            alignment,
            risk,
            self._config,
        )
        decision = _decision(
            market_state,
            price_context,
            alignment,
            risk,
            score,
            self._config,
        )
        reasons = _opportunity_reasons(
            market_state,
            price_context,
            alignment,
            decision,
        )
        confirmations = _unique(
            (
                *market_state.confirmations,
                *price_context.confirmations,
                *_alignment_confirmations(alignment),
            )
        )
        warnings = _unique(
            (
                *market_state.warnings,
                *price_context.warnings,
                *risk.reasons,
            )
        )
        return ShadowOpportunity(
            symbol=price_context.symbol,
            assessed_at=price_context.assessed_at,
            direction=price_context.direction,
            decision=decision,
            opportunity_score=score,
            confidence=_confidence(market_state, risk),
            market_alignment=alignment,
            market_state=market_state,
            price_context=price_context,
            risk=risk,
            reasons=reasons,
            confirmations=confirmations,
            warnings=warnings,
        )


def simulate_setup_context(
    market_state: MarketStateAssessment,
    price_context: PriceContext,
    *,
    config: SetupContextEngineConfig | None = None,
) -> ShadowOpportunity:
    """Offline entry point sharing the exact shadow-analysis implementation."""

    return SetupContextEngine(config).analyze(market_state, price_context)


def _risk_context(
    market_state: MarketStateAssessment,
    price: PriceContext,
    alignment: float | None,
    config: SetupContextEngineConfig,
) -> RiskContext:
    factors: list[RiskFactor] = []
    reasons: list[str] = []
    score = 0.0
    blocked = False

    if not market_state.ready or not price.ready:
        factors.append(RiskFactor.DATA_NOT_READY)
        reasons.append("Market State или Price Context не готов к анализу.")
        blocked = True
    if not price.structure_valid:
        factors.append(RiskFactor.STRUCTURE_INVALID)
        reasons.append("Structural invalidation расположен с неверной стороны цены.")
        blocked = True
    if alignment is not None and alignment < 0:
        factors.append(RiskFactor.MARKET_CONFLICT)
        reasons.append("Market State противоречит направлению Price Context.")
        score += 50

    extension = price.trigger_progress_atr
    if extension is not None and extension > config.maximum_extension_atr:
        factors.append(RiskFactor.EXCESSIVE_EXTENSION)
        reasons.append("Цена слишком далеко ушла от trigger относительно ATR.")
        score += min(30.0, extension / config.maximum_extension_atr * 15)

    stop_distance = price.stop_distance_atr
    if stop_distance is not None and stop_distance < config.minimum_stop_distance_atr:
        factors.append(RiskFactor.STOP_TOO_TIGHT)
        reasons.append("Structural invalidation слишком близок относительно ATR.")
        score += 25
    if stop_distance is not None and stop_distance > config.maximum_stop_distance_atr:
        factors.append(RiskFactor.STOP_TOO_WIDE)
        reasons.append("Structural invalidation слишком далёк относительно ATR.")
        score += 30

    spread = market_state.metrics.spread_bps
    if spread is None:
        factors.append(RiskFactor.DATA_NOT_READY)
        reasons.append("Spread недоступен.")
        blocked = True
    elif spread > config.maximum_spread_bps:
        factors.append(RiskFactor.WIDE_SPREAD)
        reasons.append("Spread превышает shadow-safe порог.")
        score += 30

    if market_state.warnings:
        factors.append(RiskFactor.OPPOSING_LIQUIDITY)
        reasons.append("Market State содержит предупреждение о встречной ликвидности.")
        score += 10

    score = min(100.0, score)
    if blocked:
        level = RiskLevel.BLOCKED
        score = 100.0
    elif score >= 60:
        level = RiskLevel.HIGH
    elif score >= 25:
        level = RiskLevel.MODERATE
    else:
        level = RiskLevel.LOW
    return RiskContext(
        level=level,
        score=score,
        factors=tuple(dict.fromkeys(factors)),
        reasons=_unique(tuple(reasons)),
    )


def _opportunity_score(
    market_state: MarketStateAssessment,
    price: PriceContext,
    alignment: float | None,
    risk: RiskContext,
    config: SetupContextEngineConfig,
) -> float:
    if risk.level is RiskLevel.BLOCKED:
        return 0.0
    if price.direction is ShadowDirection.NEUTRAL:
        return max(0.0, 25.0 - risk.score * 0.25)

    if alignment is not None and alignment > 0:
        market_component = 20 + 20 * abs(market_state.directional_score)
    elif alignment == 0:
        market_component = 15.0
    else:
        market_component = 0.0

    extension = price.trigger_progress_atr
    if extension is None:
        trigger_component = 0.0
    elif extension > config.maximum_extension_atr:
        trigger_component = 5.0
    elif extension >= -0.25:
        trigger_component = 20.0
    else:
        trigger_component = max(0.0, 20 + extension * 20)

    structure_component = 15.0 if price.structure_valid else 0.0
    confidence_component = market_state.confidence * 0.15
    risk_penalty = risk.score * 0.25
    raw = (
        market_component
        + trigger_component
        + structure_component
        + confidence_component
        - risk_penalty
    )
    return max(0.0, min(100.0, raw))


def _decision(
    market_state: MarketStateAssessment,
    price: PriceContext,
    alignment: float | None,
    risk: RiskContext,
    score: float,
    config: SetupContextEngineConfig,
) -> ShadowOpportunityDecision:
    if risk.level is RiskLevel.BLOCKED:
        return ShadowOpportunityDecision.BLOCKED
    if price.direction is ShadowDirection.NEUTRAL:
        return ShadowOpportunityDecision.WATCH
    if alignment is not None and alignment < 0:
        return ShadowOpportunityDecision.CONFLICTED
    if risk.level is RiskLevel.HIGH:
        return ShadowOpportunityDecision.CONFLICTED
    if score >= config.shadow_candidate_score:
        return ShadowOpportunityDecision.SHADOW_CANDIDATE
    return ShadowOpportunityDecision.WATCH


def _opportunity_reasons(
    market_state: MarketStateAssessment,
    price: PriceContext,
    alignment: float | None,
    decision: ShadowOpportunityDecision,
) -> tuple[str, ...]:
    reasons = [f"Market State: {market_state.state.value}."]
    if price.direction is ShadowDirection.NEUTRAL:
        reasons.append("Price Context не задаёт направленный setup.")
    elif alignment is not None and alignment > 0:
        reasons.append("Market State подтверждает направление Price Context.")
    elif alignment is not None and alignment < 0:
        reasons.append("Market State конфликтует с направлением Price Context.")
    else:
        reasons.append("Market State не даёт направленного подтверждения.")
    reasons.append(f"Shadow decision: {decision.value}.")
    return _unique(tuple(reasons))


def _alignment(
    direction: ShadowDirection,
    bias: MarketBias,
) -> float | None:
    if bias is MarketBias.UNKNOWN:
        return None
    if direction is ShadowDirection.NEUTRAL or bias is MarketBias.NEUTRAL:
        return 0.0
    bullish = direction is ShadowDirection.LONG
    aligned = (bullish and bias is MarketBias.BULLISH) or (
        not bullish and bias is MarketBias.BEARISH
    )
    return 1.0 if aligned else -1.0


def _alignment_confirmations(alignment: float | None) -> tuple[str, ...]:
    if alignment is not None and alignment > 0:
        return ("Market State и Price Context направлены согласованно.",)
    return ()


def _confidence(market_state: MarketStateAssessment, risk: RiskContext) -> float:
    if risk.level is RiskLevel.BLOCKED:
        return 0.0
    return max(0.0, min(100.0, market_state.confidence * (1 - risk.score / 100)))


def _validate_inputs(
    market_state: MarketStateAssessment,
    price: PriceContext,
) -> None:
    if market_state.symbol != price.symbol:
        raise ValueError("Setup context inputs must use the same symbol.")
    if market_state.assessed_at != price.assessed_at:
        raise ValueError("Setup context inputs must use the same assessed_at.")


def _texts(name: str, values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(value.strip() for value in values)
    if any(not value for value in normalized):
        raise ValueError(f"{name} cannot be empty.")
    return tuple(dict.fromkeys(normalized))


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value.strip()))


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Price context assessed_at must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError("Price context assessed_at must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _positive(value: float) -> bool:
    return _finite(value) and value > 0
