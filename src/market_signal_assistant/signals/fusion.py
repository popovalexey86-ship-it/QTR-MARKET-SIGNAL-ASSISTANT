from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from market_signal_assistant.derivatives.models import (
    MarketPositioning,
    MarketPositioningSignal,
)
from market_signal_assistant.models import MarketSignal, SignalDirection


class FusionEffect(Enum):
    STRENGTHENED = "strengthened"
    WEAKENED = "weakened"
    NEUTRAL = "neutral"


@dataclass(frozen=True, slots=True)
class EnrichedMarketSignal:
    """Immutable, attributable fusion result.

    ``technical_score`` remains on the source 0..100 scale.
    ``derivatives_directional_score`` remains on -1..1.
    ``combined_score`` is a signed -100..100 scale: positive is bullish,
    negative is bearish, and magnitude is confidence-adjusted conviction.
    """

    technical_signal: MarketSignal
    technical_score: float
    derivatives_directional_score: float
    technical_confidence: float
    derivatives_confidence: float
    combined_score: float
    effect: FusionEffect
    derivatives_regime: MarketPositioning
    technical_explanations: tuple[str, ...]
    derivatives_explanations: tuple[str, ...]

    def __post_init__(self) -> None:
        if not -100.0 <= self.combined_score <= 100.0:
            raise ValueError("Combined score must be between -100 and 100.")


class SignalFusion:
    def __init__(self, derivatives_weight: float = 0.35) -> None:
        if not 0.0 <= derivatives_weight <= 1.0:
            raise ValueError("Derivatives weight must be between 0 and 1.")
        self._derivatives_weight = derivatives_weight

    def combine(
        self,
        technical: MarketSignal,
        derivatives: MarketPositioningSignal,
    ) -> EnrichedMarketSignal:
        technical_direction = (
            1.0 if technical.direction is SignalDirection.BULLISH else -1.0
        )
        technical_signed = (
            technical_direction * technical.score * technical.confidence / 100.0
        )
        derivatives_signed = (
            derivatives.directional_score * derivatives.confidence
        )
        combined = (
            technical_signed * (1.0 - self._derivatives_weight)
            + derivatives_signed * self._derivatives_weight
        )
        agreement = technical_direction * derivatives.directional_score
        if agreement > 0:
            effect = FusionEffect.STRENGTHENED
        elif agreement < 0:
            effect = FusionEffect.WEAKENED
        else:
            effect = FusionEffect.NEUTRAL
        return EnrichedMarketSignal(
            technical_signal=technical,
            technical_score=technical.score,
            derivatives_directional_score=derivatives.directional_score,
            technical_confidence=technical.confidence,
            derivatives_confidence=derivatives.confidence,
            combined_score=max(-100.0, min(100.0, combined)),
            effect=effect,
            derivatives_regime=derivatives.regime,
            technical_explanations=tuple(
                f"{item.name}: {item.detail}" for item in technical.evidence
            ),
            derivatives_explanations=derivatives.reasons,
        )
