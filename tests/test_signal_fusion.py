from copy import deepcopy
from datetime import UTC, datetime

import pytest

from market_signal_assistant.derivatives.models import (
    DerivativesSnapshot,
    MarketPositioning,
    MarketPositioningSignal,
)
from market_signal_assistant.models import (
    AssetClass,
    Instrument,
    MarketSignal,
    SignalDirection,
    SignalEvidence,
)
from market_signal_assistant.signals.fusion import FusionEffect, SignalFusion

NOW = datetime(2026, 7, 31, tzinfo=UTC)


def technical(direction: SignalDirection) -> MarketSignal:
    return MarketSignal(
        instrument=Instrument("BTCUSDT", AssetClass.CRYPTO),
        interval="5m",
        timestamp=NOW,
        direction=direction,
        score=80.0,
        confidence=75.0,
        confirmations=2,
        conflicts=0,
        price=100.0,
        evidence=(SignalEvidence("trend", direction, 25.0, "aligned trend"),),
    )


def derivatives(score: float) -> MarketPositioningSignal:
    return MarketPositioningSignal(
        regime=MarketPositioning.SUSTAINABLE_GROWTH,
        directional_score=score,
        confidence=80.0,
        snapshot=DerivativesSnapshot(
            provider="test",
            symbol="BTCUSDT",
            as_of=NOW,
            funding_rate=0.0,
            open_interest=100.0,
            open_interest_change=0.02,
            price_change=0.01,
            volume_change=0.2,
        ),
        reasons=("open_interest_up",),
    )


def test_matching_direction_strengthens_and_preserves_scales() -> None:
    result = SignalFusion().combine(
        technical(SignalDirection.BULLISH), derivatives(0.8)
    )
    assert result.effect is FusionEffect.STRENGTHENED
    assert result.technical_score == 80.0
    assert result.derivatives_directional_score == 0.8
    assert result.technical_confidence == 75.0
    assert result.derivatives_confidence == 80.0
    assert result.combined_score == pytest.approx(61.4)


def test_conflicting_direction_weakens() -> None:
    result = SignalFusion().combine(
        technical(SignalDirection.BULLISH), derivatives(-0.8)
    )
    assert result.effect is FusionEffect.WEAKENED
    assert result.combined_score == pytest.approx(16.6)


def test_neutral_derivatives_has_neutral_effect() -> None:
    result = SignalFusion().combine(
        technical(SignalDirection.BEARISH), derivatives(0.0)
    )
    assert result.effect is FusionEffect.NEUTRAL
    assert result.combined_score == pytest.approx(-39.0)


def test_fusion_does_not_mutate_technical_signal() -> None:
    source = technical(SignalDirection.BULLISH)
    before = deepcopy(source)
    result = SignalFusion().combine(source, derivatives(0.8))
    assert source == before
    assert result.technical_signal is source
    assert result.technical_explanations == ("trend: aligned trend",)
    assert result.derivatives_explanations == ("open_interest_up",)
