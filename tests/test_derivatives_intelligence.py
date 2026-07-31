from datetime import UTC, datetime

import pytest

from market_signal_assistant.derivatives.intelligence import (
    DerivativesIntelligence,
)
from market_signal_assistant.derivatives.models import (
    DerivativesSnapshot,
    MarketPositioning,
)

NOW = datetime(2026, 7, 31, tzinfo=UTC)


def snapshot(**changes: float) -> DerivativesSnapshot:
    values = {
        "funding_rate": 0.0,
        "open_interest": 1_000.0,
        "open_interest_change": 0.0,
        "price_change": 0.0,
        "volume_change": 0.0,
        "long_liquidations": 0.0,
        "short_liquidations": 0.0,
    }
    values.update(changes)
    return DerivativesSnapshot(
        provider="test",
        symbol="BTCUSDT",
        as_of=NOW,
        **values,
    )


@pytest.mark.parametrize(
    ("observations", "expected"),
    [
        (
            {"price_change": 0.01, "open_interest_change": 0.02,
             "volume_change": 0.20},
            MarketPositioning.SUSTAINABLE_GROWTH,
        ),
        (
            {"price_change": 0.01, "open_interest_change": 0.02,
             "funding_rate": 0.0005},
            MarketPositioning.OVERHEATED_LONG,
        ),
        (
            {"price_change": 0.0, "open_interest_change": 0.02,
             "funding_rate": -0.0005},
            MarketPositioning.SHORT_ACCUMULATION,
        ),
        (
            {"price_change": 0.01, "open_interest_change": -0.02,
             "short_liquidations": 200.0, "long_liquidations": 100.0},
            MarketPositioning.SHORT_SQUEEZE,
        ),
        (
            {"price_change": -0.01, "open_interest_change": -0.02,
             "long_liquidations": 200.0, "short_liquidations": 100.0},
            MarketPositioning.LONG_SQUEEZE,
        ),
        (
            {"price_change": 0.01, "open_interest_change": 0.0199},
            MarketPositioning.UNCONFIRMED_MOVE,
        ),
        ({}, MarketPositioning.NEUTRAL),
    ],
)
def test_all_regimes_and_inclusive_threshold_boundaries(
    observations: dict[str, float], expected: MarketPositioning
) -> None:
    result = DerivativesIntelligence().analyze(snapshot(**observations))
    assert result.regime is expected
    assert result.reasons
    assert -1 <= result.directional_score <= 1
    assert 0 <= result.confidence <= 100


def test_just_below_price_threshold_remains_neutral() -> None:
    result = DerivativesIntelligence().analyze(
        snapshot(price_change=0.0099, open_interest_change=0.0)
    )
    assert result.regime is MarketPositioning.NEUTRAL
