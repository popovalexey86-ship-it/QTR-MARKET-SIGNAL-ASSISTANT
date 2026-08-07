from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.inplay.models import InPlayDirection, InPlayResult
from market_signal_assistant.inplay.safety import (
    AutomaticDisplayStatus,
    AutomaticRiskClass,
    automatic_semantics,
)


def _result(change: float, *, direction: InPlayDirection) -> InPlayResult:
    rendered = f"{change:+.1f}".replace(".", ",")
    return InPlayResult(
        symbol="TESTUSDT",
        direction=direction,
        inplay_score=80.0,
        directional_score=75.0,
        reasons=(f"Изменение цены {rendered}%",),
        warnings=(),
        first_seen=datetime(2026, 8, 1, tzinfo=UTC) - timedelta(days=30),
        is_new_listing=False,
    )


@pytest.mark.parametrize(
    ("change", "direction", "expected_status", "expected_risk"),
    (
        (
            14.9,
            InPlayDirection.LONG,
            AutomaticDisplayStatus.LONG,
            AutomaticRiskClass.NORMAL,
        ),
        (
            15.0,
            InPlayDirection.LONG,
            AutomaticDisplayStatus.LATE_ENTRY,
            AutomaticRiskClass.LATE_ENTRY,
        ),
        (
            -15.0,
            InPlayDirection.SHORT,
            AutomaticDisplayStatus.LATE_ENTRY,
            AutomaticRiskClass.LATE_ENTRY,
        ),
        (
            29.9,
            InPlayDirection.LONG,
            AutomaticDisplayStatus.LATE_ENTRY,
            AutomaticRiskClass.LATE_ENTRY,
        ),
        (
            30.0,
            InPlayDirection.LONG,
            AutomaticDisplayStatus.DO_NOT_CHASE,
            AutomaticRiskClass.EXTREME,
        ),
        (
            -30.0,
            InPlayDirection.SHORT,
            AutomaticDisplayStatus.DO_NOT_CHASE,
            AutomaticRiskClass.EXTREME,
        ),
    ),
)
def test_automatic_safety_boundaries_are_symmetric(
    change: float,
    direction: InPlayDirection,
    expected_status: AutomaticDisplayStatus,
    expected_risk: AutomaticRiskClass,
) -> None:
    semantics = automatic_semantics(_result(change, direction=direction))

    assert semantics.display_status is expected_status
    assert semantics.risk_class is expected_risk
    assert semantics.internal_direction is direction


def test_significant_warning_blocks_direction_without_numeric_metric() -> None:
    item = _result(1.0, direction=InPlayDirection.LONG)
    item = InPlayResult(
        symbol=item.symbol,
        direction=item.direction,
        inplay_score=item.inplay_score,
        directional_score=item.directional_score,
        reasons=("Относительный объём 2,0×",),
        warnings=(
            "Движение уже значительно реализовано; повышен риск отката.",
        ),
        first_seen=item.first_seen,
        is_new_listing=item.is_new_listing,
    )

    semantics = automatic_semantics(item)

    assert semantics.display_status is AutomaticDisplayStatus.LATE_ENTRY
    assert semantics.directional_entry_allowed is False
