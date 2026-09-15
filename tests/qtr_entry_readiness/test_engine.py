from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from market_signal_assistant.providers import PublicPriceQuote
from market_signal_assistant.qtr_entry_readiness.engine import (
    EntryReadinessEngine,
    setup_episode_key,
)
from market_signal_assistant.qtr_entry_readiness.models import (
    EntryReadinessConfig,
    EntryReadinessEvaluation,
    InternalDisposition,
    InternalReason,
    UserReadiness,
    WaitReason,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.qtr_setup_pilot.notifications import (
    qtr_telegram_quality_score,
)
from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupDirection,
    SetupState,
    SetupType,
    TradeEligibility,
)

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


def _source(**changes: Any) -> SetupAnalysisInput:
    baseline = SetupAnalysisInput(
        snapshot_ids=("scan-1",),
        source="early_discovery_v2",
        symbol="BTCUSDT",
        analyzed_at=NOW,
        direction=SetupDirection.UP,
        current_price=101.0,
        trigger_level=100.0,
        invalidation_level=98.0,
        distance_to_trigger_pct=1.0,
        distance_to_trigger_atr=0.5,
        breakout_age_bars=1,
        hold_candles=2,
        breakout_confirmed=True,
        correct_side_of_level=True,
        retest_detected=False,
        retest_held=False,
        breakout_failed=False,
        volume_confirmation=True,
        volatility_confirmation=True,
        structure_confirmation=True,
        liquidity_ok=True,
        spread_pct=0.1,
        compression_detected=False,
        continuation_detected=False,
        reversal_detected=False,
        conflicting_confirmations=False,
        current_breakout_failure=False,
        technical_data_complete=True,
        completed_candles=3,
    )
    return replace(baseline, **changes)


def candidate(
    *,
    setup_type: SetupType = SetupType.BREAKOUT,
    direction: SetupDirection = SetupDirection.UP,
    confirmed: bool = True,
    analyzed_at: datetime = NOW,
    atr: float | None = 2.0,
    trigger: float | None = 100.0,
    invalidation: float | None = None,
    source_changes: dict[str, Any] | None = None,
    result_changes: dict[str, Any] | None = None,
    episode_id: str = "2026-09-15T11:59:00+00:00",
) -> QtrSetupCandidate:
    is_long = direction is SetupDirection.UP
    invalidation_value = (
        invalidation if invalidation is not None else (98.0 if is_long else 102.0)
    )
    source_values: dict[str, Any] = {
        "analyzed_at": analyzed_at,
        "direction": direction,
        "trigger_level": trigger,
        "invalidation_level": invalidation_value,
        "current_price": 101.0 if is_long else 99.0,
        "correct_side_of_level": True,
        "breakout_confirmed": confirmed,
        "retest_detected": setup_type is SetupType.RETEST,
        "retest_held": confirmed if setup_type is SetupType.RETEST else False,
        "structure_confirmation": confirmed,
        "volume_confirmation": confirmed,
    }
    source_values.update(source_changes or {})
    source = _source(**source_values)
    result = analyze_setup(source)
    result_values: dict[str, Any] = {
        "direction": direction,
        "setup_type": setup_type,
        "setup_state": (
            SetupState.READY_TO_CONSIDER if confirmed else SetupState.CONFIRMING
        ),
        "trigger_level": trigger,
        "invalidation_level": invalidation_value,
        "retest_held": confirmed if setup_type is SetupType.RETEST else False,
        "volume_confirmation": confirmed,
        "structure_confirmation": confirmed,
        "freshness_confirmation": True,
        "liquidity_ok": True,
        "spread_ok": True,
        "is_late": False,
        "current_breakout_failure": False,
        "trade_eligible": confirmed,
        "trade_eligibility": (
            TradeEligibility.READY_TO_CONSIDER
            if confirmed
            else TradeEligibility.CONFIRMING
        ),
        "missing_data": (),
        "data_quality": "COMPLETE",
        "technical_gap": False,
    }
    result_values.update(result_changes or {})
    result = replace(result, **result_values)
    return QtrSetupCandidate(
        episode_id,
        source,
        result,
        atr_value=atr,
        local_range_low=98.0,
        local_range_high=102.0,
    )


def quote(price: float, at: datetime = NOW) -> PublicPriceQuote:
    return PublicPriceQuote("BTCUSDT", price, at)


def evaluate(
    item: QtrSetupCandidate,
    price: float | None,
    *,
    at: datetime = NOW,
    quote_at: datetime | None = None,
    confirmation_observed_at: datetime | None = None,
) -> EntryReadinessEvaluation:
    market = None if price is None else quote(price, quote_at or at)
    return EntryReadinessEngine(EntryReadinessConfig()).evaluate(
        item,
        market,
        at,
        first_confirmation_observed_at=confirmation_observed_at,
    )


def test_user_readiness_has_only_wait_and_now() -> None:
    assert tuple(item.value for item in UserReadiness) == ("WAIT", "NOW")


def test_internal_suppression_does_not_create_third_user_status() -> None:
    result = evaluate(candidate(direction=SetupDirection.NEUTRAL), 100.0)

    assert result.user_readiness is None
    assert result.internal_disposition is InternalDisposition.SUPPRESSED
    assert result.internal_reason is InternalReason.INVALID_DIRECTION


def test_retest_unconfirmed_waits_for_confirmation() -> None:
    result = evaluate(candidate(setup_type=SetupType.RETEST, confirmed=False), 103.0)

    assert result.user_readiness is UserReadiness.WAIT
    assert result.wait_reason is WaitReason.CONFIRMATION_PENDING


def test_confirmation_pending_precedes_missing_fresh_price() -> None:
    result = evaluate(candidate(setup_type=SetupType.RETEST, confirmed=False), None)

    assert result.user_readiness is UserReadiness.WAIT
    assert result.wait_reason is WaitReason.CONFIRMATION_PENDING
    assert result.internal_reason is None


def test_retest_held_and_in_zone_is_now() -> None:
    result = evaluate(candidate(setup_type=SetupType.RETEST), 100.2)

    assert result.user_readiness is UserReadiness.NOW
    assert result.wait_reason is None


def test_retest_held_but_too_far_waits_for_price() -> None:
    result = evaluate(candidate(setup_type=SetupType.RETEST), 100.6)

    assert result.user_readiness is UserReadiness.WAIT
    assert result.wait_reason is WaitReason.PRICE_NOT_IN_ENTRY_ZONE


def test_breakout_unconfirmed_waits() -> None:
    result = evaluate(candidate(confirmed=False), 100.2)

    assert result.user_readiness is UserReadiness.WAIT
    assert result.wait_reason is WaitReason.CONFIRMATION_PENDING


def test_breakout_confirmed_with_volume_and_in_zone_is_now() -> None:
    assert evaluate(candidate(), 100.2).user_readiness is UserReadiness.NOW


def test_breakout_without_volume_waits_for_confirmation() -> None:
    item = candidate(result_changes={"volume_confirmation": False})

    result = evaluate(item, 100.2)

    assert result.user_readiness is UserReadiness.WAIT
    assert result.wait_reason is WaitReason.CONFIRMATION_PENDING


def test_breakout_confirmed_but_too_far_waits_for_price() -> None:
    result = evaluate(candidate(), 100.51)

    assert result.user_readiness is UserReadiness.WAIT
    assert result.wait_reason is WaitReason.PRICE_NOT_IN_ENTRY_ZONE


def test_now_uses_fresh_price_instead_of_signal_price() -> None:
    item = candidate(result_changes={"current_price": 110.0})

    result = evaluate(item, 100.2)

    assert result.user_readiness is UserReadiness.NOW
    assert result.signal_price == 110.0
    assert result.fresh_price == 100.2


@pytest.mark.parametrize(
    ("direction", "price", "expected_low", "expected_high"),
    (
        (SetupDirection.UP, 100.5, 100.0, 100.5),
        (SetupDirection.DOWN, 99.5, 99.5, 100.0),
    ),
)
def test_long_and_short_correct_side_boundary_is_inclusive(
    direction: SetupDirection,
    price: float,
    expected_low: float,
    expected_high: float,
) -> None:
    result = evaluate(candidate(direction=direction), price)

    assert result.user_readiness is UserReadiness.NOW
    assert result.entry_zone_low == expected_low
    assert result.entry_zone_high == expected_high
    assert result.fresh_distance_atr == pytest.approx(0.25)
    assert result.direction == (
        "LONG" if direction is SetupDirection.UP else "SHORT"
    )


@pytest.mark.parametrize(
    ("direction", "price"),
    ((SetupDirection.UP, 99.99), (SetupDirection.DOWN, 100.01)),
)
def test_wrong_side_is_internally_suppressed(
    direction: SetupDirection, price: float
) -> None:
    result = evaluate(candidate(direction=direction), price)

    assert result.user_readiness is None
    assert result.internal_reason is InternalReason.WRONG_SIDE


def test_just_above_quarter_atr_waits() -> None:
    result = evaluate(candidate(), 100.500_001)

    assert result.user_readiness is UserReadiness.WAIT
    assert result.wait_reason is WaitReason.PRICE_NOT_IN_ENTRY_ZONE


def test_confirmation_age_of_exactly_sixty_seconds_is_allowed() -> None:
    result = evaluate(
        candidate(analyzed_at=NOW + timedelta(seconds=55)),
        100.2,
        at=NOW + timedelta(seconds=60),
        confirmation_observed_at=NOW,
    )

    assert result.user_readiness is UserReadiness.NOW
    assert result.confirmation_age_seconds == 60.0


def test_confirmation_older_than_sixty_seconds_is_suppressed() -> None:
    result = evaluate(
        candidate(analyzed_at=NOW),
        100.2,
        at=NOW + timedelta(seconds=60, microseconds=1),
        confirmation_observed_at=NOW,
    )

    assert result.user_readiness is None
    assert result.internal_reason is InternalReason.STALE_CONFIRMATION


@pytest.mark.parametrize(
    ("direction", "expected"),
    ((SetupDirection.UP, 97.7), (SetupDirection.DOWN, 102.3)),
)
def test_protective_geometry(direction: SetupDirection, expected: float) -> None:
    price = 100.2 if direction is SetupDirection.UP else 99.8
    result = evaluate(candidate(direction=direction), price)

    assert result.protective_level == pytest.approx(expected)
    assert result.risk_distance_atr is not None


@pytest.mark.parametrize(
    ("item", "price", "reason"),
    (
        (candidate(atr=None), 100.2, InternalReason.ATR_MISSING),
        (candidate(trigger=None), 100.2, InternalReason.TRIGGER_MISSING),
        (
            candidate(result_changes={"invalidation_level": None}),
            100.2,
            InternalReason.INVALIDATION_MISSING,
        ),
        (candidate(), None, InternalReason.FRESH_PRICE_MISSING),
    ),
)
def test_missing_data_is_fail_closed(
    item: QtrSetupCandidate,
    price: float | None,
    reason: InternalReason,
) -> None:
    result = evaluate(item, price)

    assert result.user_readiness is None
    assert result.internal_reason is reason


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        ({"current_breakout_failure": True}, InternalReason.CURRENT_FAILURE),
        ({"is_late": True}, InternalReason.LATE),
        ({"spread_ok": False}, InternalReason.SPREAD_BAD),
        ({"structure_confirmation": False}, InternalReason.STRUCTURE_INVALID),
    ),
)
def test_safety_gates_are_fail_closed(
    changes: dict[str, Any], reason: InternalReason
) -> None:
    result = evaluate(candidate(result_changes=changes), 100.2)

    assert result.user_readiness is None
    assert result.internal_reason is reason


def test_continuation_is_never_now() -> None:
    result = evaluate(candidate(setup_type=SetupType.CONTINUATION), 100.2)

    assert result.user_readiness is None
    assert result.internal_reason is InternalReason.UNSUPPORTED_SETUP


def test_quality_score_is_reused_without_modification() -> None:
    item = candidate()

    result = evaluate(item, 100.2)

    assert result.quality_score == qtr_telegram_quality_score(item) == 100.0


def test_stale_public_quote_is_fail_closed() -> None:
    evaluated_at = NOW + timedelta(seconds=61)
    result = evaluate(
        candidate(analyzed_at=evaluated_at),
        100.2,
        at=evaluated_at,
        quote_at=NOW,
    )

    assert result.user_readiness is None
    assert result.internal_reason is InternalReason.FRESH_PRICE_MISSING


def test_analyzed_at_is_not_used_as_confirmation_time() -> None:
    result = evaluate(
        candidate(analyzed_at=NOW - timedelta(hours=1)),
        100.2,
        at=NOW,
    )

    assert result.user_readiness is UserReadiness.NOW
    assert result.first_confirmation_observed_at == NOW
    assert result.confirmation_age_seconds == 0.0


def test_unassigned_episode_fallback_ignores_per_scan_observations() -> None:
    first = candidate(episode_id="unassigned")
    second = replace(
        first,
        source_input=replace(
            first.source_input,
            snapshot_ids=("scan-2",),
            analyzed_at=NOW + timedelta(minutes=5),
            current_price=100.4,
        ),
        result=replace(
            first.result,
            analyzed_at=NOW + timedelta(minutes=5),
            current_price=100.4,
        ),
    )

    assert setup_episode_key(first) == setup_episode_key(second)


def test_new_structural_breakout_has_new_episode_key() -> None:
    first = candidate(episode_id="unassigned", trigger=100.0)
    second = candidate(episode_id="unassigned", trigger=101.0)

    assert setup_episode_key(first) != setup_episode_key(second)


def test_new_breakout_level_splits_even_when_upstream_episode_id_is_unchanged() -> None:
    first = candidate(episode_id="stable-sequence", trigger=100.0)
    second = candidate(episode_id="stable-sequence", trigger=101.0)

    assert setup_episode_key(first) != setup_episode_key(second)
