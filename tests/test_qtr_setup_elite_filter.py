from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import market_signal_assistant.qtr_setup_pilot.notifications as notifications_module
from market_signal_assistant.qtr_setup_pilot.audit import (
    JsonlQtrSetupTelegramAuditStore,
    QtrSetupAuditOutcome,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.qtr_setup_pilot.notifications import (
    JsonQtrSetupNotificationStore,
    QtrNotificationReason,
    QtrSetupNotificationService,
    QtrTelegramFilterPolicy,
    event_from_candidate,
    qtr_telegram_quality_score,
)
from market_signal_assistant.settings import QtrSetupTelegramSettings
from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupDirection,
    SetupState,
    SetupType,
)
from market_signal_assistant.telegram.qtr_setup_pilot import (
    QtrSetupPilotNotifier,
    format_qtr_setup_event,
)

NOW = datetime(2026, 8, 31, 9, 0, tzinfo=UTC)


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
        price_change_24h_pct=2.0,
        distance_to_trigger_pct=1.0,
        distance_to_trigger_atr=0.5,
        breakout_age_bars=1,
        hold_candles=2,
        breakout_confirmed=True,
        correct_side_of_level=True,
        returned_inside_range=False,
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
        completed_candles=3,
        technical_data_complete=True,
    )
    return replace(baseline, **changes)


def _candidate(
    *,
    episode: str = "episode-1",
    input_changes: dict[str, Any] | None = None,
    result_changes: dict[str, Any] | None = None,
) -> QtrSetupCandidate:
    source = _source(**(input_changes or {}))
    result = analyze_setup(source)
    if result_changes:
        result = replace(result, **result_changes)
    return QtrSetupCandidate(episode, source, result)


def _service(
    tmp_path: Path,
    *,
    minimum_quality: float = 90.0,
    maximum_distance_atr: float = 1.2,
) -> QtrSetupNotificationService:
    return QtrSetupNotificationService(
        JsonQtrSetupNotificationStore(tmp_path / "qtr_setup_notifications.json"),
        QtrTelegramFilterPolicy(minimum_quality, maximum_distance_atr),
    )


def _commit(
    service: QtrSetupNotificationService,
    item: QtrSetupCandidate,
    when: datetime = NOW,
) -> None:
    plan = service.prepare((item,), when)
    decision = plan.decisions[0]
    assert decision.should_notify
    service.commit(plan, frozenset((decision.decision_id,)))


def test_ready_candidate_at_threshold_is_sent(tmp_path: Path) -> None:
    item = _candidate(result_changes={"volatility_confirmation": False})
    decision = _service(tmp_path).prepare((item,), NOW).decisions[0]

    assert qtr_telegram_quality_score(item) == 90.0
    assert decision.should_notify is True


def test_ready_below_quality_threshold_is_suppressed(tmp_path: Path) -> None:
    item = _candidate(result_changes={"volume_confirmation": False})
    decision = _service(tmp_path).prepare((item,), NOW).decisions[0]

    assert qtr_telegram_quality_score(item) == 85.0
    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.QUALITY_BELOW_THRESHOLD


@pytest.mark.parametrize(
    "result_changes",
    (
        {"structure_confirmation": False},
        {"liquidity_ok": False},
        {"spread_ok": False},
        {"freshness_confirmation": False},
        {"setup_type": SetupType.RETEST, "retest_held": False},
    ),
)
def test_mandatory_quality_gate_failure_is_suppressed(
    tmp_path: Path,
    result_changes: dict[str, Any],
) -> None:
    decision = _service(tmp_path).prepare(
        (_candidate(result_changes=result_changes),), NOW
    ).decisions[0]

    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.QUALITY_BELOW_THRESHOLD


@pytest.mark.parametrize(
    "state",
    (
        SetupState.FORMING,
        SetupState.CONFIRMING,
        SetupState.WATCHING,
        SetupState.LATE,
        SetupState.CANCELLED,
    ),
)
def test_non_ready_status_is_suppressed(
    tmp_path: Path,
    state: SetupState,
) -> None:
    decision = _service(tmp_path).prepare(
        (_candidate(result_changes={"setup_state": state}),), NOW
    ).decisions[0]

    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.STATUS_SUPPRESSED


def test_watch_and_no_trade_are_suppressed(tmp_path: Path) -> None:
    neutral = _candidate(result_changes={"direction": SetupDirection.NEUTRAL})
    no_trade = _candidate(result_changes={"setup_type": SetupType.NO_TRADE})
    decisions = _service(tmp_path).prepare((neutral, no_trade), NOW).decisions

    assert all(not item.should_notify for item in decisions)
    assert all(
        item.reason is QtrNotificationReason.STATUS_SUPPRESSED
        for item in decisions
    )


def test_distance_above_limit_is_suppressed(tmp_path: Path) -> None:
    item = _candidate(
        input_changes={"distance_to_trigger_atr": 1.21},
        result_changes={"distance_to_trigger_atr": 1.21},
    )
    decision = _service(tmp_path).prepare((item,), NOW).decisions[0]

    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.DISTANCE_EXCEEDED


def test_duplicate_is_suppressed_without_cooldown_resend(tmp_path: Path) -> None:
    service = _service(tmp_path)
    item = _candidate()
    _commit(service, item)

    decision = service.prepare((item,), NOW + timedelta(hours=7)).decisions[0]

    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.DUPLICATE


def test_new_episode_is_allowed(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _commit(service, _candidate(episode="episode-1"))

    decision = service.prepare(
        (_candidate(episode="episode-2"),), NOW + timedelta(minutes=1)
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.reason is QtrNotificationReason.NEW_EPISODE


@pytest.mark.parametrize(
    ("input_changes", "result_changes", "reason"),
    (
        (
            {"direction": SetupDirection.DOWN},
            {"direction": SetupDirection.DOWN},
            QtrNotificationReason.DIRECTION_CHANGED,
        ),
        (
            {"retest_detected": True, "retest_held": True},
            {"setup_type": SetupType.RETEST, "retest_held": True},
            QtrNotificationReason.TYPE_CHANGED,
        ),
    ),
)
def test_direction_or_setup_change_is_allowed(
    tmp_path: Path,
    input_changes: dict[str, Any],
    result_changes: dict[str, Any],
    reason: QtrNotificationReason,
) -> None:
    service = _service(tmp_path)
    _commit(service, _candidate())
    changed = _candidate(
        input_changes=input_changes,
        result_changes=result_changes,
    )

    decision = service.prepare(
        (changed,), NOW + timedelta(minutes=1)
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.reason is reason


def test_quality_improvement_of_ten_is_allowed(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _commit(service, _candidate(result_changes={"volatility_confirmation": False}))

    decision = service.prepare(
        (_candidate(),), NOW + timedelta(minutes=1)
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.reason is QtrNotificationReason.QUALITY_IMPROVED


def test_suppressed_candidate_remains_in_audit(tmp_path: Path) -> None:
    decision = _service(tmp_path).prepare(
        (_candidate(result_changes={"setup_state": SetupState.LATE}),), NOW
    ).decisions[0]
    path = tmp_path / "audit.jsonl"
    JsonlQtrSetupTelegramAuditStore(path).append(
        (QtrSetupAuditOutcome(decision, False, False),), NOW
    )

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["state"] == "LATE"
    assert record["decision"] == "suppress"
    assert record["sent"] is False
    assert record["telegram_quality_score"] == 100.0


def test_notification_state_is_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        notifications_module,
        "QTR_SETUP_NOTIFICATION_STATE_CAPACITY",
        2,
    )
    service = _service(tmp_path)
    for index in range(3):
        _commit(
            service,
            _candidate(episode=f"episode-{index}"),
            NOW + timedelta(minutes=index),
        )

    state = JsonQtrSetupNotificationStore(
        tmp_path / "qtr_setup_notifications.json"
    ).load()
    assert len(state.records) == 2


def test_filter_counters_cover_each_outcome(tmp_path: Path) -> None:
    service = _service(tmp_path)
    ready = _candidate(episode="ready")
    first_plan = service.prepare(
        (
            ready,
            _candidate(
                episode="late",
                result_changes={"setup_state": SetupState.LATE},
            ),
            _candidate(
                episode="quality",
                result_changes={"volume_confirmation": False},
            ),
            _candidate(
                episode="distance",
                input_changes={"distance_to_trigger_atr": 1.3},
                result_changes={"distance_to_trigger_atr": 1.3},
            ),
        ),
        NOW,
    )
    ready_decision = first_plan.decisions[0]
    service.commit(first_plan, frozenset((ready_decision.decision_id,)))
    service.prepare((ready,), NOW + timedelta(minutes=1))

    metrics = service.metrics
    assert metrics.candidates_seen == 5
    assert metrics.telegram_quality_passed == 1
    assert metrics.suppressed_status == 1
    assert metrics.suppressed_quality == 1
    assert metrics.suppressed_distance == 1
    assert metrics.suppressed_duplicate == 1


def test_legacy_state_without_quality_loads_safely(tmp_path: Path) -> None:
    path = tmp_path / "qtr_setup_notifications.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": NOW.isoformat(),
                "records": {
                    "BTCUSDT::episode-1": {
                        "symbol": "BTCUSDT",
                        "episode_id": "episode-1",
                        "last_semantic_fingerprint": "legacy",
                        "last_state": "READY_TO_CONSIDER",
                        "last_direction": "UP",
                        "last_setup_type": "BREAKOUT",
                        "first_sent_at": NOW.isoformat(),
                        "last_sent_at": NOW.isoformat(),
                        "send_count": 1,
                        "cancellation_sent": False,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    state = JsonQtrSetupNotificationStore(path).load()
    assert state.records["BTCUSDT::episode-1"].last_quality_score is None


def test_formatter_is_short_and_not_a_probability() -> None:
    text = format_qtr_setup_event(event_from_candidate(_candidate()))

    assert "🔥 QTR A+ CANDIDATE" in text
    assert "BTCUSDT — LONG 🟢" in text
    assert "Quality: 100/100" in text
    assert "НЕ вероятность выигрыша" in text
    assert "QTR SCANNER —" not in text
    assert text.count("✅ ") <= 6


def test_no_elite_candidate_is_silent(tmp_path: Path) -> None:
    class Scanner:
        def scan(self) -> tuple[QtrSetupCandidate, ...]:
            return (
                _candidate(result_changes={"setup_state": SetupState.LATE}),
            )

    notifier = QtrSetupPilotNotifier(
        scanner=Scanner(),
        notification_service=_service(tmp_path),
        audit_store=JsonlQtrSetupTelegramAuditStore(tmp_path / "audit.jsonl"),
        allowed_chat_ids=frozenset((1,)),
        clock=lambda: NOW,
    )

    async def unexpected_send(chat_id: int, text: str) -> None:
        raise AssertionError((chat_id, text))

    assert asyncio.run(notifier.run_once(unexpected_send)) is False


def test_filter_settings_defaults_and_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QTR_SCANNER_TELEGRAM_MIN_QUALITY", raising=False)
    monkeypatch.delenv("QTR_SCANNER_TELEGRAM_MAX_DISTANCE_ATR", raising=False)
    defaults = QtrSetupTelegramSettings.from_environment()
    assert defaults.minimum_quality == 90.0
    assert defaults.maximum_distance_atr == 1.2

    monkeypatch.setenv("QTR_SCANNER_TELEGRAM_MIN_QUALITY", "92")
    monkeypatch.setenv("QTR_SCANNER_TELEGRAM_MAX_DISTANCE_ATR", "1.1")
    configured = QtrSetupTelegramSettings.from_environment()
    assert configured.minimum_quality == 92.0
    assert configured.maximum_distance_atr == 1.1
