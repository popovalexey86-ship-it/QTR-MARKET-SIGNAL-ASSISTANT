from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import market_signal_assistant.qtr_setup_pilot.notifications as qtr_notifications
from market_signal_assistant.inplay.early_discovery_v2 import (
    EarlyDiscoveryV2ScanReport,
)
from market_signal_assistant.inplay.notifications import (
    InPlayNotificationService,
    JsonInPlayNotificationStore,
)
from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
    JsonlVerifiedSetupProvider,
    VerifiedPriceContextAdapter,
)
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
from market_signal_assistant.qtr_setup_pilot.service import QtrSetupScanService
from market_signal_assistant.settings import (
    InPlayAutoSettings,
    QtrSetupTelegramSettings,
    TelegramSettings,
)
from market_signal_assistant.setup_engine import (
    SetupAnalysisInput,
    SetupDirection,
    SetupState,
    SetupType,
    analyze_setup,
)
from market_signal_assistant.telegram.bot import (
    _run_sdk_bot_handlers,
    execute_command,
)
from market_signal_assistant.telegram.qtr_setup_pilot import (
    QtrSetupPilotNotifier,
    format_qtr_setup_event,
    select_qtr_setup_decisions,
)

NOW = datetime(2026, 8, 10, 9, 0, tzinfo=UTC)


def source_input(**changes: Any) -> SetupAnalysisInput:
    baseline = SetupAnalysisInput(
        snapshot_ids=("scan-1",),
        source="early_discovery_v2",
        symbol="BTCUSDT",
        analyzed_at=NOW,
        direction=SetupDirection.UP,
        current_price=101.0,
        trigger_level=100.0,
        invalidation_level=99.0,
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


def candidate(
    *,
    episode: str = "episode-1",
    input_changes: dict[str, Any] | None = None,
    result_changes: dict[str, Any] | None = None,
) -> QtrSetupCandidate:
    source = source_input(**(input_changes or {}))
    result = analyze_setup(source)
    if result_changes:
        result = replace(result, **result_changes)
    return QtrSetupCandidate(episode, source, result)


def service(
    tmp_path: Path,
    *,
    minimum_quality: float = 90.0,
    maximum_distance_atr: float = 1.2,
) -> QtrSetupNotificationService:
    return QtrSetupNotificationService(
        JsonQtrSetupNotificationStore(tmp_path / "qtr_setup_notifications.json"),
        QtrTelegramFilterPolicy(minimum_quality, maximum_distance_atr),
    )


def test_producer_projection_is_consumed_as_verified_price_context(
    tmp_path: Path,
) -> None:
    source = source_input()
    item = QtrSetupCandidate(
        "episode-1",
        source,
        analyze_setup(source),
        atr_value=2.0,
        local_range_low=98.0,
        local_range_high=100.0,
    )
    notifications = service(tmp_path)
    decision = notifications.prepare((item,), NOW).decisions[0]
    audit_path = tmp_path / "scanner" / "qtr_setup_audit.jsonl"
    JsonlQtrSetupTelegramAuditStore(audit_path).append(
        (QtrSetupAuditOutcome(decision, False, False),),
        NOW,
    )
    adapter = VerifiedPriceContextAdapter(
        JsonlVerifiedSetupProvider(audit_path)
    )

    context = adapter("BTCUSDT", NOW, 101.0)

    assert context is not None
    assert context.atr == 2.0
    assert context.trigger_price == 100.0
    assert context.invalidation_price == 99.0
    assert context.local_range_low == 98.0
    assert context.local_range_high == 100.0


def commit_first(
    notifications: QtrSetupNotificationService,
    item: QtrSetupCandidate,
    when: datetime = NOW,
) -> None:
    plan = notifications.prepare((item,), when)
    decision = plan.decisions[0]
    assert decision.should_notify
    notifications.commit(plan, frozenset((decision.decision_id,)))


def test_new_setup_is_sent_and_identical_setup_is_suppressed(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    item = candidate()
    commit_first(notifications, item)
    plan = notifications.prepare((item,), NOW + timedelta(minutes=5))
    assert plan.decisions[0].should_notify is False
    assert plan.decisions[0].reason is QtrNotificationReason.DUPLICATE


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
def test_only_ready_status_is_sent(tmp_path: Path, state: SetupState) -> None:
    notifications = service(tmp_path)
    item = candidate(result_changes={"setup_state": state})
    decision = notifications.prepare((item,), NOW).decisions[0]
    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.STATUS_SUPPRESSED


def test_ready_duplicate_and_numeric_drift_are_suppressed(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    first = candidate()
    commit_first(notifications, first)
    changed = candidate(
        input_changes={"current_price": 101.7, "distance_to_trigger_atr": 0.7},
        result_changes={"confidence": first.result.confidence - 1},
    )
    decision = notifications.prepare(
        (changed,), NOW + timedelta(minutes=10)
    ).decisions[0]
    assert decision.should_notify is False
    assert event_from_candidate(first).semantic_fingerprint == event_from_candidate(
        changed
    ).semantic_fingerprint


def test_duplicate_remains_suppressed_after_legacy_cooldown(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    item = candidate()
    commit_first(notifications, item)
    decision = notifications.prepare(
        (item,), NOW + timedelta(hours=7)
    ).decisions[0]
    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.DUPLICATE


def test_new_episode_can_be_sent(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    commit_first(notifications, candidate(episode="episode-1"))
    decision = notifications.prepare(
        (candidate(episode="episode-2"),), NOW + timedelta(minutes=5)
    ).decisions[0]
    assert decision.should_notify is True
    assert decision.reason is QtrNotificationReason.NEW_EPISODE


def test_quality_improvement_of_ten_points_can_be_sent(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    initial = candidate(result_changes={"volatility_confirmation": False})
    assert qtr_telegram_quality_score(initial) == 90.0
    commit_first(notifications, initial)
    decision = notifications.prepare(
        (candidate(),), NOW + timedelta(minutes=5)
    ).decisions[0]
    assert decision.should_notify is True
    assert decision.reason is QtrNotificationReason.QUALITY_IMPROVED


def test_ready_quality_threshold_is_inclusive(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    item = candidate(result_changes={"volatility_confirmation": False})
    decision = notifications.prepare((item,), NOW).decisions[0]
    assert decision.event is not None
    assert decision.event.quality_score == 90.0
    assert decision.should_notify is True


def test_ready_below_quality_threshold_is_suppressed(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    item = candidate(result_changes={"volume_confirmation": False})
    decision = notifications.prepare((item,), NOW).decisions[0]
    assert decision.event is not None
    assert decision.event.quality_score == 85.0
    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.QUALITY_BELOW_THRESHOLD


def test_ready_beyond_atr_distance_is_suppressed(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    item = candidate(
        input_changes={"distance_to_trigger_atr": 1.21},
        result_changes={"distance_to_trigger_atr": 1.21},
    )
    decision = notifications.prepare((item,), NOW).decisions[0]
    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.DISTANCE_EXCEEDED


@pytest.mark.parametrize(
    "result_changes",
    (
        {"liquidity_ok": False},
        {"spread_ok": False},
        {"freshness_confirmation": False},
    ),
)
def test_liquidity_spread_or_freshness_failure_is_suppressed(
    tmp_path: Path,
    result_changes: dict[str, Any],
) -> None:
    decision = service(tmp_path).prepare(
        (candidate(result_changes=result_changes),), NOW
    ).decisions[0]
    assert decision.should_notify is False
    assert decision.reason is QtrNotificationReason.QUALITY_BELOW_THRESHOLD


@pytest.mark.parametrize(
    ("input_changes", "result_changes"),
    [
        ({"direction": SetupDirection.DOWN}, {"direction": SetupDirection.DOWN}),
        (
            {"retest_detected": True, "retest_held": True},
            {"setup_type": SetupType.RETEST, "retest_held": True},
        ),
    ],
)
def test_direction_or_type_change_is_sent(
    tmp_path: Path,
    input_changes: dict[str, Any],
    result_changes: dict[str, Any],
) -> None:
    notifications = service(tmp_path)
    commit_first(notifications, candidate())
    changed = candidate(
        input_changes=input_changes,
        result_changes=result_changes,
    )
    assert notifications.prepare(
        (changed,), NOW + timedelta(minutes=5)
    ).decisions[0].should_notify


def test_late_and_cancelled_are_suppressed_but_audited(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    late = candidate(result_changes={"setup_state": SetupState.LATE})
    cancelled = candidate(result_changes={"setup_state": SetupState.CANCELLED})
    plan = notifications.prepare((late, cancelled), NOW)
    assert all(not item.should_notify for item in plan.decisions)
    audit_path = tmp_path / "qtr_setup_audit.jsonl"
    JsonlQtrSetupTelegramAuditStore(audit_path).append(
        tuple(QtrSetupAuditOutcome(item, False, False) for item in plan.decisions),
        NOW,
    )
    records = [
        json.loads(line)
        for line in audit_path.read_text(encoding="utf-8").splitlines()
    ]
    assert {item["state"] for item in records} == {"LATE", "CANCELLED"}
    assert all(item["decision"] == "suppress" for item in records)


def test_watching_no_trade_and_technical_errors_are_not_sent(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    watching = candidate(
        result_changes={
            "setup_state": SetupState.WATCHING,
            "setup_type": SetupType.NO_TRADE,
        }
    )
    technical = candidate(
        result_changes={"technical_gap": True, "missing_data": ("candles",)}
    )
    decisions = notifications.prepare((watching, technical), NOW).decisions
    assert not any(item.should_notify for item in decisions)


def test_prepare_does_not_write_and_failed_delivery_can_retry(tmp_path: Path) -> None:
    path = tmp_path / "qtr_setup_notifications.json"
    notifications = QtrSetupNotificationService(JsonQtrSetupNotificationStore(path))
    first = notifications.prepare((candidate(),), NOW)
    second = notifications.prepare((candidate(),), NOW + timedelta(minutes=1))
    assert first.decisions[0].should_notify
    assert second.decisions[0].should_notify
    assert not path.exists()


def test_commit_persists_atomic_separate_state(tmp_path: Path) -> None:
    path = tmp_path / "qtr_setup_notifications.json"
    notifications = QtrSetupNotificationService(JsonQtrSetupNotificationStore(path))
    plan = notifications.prepare((candidate(),), NOW)
    notifications.commit(plan, frozenset((plan.decisions[0].decision_id,)))
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["version"] == 1
    assert raw["records"]
    assert not path.with_suffix(".json.tmp").exists()
    assert path.name not in {"inplay_notifications.json", "news_notifications.json"}


def test_corrupt_state_is_backed_up_and_recovers(tmp_path: Path) -> None:
    path = tmp_path / "qtr_setup_notifications.json"
    path.write_text("{broken", encoding="utf-8")
    store = JsonQtrSetupNotificationStore(path)
    assert store.load().records == {}
    assert path.with_suffix(".json.corrupt").exists()


def test_priority_and_maximum_three(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    items = (
        candidate(
            episode="a",
            input_changes={"symbol": "AUSDT"},
            result_changes={"confidence": 60.0},
        ),
        candidate(
            episode="b",
            input_changes={"symbol": "BUSDT"},
            result_changes={"confidence": 90.0},
        ),
        candidate(
            episode="c",
            input_changes={"symbol": "CUSDT"},
            result_changes={"confidence": 80.0},
        ),
        candidate(
            episode="d",
            input_changes={"symbol": "DUSDT"},
            result_changes={"confidence": 70.0},
        ),
    )
    selected = select_qtr_setup_decisions(notifications.prepare(items, NOW))
    assert len(selected) == 3
    assert tuple(item.candidate.result.symbol for item in selected) == (
        "BUSDT",
        "CUSDT",
        "DUSDT",
    )


def test_russian_format_has_no_machine_states_or_buy_sell() -> None:
    text = format_qtr_setup_event(event_from_candidate(candidate()))
    assert "🔥 QTR A+ CANDIDATE" in text
    assert "BTCUSDT — LONG 🟢" in text
    assert "Quality: 100/100" in text
    assert "НЕ вероятность выигрыша" in text
    assert "QTR SCANNER —" not in text
    assert "BUY" not in text
    assert "SELL" not in text
    for machine_value in ("READY_TO_CONSIDER", "UP", "BREAKOUT"):
        assert machine_value not in text
    assert text.count("✅ ") <= 6


def test_filter_metrics_are_lightweight_counters(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    ready = candidate(episode="ready")
    commit_first(notifications, ready)
    items = (
        ready,
        candidate(episode="late", result_changes={"setup_state": SetupState.LATE}),
        candidate(
            episode="quality", result_changes={"volume_confirmation": False}
        ),
        candidate(
            episode="distance",
            input_changes={"distance_to_trigger_atr": 1.3},
            result_changes={"distance_to_trigger_atr": 1.3},
        ),
    )
    notifications.prepare(items, NOW + timedelta(minutes=5))
    metrics = notifications.metrics
    assert metrics.candidates_seen == 5
    assert metrics.telegram_quality_passed == 1
    assert metrics.suppressed_status == 1
    assert metrics.suppressed_quality == 1
    assert metrics.suppressed_distance == 1
    assert metrics.suppressed_duplicate == 1


def test_notification_state_is_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(qtr_notifications, "QTR_SETUP_NOTIFICATION_STATE_CAPACITY", 2)
    notifications = service(tmp_path)
    for index in range(3):
        item = candidate(episode=f"episode-{index}")
        commit_first(notifications, item, NOW + timedelta(minutes=index))
    state = JsonQtrSetupNotificationStore(
        tmp_path / "qtr_setup_notifications.json"
    ).load()
    assert len(state.records) == 2


def test_delivery_failure_does_not_commit_and_retry_succeeds(
    tmp_path: Path,
) -> None:
    class Scanner:
        def scan(self) -> tuple[QtrSetupCandidate, ...]:
            return (candidate(),)

    path = tmp_path / "qtr_setup_notifications.json"
    handled: list[tuple[QtrSetupCandidate, ...]] = []

    async def handle_candidates(
        items: tuple[QtrSetupCandidate, ...], send_callback: Any
    ) -> None:
        del send_callback
        handled.append(items)

    notifier = QtrSetupPilotNotifier(
        scanner=Scanner(),
        notification_service=QtrSetupNotificationService(
            JsonQtrSetupNotificationStore(path)
        ),
        audit_store=JsonlQtrSetupTelegramAuditStore(tmp_path / "audit.jsonl"),
        allowed_chat_ids=frozenset((1,)),
        clock=lambda: NOW,
        candidate_handler=handle_candidates,
    )

    async def fail(chat_id: int, text: str) -> None:
        del chat_id, text
        raise RuntimeError("offline")

    delivered: list[str] = []

    async def send(chat_id: int, text: str) -> None:
        del chat_id
        delivered.append(text)

    assert asyncio.run(notifier.run_once(fail)) is False
    assert not path.exists()
    assert handled == []
    assert asyncio.run(notifier.run_once(send)) is True
    assert delivered
    assert path.exists()
    assert len(handled) == 1


def test_setting_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QTR_SETUP_TELEGRAM_ENABLED", raising=False)
    monkeypatch.delenv("QTR_SCANNER_TELEGRAM_MIN_QUALITY", raising=False)
    monkeypatch.delenv("QTR_SCANNER_TELEGRAM_MAX_DISTANCE_ATR", raising=False)
    settings = QtrSetupTelegramSettings.from_environment()
    assert settings.enabled is False
    assert settings.minimum_quality == 90.0
    assert settings.maximum_distance_atr == 1.2


def test_quality_and_distance_settings_are_configurable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QTR_SCANNER_TELEGRAM_MIN_QUALITY", "92.5")
    monkeypatch.setenv("QTR_SCANNER_TELEGRAM_MAX_DISTANCE_ATR", "1.1")
    settings = QtrSetupTelegramSettings.from_environment()
    assert settings.minimum_quality == 92.5
    assert settings.maximum_distance_atr == 1.1


@pytest.mark.parametrize(
    ("name", "value"),
    (
        ("QTR_SCANNER_TELEGRAM_MIN_QUALITY", "101"),
        ("QTR_SCANNER_TELEGRAM_MAX_DISTANCE_ATR", "0"),
    ),
)
def test_invalid_filter_settings_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        QtrSetupTelegramSettings.from_environment()


def test_no_elite_candidate_is_silent_but_still_audited(tmp_path: Path) -> None:
    class Scanner:
        def scan(self) -> tuple[QtrSetupCandidate, ...]:
            return (
                candidate(result_changes={"setup_state": SetupState.LATE}),
            )

    audit_path = tmp_path / "audit.jsonl"
    notifier = QtrSetupPilotNotifier(
        scanner=Scanner(),
        notification_service=service(tmp_path),
        audit_store=JsonlQtrSetupTelegramAuditStore(audit_path),
        allowed_chat_ids=frozenset((1,)),
        clock=lambda: NOW,
    )

    async def unexpected_send(chat_id: int, text: str) -> None:
        raise AssertionError((chat_id, text))

    assert asyncio.run(notifier.run_once(unexpected_send)) is False
    record = json.loads(audit_path.read_text(encoding="utf-8").strip())
    assert record["state"] == "LATE"
    assert record["decision"] == "suppress"
    assert record["sent"] is False


def test_status_shows_qtr_setup_pilot_state() -> None:
    class Screening:
        def screen(self, request: object) -> object:
            raise AssertionError(request)

    execution = execute_command(
        "/status",
        chat_id=1,
        allowed_chat_ids=frozenset((1,)),
        service=Screening(),  # type: ignore[arg-type]
        qtr_setup_telegram_enabled=True,
    )
    assert "QTR Setup Pilot: включён" in execution.messages[0]


def test_setup_scan_reuses_exactly_one_v2_report() -> None:
    class Scanner:
        calls = 0

        def scan(self) -> EarlyDiscoveryV2ScanReport:
            self.calls += 1
            return EarlyDiscoveryV2ScanReport(NOW, NOW, 0, 0, 0, 0, 0, 0, ())

    scanner = Scanner()
    assert QtrSetupScanService(scanner).scan() == ()
    assert scanner.calls == 1


def test_qtr_pilot_uses_existing_telegram_lifecycle(tmp_path: Path) -> None:
    class Screening:
        def screen(self, request: object) -> object:
            raise AssertionError(request)

    class InPlay:
        def scan(self, maximum_results: int = 10, **kwargs: object) -> object:
            raise AssertionError((maximum_results, kwargs))

    class Notifier:
        calls = 0

        async def run_once(self, send: Any) -> bool:
            self.calls += 1
            await send(100, "пилот")
            return True

    class FakeBot:
        def __init__(self) -> None:
            self.messages: list[tuple[int, str]] = []

        async def send_message(self, *, chat_id: int, text: str) -> None:
            self.messages.append((chat_id, text))

    class FakeApplication:
        def __init__(self, builder: FakeBuilder) -> None:
            self._builder = builder
            self.bot = FakeBot()

        def add_handler(self, handler: object) -> None:
            del handler

        def run_polling(self) -> None:
            async def lifecycle() -> None:
                assert self._builder.on_start is not None
                assert self._builder.on_stop is not None
                await self._builder.on_start(self)
                for _ in range(100):
                    if notifier.calls:
                        break
                    await asyncio.sleep(0.001)
                await self._builder.on_stop(self)

            asyncio.run(lifecycle())

    class FakeBuilder:
        def __init__(self) -> None:
            self.on_start: Any = None
            self.on_stop: Any = None
            self.application: FakeApplication | None = None

        def token(self, value: str) -> FakeBuilder:
            assert value == "token"
            return self

        def post_init(self, callback: Any) -> FakeBuilder:
            self.on_start = callback
            return self

        def post_shutdown(self, callback: Any) -> FakeBuilder:
            self.on_stop = callback
            return self

        def build(self) -> FakeApplication:
            self.application = FakeApplication(self)
            return self.application

    notifier = Notifier()
    builder = FakeBuilder()
    _run_sdk_bot_handlers(
        TelegramSettings("token", frozenset((100,))),
        Screening(),  # type: ignore[arg-type]
        InPlay(),  # type: ignore[arg-type]
        False,
        InPlayAutoSettings(),
        InPlayNotificationService(
            JsonInPlayNotificationStore(tmp_path / "inplay_notifications.json")
        ),
        lambda: builder,
        lambda *args: args,
        SimpleNamespace(COMMAND=object()),
        qtr_setup_settings=QtrSetupTelegramSettings(enabled=True),
        qtr_setup_interval_minutes=5,
        qtr_setup_notifier=notifier,  # type: ignore[arg-type]
    )
    assert builder.application is not None
    assert builder.application.bot.messages == [(100, "пилот")]
    assert notifier.calls == 1
