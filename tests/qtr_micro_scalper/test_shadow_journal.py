import json
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_shadow_runtime import bar
from test_snapshot import NOW, build_complete

from market_signal_assistant.qtr_micro_scalper.scoring import ScalperDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    ShadowTradeRecord,
    build_shadow_trade_record,
    deserialize_shadow_trade_record,
    recover_shadow_journal,
    serialize_shadow_trade_record,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntime,
    ShadowRuntimeDecision,
    ShadowRuntimeEvent,
    ShadowRuntimeEventType,
    ShadowRuntimeUpdate,
)


def created() -> tuple[ShadowRuntime, ShadowRuntimeDecision]:
    runtime = ShadowRuntime()
    decision = runtime.process_snapshot(build_complete())
    assert decision.trade is not None
    assert decision.score is not None
    return runtime, decision


def record_from_decision(
    decision: ShadowRuntimeDecision,
    *,
    seconds: int = 0,
) -> ShadowTradeRecord:
    assert decision.trade is not None
    assert decision.score is not None
    return build_shadow_trade_record(
        decision.trade,
        decision.score,
        recorded_at=NOW + timedelta(seconds=seconds),
        events=decision.events,
        reasons=("Русская причина журнала.",),
        warnings=("Тестовое предупреждение.",),
    )


def record_from_update(
    update: ShadowRuntimeUpdate,
    decision: ShadowRuntimeDecision,
    *,
    seconds: int,
) -> ShadowTradeRecord:
    assert update.trade is not None
    assert decision.score is not None
    return build_shadow_trade_record(
        update.trade,
        decision.score,
        recorded_at=NOW + timedelta(seconds=seconds),
        events=update.events,
    )


def test_record_contains_requested_trade_and_outcome_fields() -> None:
    _, decision = created()
    assert decision.trade is not None
    assert decision.score is not None
    record = record_from_decision(decision)
    assert record.trade_id == decision.trade.trade_id
    assert record.symbol == "BTCUSDT"
    assert record.direction.value == "LONG"
    assert record.entry == 100.0
    assert record.stop == 97.8
    assert record.tp1 == pytest.approx(102.2)
    assert record.tp2 == pytest.approx(104.4)
    assert record.score == decision.score.total_score
    assert record.entry_time is None
    assert record.exit_time is None
    assert record.outcome is ShadowOutcomeStatus.PENDING
    assert record.result_r == 0.0
    assert record.mfe == 0.0
    assert record.mae == 0.0


def test_jsonl_append_is_utf8_and_append_only(tmp_path: Path) -> None:
    path = tmp_path / "shadow.jsonl"
    _, decision = created()
    record = record_from_decision(decision)
    journal = ShadowTradeJournal(path)
    assert journal.append(record) is True
    first_bytes = path.read_bytes()
    assert "Русская причина" in first_bytes.decode("utf-8")

    runtime = ShadowRuntime()
    second_decision = runtime.process_snapshot(build_complete())
    assert second_decision.trade is not None
    assert second_decision.score is not None
    changed = replace(second_decision.trade, max_favorable_excursion_r=0.25)
    second = build_shadow_trade_record(
        changed,
        second_decision.score,
        recorded_at=NOW + timedelta(seconds=1),
    )
    assert journal.append(second) is True
    assert path.read_bytes().startswith(first_bytes)
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_duplicate_identity_ignores_recorded_at(tmp_path: Path) -> None:
    _, decision = created()
    first = record_from_decision(decision)
    later = record_from_decision(decision, seconds=10)
    assert first.record_id == later.record_id
    journal = ShadowTradeJournal(tmp_path / "shadow.jsonl")
    assert journal.append(first) is True
    assert journal.append(later) is False


def test_duplicate_protection_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "shadow.jsonl"
    _, decision = created()
    record = record_from_decision(decision)
    assert ShadowTradeJournal(path).append(record) is True
    restarted = ShadowTradeJournal(path)
    assert restarted.append(record) is False
    assert restarted.records() == (record,)


def test_recovery_skips_corrupted_line_and_keeps_valid_records(tmp_path: Path) -> None:
    path = tmp_path / "shadow.jsonl"
    runtime, decision = created()
    first = record_from_decision(decision)
    opened = runtime.process_bar(bar(1))
    second = record_from_update(opened, decision, seconds=2)
    path.write_text(
        f"{serialize_shadow_trade_record(first)}\n{{broken json\n"
        f"{serialize_shadow_trade_record(second)}\n",
        encoding="utf-8",
    )
    recovery = recover_shadow_journal(path)
    assert recovery.records == (first, second)
    assert recovery.corrupted_line_numbers == (2,)
    assert "line 2" in recovery.warnings[0]


def test_recovery_skips_invalid_utf8_line(tmp_path: Path) -> None:
    path = tmp_path / "shadow.jsonl"
    _, decision = created()
    record = record_from_decision(decision)
    valid = serialize_shadow_trade_record(record).encode("utf-8")
    path.write_bytes(b"\xff\xfe\n" + valid + b"\n")
    recovery = recover_shadow_journal(path)
    assert recovery.corrupted_line_numbers == (1,)
    assert recovery.records == (record,)


def test_missing_file_recovers_without_creating_it(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "shadow.jsonl"
    recovery = recover_shadow_journal(path)
    assert recovery.records == ()
    assert recovery.warnings == ()
    assert path.exists() is False


def test_serialization_is_deterministic_and_round_trips() -> None:
    _, decision = created()
    record = record_from_decision(decision)
    first = serialize_shadow_trade_record(record)
    second = serialize_shadow_trade_record(record)
    assert first == second
    assert first.startswith('{"direction":')
    assert deserialize_shadow_trade_record(first) == record


def test_tampered_line_is_reported_as_corrupted(tmp_path: Path) -> None:
    _, decision = created()
    record = record_from_decision(decision)
    payload = json.loads(serialize_shadow_trade_record(record))
    payload["score"] = 1.0
    path = tmp_path / "shadow.jsonl"
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    recovery = recover_shadow_journal(path)
    assert recovery.records == ()
    assert recovery.corrupted_line_numbers == (1,)


def test_runtime_events_are_persisted_and_recovered(tmp_path: Path) -> None:
    path = tmp_path / "shadow.jsonl"
    _, decision = created()
    record = record_from_decision(decision)
    journal = ShadowTradeJournal(path)
    assert journal.append(record) is True
    recovered = ShadowTradeJournal(path).records()[0]
    assert recovered.events == decision.events
    assert recovered.events[0].event_type is ShadowRuntimeEventType.ENTRY_CREATED


def test_full_trade_lifecycle_is_persisted_as_separate_records(tmp_path: Path) -> None:
    runtime, decision = created()
    waiting = record_from_decision(decision)
    opened_update = runtime.process_bar(bar(1))
    opened = record_from_update(opened_update, decision, seconds=2)
    tp1_update = runtime.process_bar(bar(3, high=102.3, low=100.0, close=102.0))
    tp1 = record_from_update(tp1_update, decision, seconds=4)
    closed_update = runtime.process_bar(
        bar(5, open_price=102.0, high=104.5, low=101.0, close=104.0)
    )
    closed = record_from_update(closed_update, decision, seconds=6)

    journal = ShadowTradeJournal(tmp_path / "shadow.jsonl")
    assert all(journal.append(item) for item in (waiting, opened, tp1, closed))
    assert [record.stage for record in journal.records()] == [
        ShadowTradeStage.WAITING_ENTRY,
        ShadowTradeStage.OPEN,
        ShadowTradeStage.TP1_HIT,
        ShadowTradeStage.CLOSED,
    ]
    assert closed.outcome is ShadowOutcomeStatus.WIN
    assert closed.entry_time == NOW + timedelta(seconds=2)
    assert closed.exit_time == NOW + timedelta(seconds=6)
    assert closed.result_r == pytest.approx(1.5)
    assert closed.mfe > 0


def test_latest_returns_last_lifecycle_record(tmp_path: Path) -> None:
    runtime, decision = created()
    waiting = record_from_decision(decision)
    opened = record_from_update(runtime.process_bar(bar(1)), decision, seconds=2)
    journal = ShadowTradeJournal(tmp_path / "shadow.jsonl")
    journal.append(waiting)
    journal.append(opened)
    assert journal.latest(waiting.trade_id) == opened
    assert journal.latest("missing") is None


def test_direction_mismatch_is_rejected() -> None:
    _, decision = created()
    assert decision.trade is not None
    assert decision.score is not None
    wrong_score = replace(decision.score, direction=ScalperDirection.SHORT)
    with pytest.raises(ValueError, match="directions do not match"):
        build_shadow_trade_record(
            decision.trade,
            wrong_score,
            recorded_at=NOW,
        )


def test_record_and_recovery_models_are_immutable(tmp_path: Path) -> None:
    _, decision = created()
    record = record_from_decision(decision)
    with pytest.raises(FrozenInstanceError):
        record.result_r = 1.0  # type: ignore[misc]
    recovery = recover_shadow_journal(tmp_path / "missing.jsonl")
    with pytest.raises(FrozenInstanceError):
        recovery.records = (record,)  # type: ignore[misc]


def test_event_trade_identity_is_validated() -> None:
    _, decision = created()
    assert decision.trade is not None
    assert decision.score is not None
    foreign_event = ShadowRuntimeEvent(
        event_id="event-foreign",
        event_type=ShadowRuntimeEventType.ENTRY_CREATED,
        occurred_at=NOW,
        trade_id="foreign-trade",
        symbol="BTCUSDT",
        stage=ShadowTradeStage.WAITING_ENTRY,
        price=100.0,
        realized_r=0.0,
        message="Virtual entry.",
    )
    with pytest.raises(ValueError, match="another trade"):
        build_shadow_trade_record(
            decision.trade,
            decision.score,
            recorded_at=NOW,
            events=(foreign_event,),
        )


def test_recorded_at_cannot_precede_trade_state() -> None:
    runtime, decision = created()
    update = runtime.process_bar(bar(1))
    assert update.trade is not None
    assert decision.score is not None
    with pytest.raises(ValueError, match="cannot precede"):
        build_shadow_trade_record(
            update.trade,
            decision.score,
            recorded_at=NOW,
        )
