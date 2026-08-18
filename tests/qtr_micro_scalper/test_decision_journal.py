from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_orchestrator import activate, analysis_input
from test_shadow_runtime import bar

from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    DecisionJournalIndex,
    DecisionJournalRecovery,
    ShadowDecisionEventType,
    ShadowDecisionJournal,
    ShadowDecisionRecord,
    deserialize_decision_record,
    recover_decision_journal,
    serialize_decision_record,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import (
    ShadowOrchestrator,
)
from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperComponentScores,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def components() -> ScalperComponentScores:
    return ScalperComponentScores(
        liquidity_score=20.0,
        trade_flow_score=25.0,
        orderbook_score=15.0,
        market_state_score=12.0,
        setup_score=8.0,
        risk_score=-5.0,
    )


def record(
    event_type: ShadowDecisionEventType = ShadowDecisionEventType.SCORE_CREATED,
    *,
    seconds: int = 0,
    score: float | None = 75.0,
    score_components: ScalperComponentScores | None = None,
) -> ShadowDecisionRecord:
    return ShadowDecisionRecord(
        timestamp=NOW + timedelta(seconds=seconds),
        symbol="btcusdt",
        event_type=event_type,
        score=score,
        score_components=score_components
        or (components() if score is not None else None),
        market_state="buy_pressure" if score is not None else None,
        setup_context="shadow_candidate" if score is not None else None,
        reasons=(f"Reason for {event_type.value}.",),
        warnings=("Offline shadow warning.",),
    )


def test_all_required_decision_event_types_are_supported() -> None:
    expected = {
        "TARGET_FOUND",
        "ANALYSIS_STARTED",
        "SNAPSHOT_READY",
        "SCORE_CREATED",
        "DECISION_BLOCKED",
        "SHADOW_ENTRY_CREATED",
        "TRADE_UPDATED",
        "TRADE_FINISHED",
    }
    assert {event.value for event in ShadowDecisionEventType} == expected


def test_record_normalizes_fields_and_is_immutable() -> None:
    value = record()
    assert value.symbol == "BTCUSDT"
    assert value.market_state == "BUY_PRESSURE"
    assert value.setup_context == "SHADOW_CANDIDATE"
    assert value.timestamp.tzinfo is UTC
    assert value.event_id.startswith("shadow-decision-")
    with pytest.raises(FrozenInstanceError):
        value.score = 1.0  # type: ignore[misc]


def test_deterministic_id_and_serialization() -> None:
    first = record()
    second = record()
    assert first.event_id == second.event_id
    assert serialize_decision_record(first) == serialize_decision_record(second)
    changed = record(ShadowDecisionEventType.DECISION_BLOCKED)
    assert changed.event_id != first.event_id


def test_round_trip_preserves_score_components_and_unicode() -> None:
    value = ShadowDecisionRecord(
        timestamp=NOW,
        symbol="BTCUSDT",
        event_type=ShadowDecisionEventType.SCORE_CREATED,
        score=75.0,
        score_components=components(),
        market_state="BUY_PRESSURE",
        setup_context="SHADOW_CANDIDATE",
        reasons=("Причина решения.",),
        warnings=("Предупреждение.",),
    )
    serialized = serialize_decision_record(value)
    assert "Причина решения" in serialized
    assert deserialize_decision_record(serialized) == value


def test_payload_contains_every_required_field() -> None:
    payload = json.loads(serialize_decision_record(record()))
    assert set(payload) == {
        "event_id",
        "event_type",
        "market_state",
        "reasons",
        "schema_version",
        "score",
        "score_components",
        "setup_context",
        "symbol",
        "timestamp",
        "warnings",
    }
    assert set(payload["score_components"]) == {
        "liquidity_score",
        "trade_flow_score",
        "orderbook_score",
        "market_state_score",
        "setup_score",
        "risk_score",
    }


def test_append_only_journal_and_duplicate_protection(tmp_path: Path) -> None:
    path = tmp_path / "decisions.jsonl"
    journal = ShadowDecisionJournal(path)
    first = record()
    second = record(ShadowDecisionEventType.DECISION_BLOCKED, seconds=1)
    assert journal.append(first)
    first_bytes = path.read_bytes()
    assert not journal.append(first)
    assert journal.append(second)
    assert path.read_bytes().startswith(first_bytes)
    assert journal.records() == (first, second)


def test_duplicate_protection_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "decisions.jsonl"
    value = record()
    assert ShadowDecisionJournal(path).append(value)
    restarted = ShadowDecisionJournal(path)
    assert not restarted.append(value)
    assert restarted.records() == (value,)


def test_recovery_skips_corruption_and_invalid_utf8(tmp_path: Path) -> None:
    path = tmp_path / "decisions.jsonl"
    valid = serialize_decision_record(record()).encode("utf-8")
    path.write_bytes(valid + b"\n{broken\n\xff\xfe\n")
    recovery = recover_decision_journal(path)
    assert recovery.records == (record(),)
    assert recovery.corrupted_line_numbers == (2, 3)
    assert len(recovery.warnings) == 2


def test_missing_journal_recovers_without_creating_file(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "decisions.jsonl"
    recovery = recover_decision_journal(path)
    assert recovery == DecisionJournalRecovery((), (), ())
    assert not path.exists()


def test_tampered_event_id_is_rejected() -> None:
    payload = json.loads(serialize_decision_record(record()))
    payload["score"] = 1.0
    with pytest.raises(ValueError, match="does not match"):
        deserialize_decision_record(json.dumps(payload))


def test_nullable_analysis_fields_are_serialized_as_null() -> None:
    value = record(
        ShadowDecisionEventType.TARGET_FOUND,
        score=None,
        score_components=None,
    )
    payload = json.loads(serialize_decision_record(value))
    assert payload["score"] is None
    assert payload["score_components"] is None
    assert payload["market_state"] is None
    assert payload["setup_context"] is None


def test_invalid_score_and_naive_timestamp_are_rejected() -> None:
    with pytest.raises(ValueError, match="between 0 and 100"):
        record(score=101.0)
    with pytest.raises(ValueError, match="timezone-aware"):
        ShadowDecisionRecord(
            timestamp=datetime(2026, 8, 16, 12),
            symbol="BTCUSDT",
            event_type=ShadowDecisionEventType.TARGET_FOUND,
            score=None,
            score_components=None,
            market_state=None,
            setup_context=None,
            reasons=("Target found.",),
        )


def test_flush_is_safe_for_missing_and_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "decisions.jsonl"
    journal = ShadowDecisionJournal(path)
    journal.flush()
    assert journal.append(record())
    journal.flush()
    assert deserialize_decision_record(path.read_text(encoding="utf-8")) == record()


def test_orchestrator_persists_full_shadow_decision_lifecycle(
    tmp_path: Path,
) -> None:
    decisions = ShadowDecisionJournal(tmp_path / "decisions.jsonl")
    coordinator = ShadowOrchestrator(
        journal=ShadowTradeJournal(tmp_path / "trades.jsonl"),
        decision_journal=decisions,
    )
    activate(coordinator)
    analyzed = coordinator.analyze(analysis_input())
    assert analyzed.trade is not None
    coordinator.process_bars((bar(1),))
    coordinator.process_bars((bar(3, high=102.3, low=100.0, close=102.0),))
    coordinator.process_bars(
        (bar(5, open_price=102.0, high=104.5, low=101.0, close=104.0),)
    )

    records = decisions.records()
    assert [item.event_type for item in records] == [
        ShadowDecisionEventType.TARGET_FOUND,
        ShadowDecisionEventType.ANALYSIS_STARTED,
        ShadowDecisionEventType.SNAPSHOT_READY,
        ShadowDecisionEventType.SCORE_CREATED,
        ShadowDecisionEventType.SHADOW_ENTRY_CREATED,
        ShadowDecisionEventType.TRADE_UPDATED,
        ShadowDecisionEventType.TRADE_UPDATED,
        ShadowDecisionEventType.TRADE_FINISHED,
    ]
    assert records[3].score_components is not None
    assert records[-1].market_state == "BUY_PRESSURE"
    assert records[-1].setup_context == "SHADOW_CANDIDATE"


def test_orchestrator_persists_blocked_decision(tmp_path: Path) -> None:
    decisions = ShadowDecisionJournal(tmp_path / "decisions.jsonl")
    coordinator = ShadowOrchestrator(
        journal=ShadowTradeJournal(tmp_path / "trades.jsonl"),
        decision_journal=decisions,
    )
    activate(coordinator)
    result = coordinator.analyze(analysis_input())
    assert result.trade is not None
    duplicate = coordinator.analyze(analysis_input())
    assert duplicate.trade is None
    blocked = decisions.records()[-1]
    assert blocked.event_type is ShadowDecisionEventType.DECISION_BLOCKED
    assert blocked.score is not None

def test_streaming_index_bootstraps_once_and_reads_only_append_delta(
    tmp_path: Path,
) -> None:
    path = tmp_path / "large-decisions.jsonl"
    records = tuple(record(seconds=index) for index in range(2_000))
    path.write_text(
        "".join(f"{serialize_decision_record(item)}\n" for item in records),
        encoding="utf-8",
    )
    seen: list[ShadowDecisionRecord] = []
    index = DecisionJournalIndex(path, on_record=seen.append)

    assert index.refresh() == len(records)
    bootstrapped = index.metrics()
    assert bootstrapped.bootstrap_scans == 1
    assert bootstrapped.incremental_reads == 0
    assert bootstrapped.records_processed == len(records)
    assert bootstrapped.bytes_read == path.stat().st_size

    for _ in range(100):
        assert index.refresh() == 0
    unchanged = index.metrics()
    assert unchanged == bootstrapped
    assert len(seen) == len(records)

    appended = record(seconds=2_001)
    encoded = f"{serialize_decision_record(appended)}\n"
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(encoded)
    assert index.refresh() == 1
    updated = index.metrics()
    assert updated.bootstrap_scans == 1
    assert updated.incremental_reads == 1
    assert updated.records_processed == len(records) + 1
    assert updated.bytes_read == bootstrapped.bytes_read + len(
        encoded.encode("utf-8")
    )


def test_streaming_index_defers_incomplete_trailing_line(tmp_path: Path) -> None:
    path = tmp_path / "partial.jsonl"
    first = f"{serialize_decision_record(record())}\n".encode()
    second = f"{serialize_decision_record(record(seconds=1))}\n".encode()
    split_at = len(second) // 2
    path.write_bytes(first + second[:split_at])
    index = DecisionJournalIndex(path)

    assert index.refresh() == 1
    partial_metrics = index.metrics()
    assert partial_metrics.records_processed == 1
    assert index.refresh() == 0
    assert index.metrics() == partial_metrics

    with path.open("ab") as stream:
        stream.write(second[split_at:])
    assert index.refresh() == 1
    assert index.metrics().records_processed == 2


def test_streaming_index_resets_after_truncation(tmp_path: Path) -> None:
    path = tmp_path / "rotated.jsonl"
    original = tuple(record(seconds=index) for index in range(10))
    path.write_text(
        "".join(f"{serialize_decision_record(item)}\n" for item in original),
        encoding="utf-8",
    )
    seen: list[ShadowDecisionRecord] = []
    resets = 0

    def reset() -> None:
        nonlocal resets
        resets += 1
        seen.clear()

    index = DecisionJournalIndex(path, on_record=seen.append, on_reset=reset)
    assert index.refresh() == len(original)
    replacement = record(seconds=100)
    path.write_text(
        f"{serialize_decision_record(replacement)}\n",
        encoding="utf-8",
    )

    assert index.refresh() == 1
    assert seen == [replacement]
    assert resets == 1
    assert index.metrics().resets_rotations == 1


def test_lean_runtime_journal_does_not_retain_full_records(tmp_path: Path) -> None:
    path = tmp_path / "lean.jsonl"
    values = tuple(record(seconds=index) for index in range(500))
    path.write_text(
        "".join(f"{serialize_decision_record(item)}\n" for item in values),
        encoding="utf-8",
    )

    journal = ShadowDecisionJournal(path, retain_records=False)

    assert journal.record_count == len(values)
    assert journal.records() == ()
    assert journal.index_metrics.cached_event_ids == len(values)
    assert not journal.append(values[-1])
    assert journal.append(record(seconds=501))
    assert journal.records() == ()


def test_explicit_recovery_streams_without_path_read_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "streamed.jsonl"
    path.write_text(
        f"{serialize_decision_record(record())}\n",
        encoding="utf-8",
    )

    def forbidden_read_bytes(self: Path) -> bytes:
        del self
        raise AssertionError("raw full-file recovery is forbidden")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    recovery = recover_decision_journal(path)
    assert recovery.records == (record(),)
