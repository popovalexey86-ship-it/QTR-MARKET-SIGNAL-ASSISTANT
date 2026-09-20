from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from watchdog_evidence_helpers import evidence

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.journal import (
    ImmutableJsonlJournal,
    JournalConflictError,
)


def test_event_journal_is_immutable_idempotent_and_restart_safe(tmp_path: Path) -> None:
    path = tmp_path / "watchdog" / "events" / "events.jsonl"
    journal = WatchdogEventJournal(path)
    event = evidence()

    assert journal.append(event) is True
    assert journal.append(event) is False
    assert WatchdogEventJournal(path).append(event) is False
    assert WatchdogEventJournal(path).records() == (event,)
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1

    with pytest.raises(JournalConflictError, match="Conflicting"):
        WatchdogEventJournal(path).append(
            replace(event, price_at_detection=101.0)
        )

    with pytest.raises(ValueError, match="not deterministic"):
        replace(event, event_id="arbitrary-id")


def test_crash_before_append_is_recovered_by_deterministic_replay(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    event = evidence()
    assert WatchdogEventJournal(path).records() == ()

    restarted = WatchdogEventJournal(path)
    assert restarted.append(event) is True
    assert restarted.records() == (event,)


def test_partial_and_corrupted_tail_are_explicit_and_do_not_hide_valid_events(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    first = evidence()
    journal = WatchdogEventJournal(path)
    journal.append(first)
    with path.open("ab") as stream:
        stream.write(b"{partial")

    partial = WatchdogEventJournal(path)
    assert partial.records() == (first,)
    assert partial.recovery.partial_tail is True

    second = evidence(
        "watchdog:ABCUSDT:20260918T080500.000000Z",
        detected_at=first.detected_at + timedelta(minutes=5),
    )
    assert partial.append(second) is True
    recovered = WatchdogEventJournal(path)
    assert recovered.records() == (first, second)
    assert recovered.recovery.partial_tail is False
    assert recovered.recovery.corrupted_line_numbers == (2,)


def test_complete_valid_tail_without_newline_is_finalized_not_duplicated(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.jsonl"
    event = evidence()
    WatchdogEventJournal(source).append(event)
    raw = source.read_bytes().rstrip(b"\n")
    path = tmp_path / "tail.jsonl"
    path.write_bytes(raw)

    journal = WatchdogEventJournal(path)
    assert journal.recovery.partial_tail is True
    assert journal.append(event) is False
    assert WatchdogEventJournal(path).records() == (event,)


def test_journal_indexes_hashes_without_retaining_payload_copies(
    tmp_path: Path,
) -> None:
    journal = ImmutableJsonlJournal(tmp_path / "bounded.jsonl", id_field="id")
    payload: dict[str, object] = {"id": "one", "body": "x" * 100_000}

    assert journal.append("one", payload) is True
    assert not hasattr(journal, "_payloads")
    assert not hasattr(journal, "_records")
    assert journal.records() == (payload,)
