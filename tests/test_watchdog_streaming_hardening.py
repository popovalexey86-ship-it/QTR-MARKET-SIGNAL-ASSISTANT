"""Bounded JSONL and fail-closed recovery regressions."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.watchdog.baselines import (
    BaselineObservation,
    BaselineStateError,
    JsonBaselineStore,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal
from market_signal_assistant.watchdog.jsonl_io import (
    MAX_RECORD_BYTES,
    JsonlLineTooLong,
)
from market_signal_assistant.watchdog.runtime import index as index_module
from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.index import (
    IndexRecoveryError,
    JournalIndexSource,
    WatchdogIndexManager,
)
from market_signal_assistant.watchdog.runtime.operator import inspect


def _line(value: dict[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True).encode() + b"\n"


def test_journal_streams_unique_ids_through_disk_index(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    with path.open("wb") as stream:
        for number in range(2000):
            stream.write(_line({"id": str(number), "value": number}))
        stream.write(_line({"id": "0", "value": 0}))
    journal = ImmutableJsonlJournal(path, id_field="id")
    assert sum(1 for _ in journal.iter_records()) == 2000
    assert journal.retained_index_entries == 0
    assert journal.get("1999") == {"id": "1999", "value": 1999}
    assert journal.append("2000", {"id": "2000", "value": 2000})
    assert sum(1 for _ in journal.iter_records()) == 2001


def test_oversized_line_is_rejected_before_write_and_during_recovery(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.jsonl"
    journal = ImmutableJsonlJournal(path, id_field="id")
    with pytest.raises(ValueError, match="size limit"):
        journal.append("large", {"id": "large", "body": "x" * MAX_RECORD_BYTES})
    assert not path.exists() or path.stat().st_size == 0
    path.write_bytes(b"x" * (MAX_RECORD_BYTES + 1))
    with pytest.raises(JsonlLineTooLong):
        ImmutableJsonlJournal(path, id_field="id")


@pytest.mark.parametrize("change", ["replace", "truncate"])
def test_journal_source_identity_and_offset_fail_closed(
    tmp_path: Path, change: str
) -> None:
    path = tmp_path / "events.jsonl"
    journal = ImmutableJsonlJournal(path, id_field="id")
    assert journal.append("one", {"id": "one"})
    if change == "replace":
        replacement = tmp_path / "replacement.jsonl"
        replacement.write_bytes(path.read_bytes())
        assert replacement.stat().st_ino != path.stat().st_ino
        os.replace(replacement, path)
    else:
        path.write_bytes(b"")
    with pytest.raises(ValueError, match="replaced|truncated"):
        journal.get("one")


def test_incremental_index_does_not_rescan_old_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "events.jsonl"
    with source.open("wb") as stream:
        for number in range(2000):
            stream.write(_line({"event_id": str(number)}))
    manager = WatchdogIndexManager(
        tmp_path / "evidence.sqlite3",
        (JournalIndexSource("events", source, "event_id"),),
    )
    first = manager.ensure()
    assert first is not None and first.indexed_records == 2000
    first_size = source.stat().st_size
    with source.open("ab") as stream:
        stream.write(_line({"event_id": "new"}))
    reads = 0
    original_read = index_module.read_bounded_line

    def tracked_read(stream: object) -> bytes:
        nonlocal reads
        reads += 1
        return original_read(stream)  # type: ignore[arg-type]

    monkeypatch.setattr(index_module, "read_bounded_line", tracked_read)
    second = manager.ensure()
    assert second is not None and second.indexed_records == 1
    assert reads == 2  # one appended record and one EOF probe
    with sqlite3.connect(manager.path) as db:
        assert db.execute(
            "SELECT source_offset FROM sources WHERE kind='events'"
        ).fetchone() == (source.stat().st_size,)
        assert db.execute(
            "SELECT byte_offset FROM records WHERE logical_id='new'"
        ).fetchone() == (first_size,)


@pytest.mark.parametrize("change", ["replace", "truncate", "malformed"])
def test_index_recovery_rejects_changed_or_invalid_source(
    tmp_path: Path, change: str
) -> None:
    source = tmp_path / "events.jsonl"
    source.write_bytes(_line({"event_id": "one"}))
    manager = WatchdogIndexManager(
        tmp_path / "evidence.sqlite3",
        (JournalIndexSource("events", source, "event_id"),),
    )
    assert manager.ensure() is not None
    if change == "replace":
        replacement = tmp_path / "replacement.jsonl"
        replacement.write_bytes(source.read_bytes())
        os.replace(replacement, source)
    elif change == "truncate":
        source.write_bytes(b"")
    else:
        with source.open("ab") as stream:
            stream.write(b"not-json\n")
    with pytest.raises(IndexRecoveryError):
        manager.ensure()


def test_critical_journal_corruption_blocks_runtime_recovery(tmp_path: Path) -> None:
    root = tmp_path / "data"
    path = root / "events" / "events.jsonl"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not-json\n")
    with pytest.raises(ValueError, match="corrupt or incomplete"):
        build_bybit_shadow_runtime(root)


def test_operator_history_is_streamed_and_gap_output_is_capped(tmp_path: Path) -> None:
    root = tmp_path / "data"
    operational = root / "operational"
    operational.mkdir(parents=True)
    with (operational / "runtime.jsonl").open("wb") as stream:
        for number in range(1000):
            stream.write(_line({"event_type": "THROTTLE_WAIT", "n": number}))
    with (operational / "gaps.jsonl").open("wb") as stream:
        for number in range(1000):
            stream.write(_line({"n": number}))
    result = inspect(root)
    assert result["audit_records"] == 1000
    assert result["audit_types"] == {"THROTTLE_WAIT": 1000}
    assert result["gaps_total"] == 1000
    assert result["gaps_truncated"] is True
    assert len(result["gaps"]) == 100  # type: ignore[arg-type]


def test_baseline_recovery_retains_only_per_key_ring_after_long_update_log(
    tmp_path: Path,
) -> None:
    store = JsonBaselineStore(tmp_path / "baselines.json")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    store.append_many(
        tuple(
            BaselineObservation(
                "BTCUSDT", "range", float(number),
                start + timedelta(minutes=number),
                start + timedelta(minutes=number),
            )
            for number in range(2000)
        )
    )
    engine = RollingBaselineEngine(store, minimum_samples=2, maximum_samples=3)
    assert engine.retained_counts == (1, 3, 3)
    assert [item.value for item in engine.observations] == [1997.0, 1998.0, 1999.0]
    assert store.pending_update_count == 0


def test_baseline_recovery_rejects_incomplete_update_line(tmp_path: Path) -> None:
    store = JsonBaselineStore(tmp_path / "baselines.json")
    store.path.with_name("baselines.updates.jsonl").write_bytes(b'{"symbol":')
    with pytest.raises(BaselineStateError, match="Incomplete"):
        RollingBaselineEngine(store, minimum_samples=2, maximum_samples=3)


def test_baseline_recovery_accepts_crash_before_compacted_log_unlink(
    tmp_path: Path,
) -> None:
    store = JsonBaselineStore(tmp_path / "baselines.json")
    updates = store.path.with_name("baselines.updates.jsonl")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    store.append_many(
        tuple(
            BaselineObservation(
                "BTCUSDT", "range", float(number),
                start + timedelta(minutes=number),
                start + timedelta(minutes=number),
            )
            for number in range(20)
        )
    )
    prior_log = updates.read_bytes()
    first = RollingBaselineEngine(store, minimum_samples=2, maximum_samples=3)
    assert first.retained_counts == (1, 3, 3)
    updates.write_bytes(prior_log)
    recovered = RollingBaselineEngine(store, minimum_samples=2, maximum_samples=3)
    assert recovered.retained_counts == (1, 3, 3)
    assert [item.value for item in recovered.observations] == [17.0, 18.0, 19.0]


def test_baseline_recovery_rejects_conflicting_snapshot_overlap(
    tmp_path: Path,
) -> None:
    store = JsonBaselineStore(tmp_path / "baselines.json")
    updates = store.path.with_name("baselines.updates.jsonl")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    item = BaselineObservation("BTCUSDT", "range", 1.0, start, start)
    store.append_many((item,))
    original_log = updates.read_bytes()
    store.save((item,))
    updates.write_bytes(original_log.replace(b'"value":1.0', b'"value":2.0'))
    with pytest.raises(BaselineStateError, match="conflicts"):
        RollingBaselineEngine(store, minimum_samples=1, maximum_samples=3)
