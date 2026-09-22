from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_signal_assistant.watchdog.baselines import (
    BaselineObservation,
    JsonBaselineStore,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal
from market_signal_assistant.watchdog.runtime.audit import (
    OperationalAuditJournal,
    OperationalEvent,
    OperationalEventType,
)
from market_signal_assistant.watchdog.runtime.index import (
    JournalIndexSource,
    WatchdogIndexManager,
)

NOW = datetime(2026, 9, 22, 8, 0, tzinfo=UTC)


def test_operational_rollup_retains_count_window_and_symbols(tmp_path: Path) -> None:
    audit = OperationalAuditJournal(tmp_path / "runtime.jsonl")
    for index, symbol in enumerate(("AAAUSDT", "BBBUSDT", "AAAUSDT")):
        audit.append_rollup(
            OperationalEvent(
                OperationalEventType.SCHEDULER_TIMEOUT,
                NOW + timedelta(seconds=index),
                (("stage", "running"),),
                symbol,
            ),
            signature="scheduler_timeout:running",
        )

    assert len(audit.records()) == 1
    assert audit.rollups() == (
        {
            "signature": "scheduler_timeout:running",
            "event_type": "SCHEDULER_TIMEOUT",
            "count": 3,
            "first_seen": NOW.isoformat(),
            "last_seen": (NOW + timedelta(seconds=2)).isoformat(),
            "affected_symbols": ["AAAUSDT", "BBBUSDT"],
            "latest_details": {"stage": "running"},
        },
    )


def test_jsonl_logical_index_is_disk_backed_and_rebuildable(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    journal = ImmutableJsonlJournal(path, id_field="id")
    for index in range(200):
        assert journal.append(str(index), {"id": str(index), "value": index})

    assert journal.retained_index_entries == 0
    assert journal.recovery.record_count == 200
    connection = sqlite3.connect(journal.index_path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM records").fetchone() == (200,)
    finally:
        connection.close()

    journal.index_path.unlink()
    recovered = ImmutableJsonlJournal(path, id_field="id")
    assert recovered.retained_index_entries == 0
    assert recovered.get("199") == {"id": "199", "value": 199}


def test_evidence_index_advances_only_new_jsonl_bytes(tmp_path: Path) -> None:
    source = tmp_path / "events.jsonl"
    journal = ImmutableJsonlJournal(source, id_field="event_id")
    journal.append("one", {"event_id": "one", "symbol": "AAAUSDT"})
    manager = WatchdogIndexManager(
        tmp_path / "evidence.sqlite3",
        (JournalIndexSource("events", source, "event_id"),),
    )

    first = manager.ensure()
    assert first is not None and first.indexed_records == 1
    journal.append("two", {"event_id": "two", "symbol": "BBBUSDT"})
    incremental = manager.ensure()

    assert incremental is not None and incremental.indexed_records == 1
    assert manager.counts() == {"events": 2}


def test_baseline_rings_and_replay_log_remain_strictly_bounded(
    tmp_path: Path,
) -> None:
    store = JsonBaselineStore(tmp_path / "baselines.json")
    engine = RollingBaselineEngine(store, minimum_samples=2, maximum_samples=3)
    for index in range(10):
        for symbol in ("AAAUSDT", "BBBUSDT"):
            observed = NOW + timedelta(minutes=index)
            engine.observe(
                BaselineObservation(
                    symbol,
                    "range",
                    float(index),
                    observed,
                    observed,
                ),
                detected_at=observed,
            )

    assert engine.retained_counts == (2, 6, 3)
    assert len(engine.observations) == 6
    restarted = RollingBaselineEngine(store, minimum_samples=2, maximum_samples=3)
    assert restarted.retained_counts == (2, 6, 3)
    assert {item.value for item in restarted.observations} == {7.0, 8.0, 9.0}
