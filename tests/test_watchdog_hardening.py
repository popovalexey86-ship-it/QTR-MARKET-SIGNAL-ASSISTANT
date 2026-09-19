import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.watchdog.runtime.baseline_report import (
    build_baseline_report,
)
from market_signal_assistant.watchdog.runtime.gaps import GapLedger, SchedulingGap
from market_signal_assistant.watchdog.runtime.index import (
    JournalIndexSource,
    WatchdogIndexManager,
)
from market_signal_assistant.watchdog.runtime.lock import (
    DuplicateInstanceError,
    SingleInstanceLock,
)
from market_signal_assistant.watchdog.runtime.operator import inspect
from market_signal_assistant.watchdog.runtime.schedule import CompletedBucketScheduler
from market_signal_assistant.watchdog.runtime.storage import (
    StorageMonitor,
    StoragePolicy,
    StorageTelemetryJournal,
)

NOW = datetime(2026, 9, 19, 8, tzinfo=UTC)


def test_duplicate_instance_is_rejected_until_owner_releases(tmp_path: Path) -> None:
    first = SingleInstanceLock(tmp_path / "writer.lock")
    second = SingleInstanceLock(tmp_path / "writer.lock")
    first.acquire()

    with pytest.raises(DuplicateInstanceError):
        second.acquire()

    first.release()
    second.acquire()
    assert second.held is True
    second.release()


def test_forced_process_termination_releases_writer_lock(tmp_path: Path) -> None:
    lock_path = tmp_path / "writer.lock"
    script = (
        "import time; "
        "from pathlib import Path; "
        "from market_signal_assistant.watchdog.runtime.lock import SingleInstanceLock; "
        f"lock=SingleInstanceLock(Path({str(lock_path)!r})); "
        "lock.acquire(); print('ready', flush=True); time.sleep(60)"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "ready"
        process.terminate()
        process.wait(timeout=5)
        recovered = SingleInstanceLock(lock_path)
        recovered.acquire()
        assert recovered.held is True
        recovered.release()
    finally:
        if process.poll() is None:
            process.kill()


def test_sqlite_index_rebuilds_from_jsonl_after_corruption(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text(
        json.dumps(
            {
                "event_id": "event-1",
                "symbol": "BTCUSDT",
                "detected_at": NOW.isoformat(),
                "available_at": NOW.isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    manager = WatchdogIndexManager(
        tmp_path / "evidence.sqlite3",
        (JournalIndexSource("events", events, "event_id"),),
    )
    built = manager.ensure()

    assert built is not None
    assert built.indexed_records == 1
    assert manager.counts() == {"events": 1}

    manager.path.write_bytes(b"corrupt sqlite")
    rebuilt = manager.ensure()

    assert rebuilt is not None
    assert rebuilt.indexed_records == 1
    assert manager.counts() == {"events": 1}
    assert events.read_text(encoding="utf-8").count("event-1") == 1


def test_long_gap_is_explicit_and_backfill_policy_is_pit_safe(tmp_path: Path) -> None:
    scheduler = CompletedBucketScheduler(maximum_catchup_buckets=3)
    plan = scheduler.plan(
        now=NOW + timedelta(minutes=10),
        interval="1m",
        last_completed=NOW,
    )
    gap = SchedulingGap(
        "BTCUSDT",
        "1m",
        NOW + timedelta(minutes=1),
        NOW + timedelta(minutes=7),
        plan.omitted_bucket_count,
        NOW + timedelta(minutes=10),
    )
    ledger = GapLedger(tmp_path / "gaps.jsonl")

    assert plan.buckets == (
        NOW + timedelta(minutes=8),
        NOW + timedelta(minutes=9),
        NOW + timedelta(minutes=10),
    )
    assert plan.omitted_bucket_count == 7
    assert ledger.append(gap) is True
    assert ledger.append(gap) is False
    assert ledger.records()[0]["safe_backfill"] == (
        "OHLCV_BASELINE_ONLY_AT_RECOVERY_AVAILABILITY"
    )


def test_storage_pressure_is_measured_without_deleting_evidence(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "events" / "events.jsonl"
    evidence.parent.mkdir()
    evidence.write_text('{"event_id":"one"}\n', encoding="utf-8")
    monitor = StorageMonitor(
        tmp_path,
        StoragePolicy(minimum_free_bytes=10**30, minimum_free_ratio=0.99),
    )
    snapshot = monitor.snapshot(recorded_at=NOW)
    journal = StorageTelemetryJournal(tmp_path / "operational" / "storage.jsonl")

    assert snapshot.pressure is True
    assert snapshot.jsonl_bytes == evidence.stat().st_size
    assert journal.append(snapshot) is True
    assert evidence.exists()
    assert len(journal.records()) == 1


def test_operator_inspection_is_read_only_and_reports_stale_health(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir()
    health = state / "health.json"
    health.write_text(
        json.dumps({"last_loop_at": (NOW - timedelta(minutes=10)).isoformat()}),
        encoding="utf-8",
    )
    before = health.read_bytes()

    report = inspect(tmp_path, now=NOW)

    assert report["health_stale"] is True
    assert report["health_age_seconds"] == 600.0
    assert report["indexes"] == {"present": False}
    assert health.read_bytes() == before


def test_empty_baseline_report_is_observational_not_a_profit_claim(
    tmp_path: Path,
) -> None:
    report = build_baseline_report(tmp_path)

    assert report["observational_only"] is True
    assert report["thresholds_optimized"] is False
    assert report["event_count"] == 0
    assert report["event_rate_per_hour"] is None
