"""Offline contract tests; no VPS, Telegram API, or market provider calls."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any

import pytest
from watchdog_evidence_helpers import evidence

from market_signal_assistant.watchdog.events.journal import _event_payload
from market_signal_assistant.watchdog.runtime import observer
from market_signal_assistant.watchdog.runtime.index import IndexRecoveryError
from market_signal_assistant.watchdog.runtime.liveness import (
    RuntimeLiveness,
    SoakLivenessError,
)
from market_signal_assistant.watchdog.runtime.lock import (
    DuplicateInstanceError,
    SingleInstanceLock,
)
from market_signal_assistant.watchdog.telegram_adapter import WatchdogTelegramAdapter


class FakeChild:
    pid = 12345

    def __init__(self, result: int | None = None) -> None:
        self.result = result
        self.terminated = False

    def poll(self) -> int | None:
        return self.result

    def terminate(self) -> None:
        self.terminated = True
        self.result = 0

    def wait(self, timeout: float | None = None) -> int:
        assert timeout == 100
        assert self.result is not None
        return self.result


def _root(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "watchdog-live-observer-v1"
    journal = root / "events/events.jsonl"
    journal.parent.mkdir(parents=True)
    journal.touch()
    return root, journal


def _adapter(monkeypatch: pytest.MonkeyPatch, *, offset: int = 0) -> None:
    monkeypatch.setattr(
        observer,
        "adapter_state",
        lambda *_args, **_kwargs: observer.AdapterState(0, 0, 0, offset),
    )


def _checkpoint(root: Path) -> dict[str, Any]:
    return json.loads((root / "state/observer.json").read_text(encoding="utf-8"))


def test_normal_start_and_sigterm_stop_preserve_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _journal = _root(tmp_path)
    _adapter(monkeypatch)
    stop = Event()
    child = FakeChild()
    observed = 0

    def liveness(*_args: object, **_kwargs: object) -> RuntimeLiveness:
        nonlocal observed
        observed += 1
        if observed == 3:
            stop.set()
        now = datetime.now(UTC)
        return RuntimeLiveness(
            now,
            True,
            None,
            now,
            now,
            ("progress", observed),
            market_progress_due_at=now - timedelta(seconds=1),
        )

    monkeypatch.setattr(observer, "_runtime_liveness", liveness)
    assert (
        observer.supervise(
            root,
            tmp_path / "delivery.sqlite3",
            "adapter.service",
            lambda: child,
            stop=stop,
            preflight=False,
            probe_seconds=0.001,
        )
        == 0
    )
    assert child.terminated
    saved = _checkpoint(root)
    assert saved["status"] == "clean_stop"
    assert saved["accumulated_seconds"] > 0
    assert observed == 3


def test_worker_health_epoch_is_captured_before_child_starts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _journal = _root(tmp_path)
    _adapter(monkeypatch)
    stop = Event()
    spawned_at: datetime | None = None

    def start() -> FakeChild:
        nonlocal spawned_at
        spawned_at = datetime.now(UTC)
        return FakeChild()

    def liveness(*_args: object, **kwargs: object) -> RuntimeLiveness:
        assert spawned_at is not None
        assert kwargs["expected_started_at"] <= spawned_at
        stop.set()
        now = datetime.now(UTC)
        return RuntimeLiveness(now, True, None, now, now, ("new-worker",))

    monkeypatch.setattr(observer, "_runtime_liveness", liveness)
    assert (
        observer.supervise(
            root,
            tmp_path / "delivery.sqlite3",
            "adapter.service",
            start,
            stop=stop,
            preflight=False,
            probe_seconds=0.001,
        )
        == 0
    )


def test_manual_restart_resumes_only_saved_healthy_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _journal = _root(tmp_path)
    _adapter(monkeypatch)
    observer.save_checkpoint(
        root / "state/observer.json",
        seconds=17.5,
        status="clean_stop",
        reason="signal",
        child_pid=1,
    )
    stop = Event()
    stop.set()
    assert (
        observer.supervise(
            root,
            tmp_path / "delivery.sqlite3",
            "adapter.service",
            FakeChild,
            stop=stop,
            preflight=False,
        )
        == 0
    )
    assert _checkpoint(root)["accumulated_seconds"] == 17.5


def test_duplicate_supervisor_never_starts_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _journal = _root(tmp_path)
    _adapter(monkeypatch)
    with (
        SingleInstanceLock(root / "state/observer.lock"),
        pytest.raises(DuplicateInstanceError),
    ):
        observer.supervise(
            root,
            tmp_path / "delivery.sqlite3",
            "adapter.service",
            lambda: pytest.fail("second child started"),
            stop=Event(),
            preflight=False,
        )


def test_missing_existing_and_corrupt_checkpoint_fail_closed(tmp_path: Path) -> None:
    root, _journal = _root(tmp_path)
    assert (
        observer.load_checkpoint(root / "state/observer.json", root)["status"] == "new"
    )
    health = root / "state/health.json"
    health.parent.mkdir(parents=True)
    health.write_text("{}", encoding="utf-8")
    with pytest.raises(observer.ObserverFailure, match="checkpoint missing"):
        observer.load_checkpoint(root / "state/observer.json", root)
    path = root / "state/observer.json"
    path.write_text("{bad", encoding="utf-8")
    with pytest.raises(observer.ObserverFailure, match="corrupt"):
        observer.load_checkpoint(path, root)
    path.write_text(
        '{"version":1,"status":"integrity_failure","accumulated_seconds":8}',
        encoding="utf-8",
    )
    with pytest.raises(observer.ObserverFailure, match="requires review"):
        observer.load_checkpoint(path, root)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("runtime heartbeat is stale", observer.TRANSIENT_EXIT),
        ("market progress did not advance", observer.TRANSIENT_EXIT),
        ("runtime health is unreadable: JSONDecodeError", observer.INTEGRITY_EXIT),
    ],
)
def test_liveness_failure_is_classified_and_stops_without_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, message: str, expected: int
) -> None:
    root, _journal = _root(tmp_path)
    _adapter(monkeypatch)

    def fail(*_args: object, **_kwargs: object) -> RuntimeLiveness:
        raise SoakLivenessError(message)

    monkeypatch.setattr(observer, "_runtime_liveness", fail)
    child = FakeChild()
    assert (
        observer.supervise(
            root,
            tmp_path / "delivery.sqlite3",
            "adapter.service",
            lambda: child,
            stop=Event(),
            preflight=False,
            probe_seconds=0.001,
        )
        == expected
    )
    assert child.terminated
    assert _checkpoint(root)["status"] == (
        "integrity_failure"
        if expected == observer.INTEGRITY_EXIT
        else "transient_failure"
    )


def test_disk_and_memory_reserves() -> None:
    with pytest.raises(observer.ObserverFailure, match="disk reserve"):
        observer.check_capacity(
            observer.Capacity(40 * 1024**3, 3 * 1024**3, 2 * 1024**3),
            starting=True,
        )
    with pytest.raises(observer.ObserverFailure, match="memory"):
        observer.check_capacity(
            observer.Capacity(40 * 1024**3, 10 * 1024**3, 500 * 1024**2),
            starting=False,
        )


def test_adapter_unavailable_and_outbox_growth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _journal = _root(tmp_path)
    calls = 0

    def adapter(*_args: object, **_kwargs: object) -> observer.AdapterState:
        nonlocal calls
        calls += 1
        if calls == 1:
            return observer.AdapterState(0, 0, 0, 0)
        raise observer.ObserverFailure("Telegram adapter inactive", integrity=False)

    monkeypatch.setattr(observer, "adapter_state", adapter)
    child = FakeChild()
    assert (
        observer.supervise(
            root,
            tmp_path / "delivery.sqlite3",
            "adapter.service",
            lambda: child,
            stop=Event(),
            preflight=False,
            probe_seconds=0.001,
        )
        == observer.TRANSIENT_EXIT
    )
    assert child.terminated
    with pytest.raises(observer.ObserverFailure, match="backlog"):
        observer.check_adapter(observer.AdapterState(1001, 0, 0, 0))
    with pytest.raises(observer.ObserverFailure, match="backlog"):
        observer.check_adapter(observer.AdapterState(1, 3601, 0, 0))
    with pytest.raises(observer.ObserverFailure, match="blocked"):
        observer.check_adapter(observer.AdapterState(1, 0, 1, 0))


@pytest.mark.parametrize(
    "exit_code", [0, observer.TRANSIENT_EXIT, observer.INTEGRITY_EXIT]
)
def test_unexpected_worker_exit_is_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exit_code: int
) -> None:
    root, _journal = _root(tmp_path)
    _adapter(monkeypatch)
    child = FakeChild(exit_code)
    result = observer.supervise(
        root,
        tmp_path / "delivery.sqlite3",
        "adapter.service",
        lambda: child,
        stop=Event(),
        preflight=False,
        probe_seconds=0.001,
    )
    assert result == (
        observer.INTEGRITY_EXIT
        if exit_code == observer.INTEGRITY_EXIT
        else observer.TRANSIENT_EXIT
    )
    assert _checkpoint(root)["status"] != "clean_stop"


class NoSend:
    def send(self, token: str, chat_id: str, text: str) -> None:
        pytest.fail("a Telegram request was attempted")


def test_new_source_uses_new_sqlite_and_old_cursor_is_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = tmp_path / "old/events/events.jsonl"
    new = tmp_path / "new/events/events.jsonl"
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    old.write_bytes(b"historical-event\n")
    new.touch()
    old_db = tmp_path / "old-state/delivery.sqlite3"
    new_db = tmp_path / "new-state/delivery.sqlite3"
    old_adapter = WatchdogTelegramAdapter(
        old, old_db, sender=NoSend(), token="offline", chat_id="123"
    )
    old_offset = old_adapter.db.execute("SELECT offset FROM cursor").fetchone()[0]
    old_adapter.close()

    class Capture:
        sent: list[str] = []

        def send(self, token: str, chat_id: str, text: str) -> None:
            self.sent.append(text)

    sender = Capture()
    new_adapter = WatchdogTelegramAdapter(
        new, new_db, sender=sender, token="offline", chat_id="123"
    )
    assert new_adapter.db.execute("SELECT offset FROM cursor").fetchone()[0] == 0
    assert new_adapter.db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    event = evidence()
    with new.open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode("utf-8"))
    assert new_adapter.run_once() == (1, True)
    assert len(sender.sent) == 1
    assert event.event_id in sender.sent[0]
    assert (
        sqlite3.connect(old_db).execute("SELECT offset FROM cursor").fetchone()[0]
        == old_offset
    )
    new_adapter.close()
    monkeypatch.setattr(observer, "_unit_state", lambda *_args: None)
    assert (
        observer.adapter_state(new, new_db, "adapter.service", 100).cursor_offset
        == new.stat().st_size
    )
    original_inode = new.stat().st_ino
    new.unlink()
    new.touch()
    if new.stat().st_ino == original_inode:
        pytest.skip("filesystem reused inode")
    with pytest.raises(observer.ObserverFailure, match="identity"):
        observer.adapter_state(new, new_db, "adapter.service", 100)


def test_outbox_age_is_durable_and_old_unknown_age_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, journal = _root(tmp_path)
    db_path = tmp_path / "delivery.sqlite3"
    adapter = WatchdogTelegramAdapter(
        journal, db_path, sender=NoSend(), token="offline", chat_id="123"
    )
    adapter.db.execute(
        "INSERT INTO outbox(event_id,payload_hash,text,status,enqueued_at) "
        "VALUES('e','h','text','PENDING',100)"
    )
    adapter.close()
    monkeypatch.setattr(observer, "_unit_state", lambda *_args: None)
    state = observer.adapter_state(journal, db_path, "adapter.service", 3701)
    assert state.pending == 1 and state.oldest_age_seconds == 3601
    with pytest.raises(observer.ObserverFailure, match="backlog"):
        observer.check_adapter(state)
    with sqlite3.connect(db_path) as db:
        db.execute("UPDATE outbox SET enqueued_at=0")
    state = observer.adapter_state(journal, db_path, "adapter.service", 3701)
    assert state.oldest_age_seconds == 3701
    with sqlite3.connect(db_path) as db:
        db.execute("UPDATE outbox SET enqueued_at=5000")
    with pytest.raises(observer.ObserverFailure, match="in future"):
        observer.adapter_state(journal, db_path, "adapter.service", 3701)


def test_legacy_adapter_schema_migrates_without_cursor_reset(tmp_path: Path) -> None:
    _root_path, journal = _root(tmp_path)
    db_path = tmp_path / "legacy.sqlite3"
    stat = journal.stat()
    with sqlite3.connect(db_path) as db:
        db.execute(
            "CREATE TABLE cursor (id INTEGER PRIMARY KEY,device INTEGER,inode INTEGER,"
            "offset INTEGER NOT NULL,skip_partial INTEGER,destination_hash TEXT,"
            "next_send REAL)"
        )
        db.execute(
            "CREATE TABLE outbox (event_id TEXT PRIMARY KEY,payload_hash TEXT,"
            "text TEXT,status TEXT,attempts INTEGER,next_attempt REAL,last_error TEXT)"
        )
        db.execute(
            "INSERT INTO cursor VALUES (1,?,?,?,?,?,0)",
            (stat.st_dev, stat.st_ino, 0, 0, hashlib.sha256(b"123").hexdigest()),
        )
        db.execute("INSERT INTO outbox VALUES ('old','h',NULL,'SENT',1,0,NULL)")
    adapter = WatchdogTelegramAdapter(
        journal, db_path, sender=NoSend(), token="offline", chat_id="123"
    )
    columns = {row[1] for row in adapter.db.execute("PRAGMA table_info(outbox)")}
    assert "enqueued_at" in columns
    assert adapter.db.execute("SELECT offset FROM cursor").fetchone() == (0,)
    assert adapter.db.execute("SELECT enqueued_at FROM outbox").fetchone() == (0.0,)
    adapter.close()


def test_worker_sigterm_during_write_calls_existing_runtime_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Runtime:
        started = False
        stopped = False
        writing = False

        def start(self) -> None:
            self.started = True
            self.writing = True

        def stop(self, *, timeout: float) -> None:
            assert timeout == 45
            assert self.writing
            self.writing = False  # runtime.stop waits for in-flight writes
            self.stopped = True

    runtime = Runtime()
    monkeypatch.setattr(
        observer,
        "build_bybit_shadow_runtime",
        lambda *_a, **_kw: type("B", (), {"runtime": runtime})(),
    )

    handlers: dict[int, Any] = {}

    class Signalled:
        def __init__(self) -> None:
            self.signalled = False

        def set(self) -> None:
            self.signalled = True

        def is_set(self) -> bool:
            return self.signalled

        def wait(self, _timeout: float) -> bool:
            handlers[observer.signal.SIGTERM](observer.signal.SIGTERM, None)
            return self.signalled

    monkeypatch.setattr(observer, "Event", Signalled)
    monkeypatch.setattr(
        observer.signal,
        "signal",
        lambda signum, callback: handlers.update({signum: callback}),
    )
    assert observer.worker(tmp_path, 8, 60) == 0
    assert runtime.started and runtime.stopped


def test_worker_chronology_failure_is_integrity_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Runtime:
        running = True
        fatal_error = ValueError("State evaluations must be chronological.")
        stopped = False

        def start(self) -> None:
            pass

        def stop(self, *, timeout: float) -> None:
            self.stopped = True

    runtime = Runtime()
    monkeypatch.setattr(
        observer,
        "build_bybit_shadow_runtime",
        lambda *_a, **_kw: type("B", (), {"runtime": runtime})(),
    )

    class Once:
        def is_set(self) -> bool:
            return False

        def wait(self, _timeout: float) -> bool:
            return False

    monkeypatch.setattr(observer, "Event", Once)
    monkeypatch.setattr(observer.signal, "signal", lambda *_a: None)
    assert observer.worker(tmp_path, 8, 60) == observer.INTEGRITY_EXIT
    assert runtime.stopped


def test_worker_recovery_integrity_failure_never_starts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_build(*_args: object, **_kwargs: object) -> None:
        raise IndexRecoveryError("SQLite integrity check failed")

    monkeypatch.setattr(observer, "build_bybit_shadow_runtime", fail_build)
    monkeypatch.setattr(observer.signal, "signal", lambda *_args: None)
    assert observer.worker(tmp_path, 8, 60) == observer.INTEGRITY_EXIT


def test_worker_failure_classification_preserves_journal_integrity() -> None:
    assert observer._failure_exit(
        OSError("Indexed journal record is not readable")
    ) == (observer.INTEGRITY_EXIT)
    assert observer._failure_exit(TimeoutError("provider timeout")) == (
        observer.TRANSIENT_EXIT
    )


def test_checkpoint_write_failure_fails_closed_and_stops_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _journal = _root(tmp_path)
    _adapter(monkeypatch)
    child = FakeChild()
    real_save = observer.save_checkpoint
    writes = 0

    def fail_second(path: Path, **kwargs: Any) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise OSError("simulated checkpoint write interruption")
        real_save(path, **kwargs)

    monkeypatch.setattr(observer, "save_checkpoint", fail_second)
    assert (
        observer.supervise(
            root,
            tmp_path / "delivery.sqlite3",
            "adapter.service",
            lambda: child,
            stop=Event(),
            preflight=False,
            probe_seconds=0.001,
        )
        == observer.INTEGRITY_EXIT
    )
    assert child.terminated
    assert _checkpoint(root)["status"] == "integrity_failure"
