"""Fail-closed, continuous supervisor for an isolated public-market Watchdog.

The worker uses the frozen runtime composition. This supervisor never restarts a
failed worker and never sends Telegram messages or trades.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from typing import Any, Protocol

from market_signal_assistant.watchdog.journal import JournalConflictError
from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.detection_only import (
    RuntimeModeError,
    ensure_dataset_mode,
)
from market_signal_assistant.watchdog.runtime.index import IndexRecoveryError
from market_signal_assistant.watchdog.runtime.liveness import (
    ConfirmedHealthyClock,
    SoakLivenessError,
)
from market_signal_assistant.watchdog.runtime.lock import (
    DuplicateInstanceError,
    SingleInstanceLock,
)
from market_signal_assistant.watchdog.runtime.models import ShadowRuntimeConfig
from market_signal_assistant.watchdog.runtime.schedule import BucketCursorError
from market_signal_assistant.watchdog.runtime.soak import _runtime_liveness

INTEGRITY_EXIT = 78
TRANSIENT_EXIT = 75
MIN_DISK_FREE = 4 * 1024**3
MIN_DISK_RATIO = 0.10
MIN_START_MEMORY = 1024**3
MIN_RUNNING_MEMORY = 512 * 1024**2
MAX_OUTBOX = 1000
MAX_OUTBOX_AGE = 3600.0


class ObserverFailure(RuntimeError):
    def __init__(self, reason: str, *, integrity: bool) -> None:
        super().__init__(reason)
        self.integrity = integrity


class Child(Protocol):
    pid: int

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


@dataclass(frozen=True, slots=True)
class Capacity:
    disk_total: int
    disk_free: int
    memory_available: int


@dataclass(frozen=True, slots=True)
class AdapterState:
    pending: int
    oldest_age_seconds: float
    blocked: int
    cursor_offset: int


def _available_memory() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise ObserverFailure("MemAvailable unavailable", integrity=False)


def capacity(root: Path) -> Capacity:
    usage = shutil.disk_usage(root)
    return Capacity(usage.total, usage.free, _available_memory())


def check_capacity(value: Capacity, *, starting: bool) -> None:
    if value.disk_free < max(MIN_DISK_FREE, int(value.disk_total * MIN_DISK_RATIO)):
        raise ObserverFailure("disk reserve breached", integrity=True)
    minimum = MIN_START_MEMORY if starting else MIN_RUNNING_MEMORY
    if value.memory_available < minimum:
        raise ObserverFailure("available memory below reserve", integrity=False)


def _unit_state(unit: str, journal: Path, state_db: Path) -> None:
    result = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "-p",
            "ActiveState",
            "-p",
            "ExecStart",
            "--no-pager",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    output = result.stdout
    if result.returncode or "ActiveState=active" not in output:
        raise ObserverFailure("Telegram adapter inactive", integrity=False)
    if (
        f"--event-journal {journal}" not in output
        or f"--state-db {state_db}" not in output
    ):
        raise ObserverFailure("Telegram adapter source/state mismatch", integrity=True)


def adapter_state(
    journal: Path,
    state_db: Path,
    unit: str,
    now: float,
    *,
    verify_integrity: bool = False,
) -> AdapterState:
    _unit_state(unit, journal, state_db)
    if not journal.is_file() or not state_db.is_file():
        raise ObserverFailure("Telegram source or state missing", integrity=True)
    try:
        with closing(
            sqlite3.connect(f"file:{state_db}?mode=ro", uri=True, timeout=10)
        ) as db:
            if verify_integrity and db.execute("PRAGMA quick_check").fetchone() != (
                "ok",
            ):
                raise ObserverFailure(
                    "Telegram SQLite integrity failure", integrity=True
                )
            row = db.execute(
                "SELECT device,inode,offset FROM cursor WHERE id=1"
            ).fetchone()
            if row is None:
                raise ObserverFailure("Telegram cursor missing", integrity=True)
            pending, first_seen, blocked = db.execute(
                "SELECT count(*),min(enqueued_at),"
                "sum(CASE WHEN status='BLOCKED' THEN 1 ELSE 0 END) "
                "FROM outbox WHERE status IN ('PENDING','INFLIGHT','BLOCKED')"
            ).fetchone()
    except (sqlite3.DatabaseError, OSError) as error:
        raise ObserverFailure("Telegram SQLite unreadable", integrity=True) from error
    stat = journal.stat()
    if (
        (row[0], row[1]) != (stat.st_dev, stat.st_ino)
        or not isinstance(row[2], int)
        or not 0 <= row[2] <= stat.st_size
    ):
        raise ObserverFailure(
            "Telegram journal identity/offset mismatch", integrity=True
        )
    if pending and first_seen is None:
        raise ObserverFailure("Telegram outbox age unknown", integrity=True)
    if first_seen is not None and (
        not math.isfinite(float(first_seen)) or float(first_seen) > now + 60
    ):
        raise ObserverFailure(
            "Telegram outbox enqueue time is in future", integrity=True
        )
    return AdapterState(
        int(pending),
        max(0.0, now - float(first_seen)) if first_seen is not None else 0.0,
        int(blocked or 0),
        int(row[2]),
    )


def check_adapter(value: AdapterState) -> None:
    if value.blocked:
        raise ObserverFailure("Telegram delivery blocked", integrity=False)
    if value.pending > MAX_OUTBOX or value.oldest_age_seconds > MAX_OUTBOX_AGE:
        raise ObserverFailure("Telegram outbox backlog exceeded", integrity=False)


def load_checkpoint(path: Path, root: Path) -> dict[str, Any]:
    if not path.exists():
        if any(
            (root / name).exists()
            for name in (
                "state/health.json",
                "state/buckets.json",
                "state/pending.json",
                "outcomes/outcomes.jsonl",
                "operational/runtime.jsonl",
            )
        ):
            raise ObserverFailure(
                "observer checkpoint missing for existing runtime", integrity=True
            )
        return {"version": 1, "accumulated_seconds": 0.0, "status": "new"}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(value, dict)
            or value.get("version") != 1
            or value.get("status") not in {"starting", "running", "clean_stop"}
            or not isinstance(value.get("accumulated_seconds"), (int, float))
            or isinstance(value.get("accumulated_seconds"), bool)
            or not 0 <= float(value["accumulated_seconds"]) < float("inf")
        ):
            raise ValueError("invalid observer checkpoint")
    except (OSError, ValueError, TypeError) as error:
        raise ObserverFailure(
            "observer checkpoint corrupt or requires review", integrity=True
        ) from error
    return value


def save_checkpoint(
    path: Path, *, seconds: float, status: str, reason: str, child_pid: int | None
) -> None:
    payload = {
        "version": 1,
        "accumulated_seconds": seconds,
        "status": status,
        "reason": reason,
        "child_pid": child_pid,
        "checkpointed_at": datetime.now(UTC).isoformat(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise


class _ChildLiveness:
    def __init__(self, child: Child) -> None:
        self.child = child

    @property
    def running(self) -> bool:
        return self.child.poll() is None

    @property
    def fatal_error(self) -> Exception | None:
        return None


def _stop_child(child: Child) -> int:
    if child.poll() is None:
        child.terminate()
    try:
        return child.wait(timeout=100)
    except subprocess.TimeoutExpired as error:
        # The systemd unit must use SendSIGKILL=no. Preserve evidence for review.
        raise ObserverFailure(
            "worker graceful shutdown timed out", integrity=True
        ) from error


def supervise(
    root: Path,
    state_db: Path,
    unit: str,
    child_factory: Callable[[], Child],
    *,
    stop: Event,
    probe_seconds: float = 5.0,
    liveness_seconds: float = 300.0,
    preflight: bool = True,
) -> int:
    journal = root / "events" / "events.jsonl"
    checkpoint_path = root / "state" / "observer.json"
    with SingleInstanceLock(root / "state" / "observer.lock"):
        checkpoint = load_checkpoint(checkpoint_path, root)
        if preflight:
            check_capacity(capacity(root), starting=True)
        adapter = adapter_state(
            journal, state_db, unit, time.time(), verify_integrity=True
        )
        check_adapter(adapter)
        if checkpoint["status"] == "new" and (
            adapter.cursor_offset or journal.stat().st_size
        ):
            raise ObserverFailure(
                "new source is not empty at adapter bootstrap", integrity=True
            )
        save_checkpoint(
            checkpoint_path,
            seconds=float(checkpoint["accumulated_seconds"]),
            status="starting",
            reason="worker_start_requested",
            child_pid=None,
        )
        # Capture this before spawning: the child can write health immediately.
        started_at = datetime.now(UTC)
        try:
            child = child_factory()
        except Exception as error:  # noqa: BLE001 - no worker was launched
            save_checkpoint(
                checkpoint_path,
                seconds=float(checkpoint["accumulated_seconds"]),
                status="transient_failure",
                reason=f"worker start error: {type(error).__name__}",
                child_pid=None,
            )
            return TRANSIENT_EXIT
        clock = ConfirmedHealthyClock(
            float(checkpoint["accumulated_seconds"]),
            stale_after_seconds=liveness_seconds,
        )
        last_save = time.monotonic()
        last_integrity = last_save
        status, reason, code = "clean_stop", "signal", 0
        try:
            save_checkpoint(
                checkpoint_path,
                seconds=clock.accumulated_seconds,
                status="starting",
                reason="worker_start",
                child_pid=child.pid,
            )
            while not stop.wait(probe_seconds):
                child_code = child.poll()
                if child_code is not None:
                    raise ObserverFailure(
                        f"unexpected worker exit code {child_code}",
                        integrity=child_code == INTEGRITY_EXIT,
                    )
                if preflight:
                    check_capacity(capacity(root), starting=False)
                verify = time.monotonic() - last_integrity >= 900
                check_adapter(
                    adapter_state(
                        journal, state_db, unit, time.time(), verify_integrity=verify
                    )
                )
                if verify:
                    last_integrity = time.monotonic()
                try:
                    advanced = clock.observe(
                        _runtime_liveness(
                            _ChildLiveness(child),
                            datetime.now(UTC),
                            root / "state" / "health.json",
                            expected_started_at=started_at,
                        ),
                        monotonic_now=time.monotonic(),
                    )
                except SoakLivenessError as error:
                    detail = str(error)
                    raise ObserverFailure(
                        f"market liveness: {detail}",
                        integrity=(
                            "unreadable" in detail
                            or "timestamp is invalid" in detail
                            or "timezone-naive" in detail
                        ),
                    ) from error
                if advanced or time.monotonic() - last_save >= 30:
                    save_checkpoint(
                        checkpoint_path,
                        seconds=clock.accumulated_seconds,
                        status="running",
                        reason="healthy",
                        child_pid=child.pid,
                    )
                    last_save = time.monotonic()
        except ObserverFailure as error:
            status = "integrity_failure" if error.integrity else "transient_failure"
            reason = str(error)
            code = INTEGRITY_EXIT if error.integrity else TRANSIENT_EXIT
        except Exception as error:  # noqa: BLE001 - fail closed and stop the worker
            status, reason, code = (
                "integrity_failure",
                f"supervisor error: {type(error).__name__}",
                INTEGRITY_EXIT,
            )
        try:
            worker_code = _stop_child(child)
            if worker_code != 0 and code == 0:
                status, reason, code = (
                    "transient_failure",
                    "worker stop failed",
                    TRANSIENT_EXIT,
                )
        except Exception as error:  # noqa: BLE001 - never claim a clean stop
            status, reason, code = (
                "integrity_failure",
                f"worker shutdown error: {type(error).__name__}",
                INTEGRITY_EXIT,
            )
        save_checkpoint(
            checkpoint_path,
            seconds=clock.accumulated_seconds,
            status=status,
            reason=reason,
            child_pid=child.pid,
        )
        return code


def worker(
    root: Path,
    symbols_per_loop: int,
    api_calls_per_minute: int,
    *,
    detection_only: bool = False,
) -> int:
    stop = Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        runtime = build_bybit_shadow_runtime(
            root,
            config=ShadowRuntimeConfig(
                maximum_symbols_per_loop=symbols_per_loop,
                api_calls_per_minute=api_calls_per_minute,
            ),
            detection_only=detection_only,
        ).runtime
    except Exception as error:  # noqa: BLE001 - classify recovery failure
        return _failure_exit(error)
    if stop.is_set():
        return 0
    try:
        runtime.start()
    except Exception as error:  # noqa: BLE001 - fail closed on worker startup
        return _failure_exit(error)
    result = 0
    try:
        while not stop.wait(1.0):
            failure = runtime.fatal_error
            if failure is not None:
                result = _failure_exit(failure)
                break
            if not runtime.running:
                result = TRANSIENT_EXIT
                break
    finally:
        try:
            runtime.stop(timeout=45.0)
        except Exception:
            result = INTEGRITY_EXIT
    return result


def _failure_exit(error: Exception) -> int:
    detail = str(error).lower()
    if (
        isinstance(
            error,
            (
                DuplicateInstanceError,
                IndexRecoveryError,
                BucketCursorError,
                RuntimeModeError,
                JournalConflictError,
                sqlite3.DatabaseError,
                ValueError,
            ),
        )
        or any(
            phrase in detail
            for phrase in (
                "chronological",
                "pit violation",
                "journal conflict",
            )
        )
        or (
            isinstance(error, OSError)
            and any(phrase in detail for phrase in ("journal", "index", "sqlite"))
        )
    ):
        return INTEGRITY_EXIT
    return TRANSIENT_EXIT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--adapter-state-db", type=Path)
    parser.add_argument("--adapter-unit", default="qtr-watchdog-telegram.service")
    parser.add_argument("--symbols-per-loop", type=int, default=8)
    parser.add_argument("--api-calls-per-minute", type=int, default=60)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--detection-only", action="store_true")
    args = parser.parse_args(argv)
    if args.symbols_per_loop <= 0 or args.api_calls_per_minute <= 0:
        parser.error("polling and API limits must be positive")
    root = args.data_root.resolve()
    if args.worker:
        return worker(
            root,
            args.symbols_per_loop,
            args.api_calls_per_minute,
            detection_only=args.detection_only,
        )
    if args.adapter_state_db is None:
        parser.error("--adapter-state-db is required for supervisor")
    if not root.is_dir():
        parser.error("data root must exist before supervisor starts")
    stop = Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    # Preflight before starting worker; the adapter must already pin byte zero.
    try:
        check_capacity(capacity(root), starting=True)
        state = adapter_state(
            root / "events/events.jsonl",
            args.adapter_state_db,
            args.adapter_unit,
            time.time(),
            verify_integrity=True,
        )
        check_adapter(state)
        ensure_dataset_mode(root, detection_only=args.detection_only)
        load_checkpoint(root / "state/observer.json", root)
    except ObserverFailure as error:
        print(f"observer preflight blocked: {error}")
        return INTEGRITY_EXIT if error.integrity else TRANSIENT_EXIT
    except Exception as error:  # noqa: BLE001 - do not start on unknown preflight
        print(f"observer preflight blocked: {type(error).__name__}")
        return INTEGRITY_EXIT

    def start_worker() -> Child:
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "market_signal_assistant.watchdog.runtime.observer",
                "--worker",
                "--data-root",
                str(root),
                "--symbols-per-loop",
                str(args.symbols_per_loop),
                "--api-calls-per-minute",
                str(args.api_calls_per_minute),
                *(["--detection-only"] if args.detection_only else []),
            ]
        )

    try:
        result = supervise(
            root, args.adapter_state_db, args.adapter_unit, start_worker, stop=stop
        )
    except DuplicateInstanceError:
        print("observer blocked: another instance holds the lock")
        return INTEGRITY_EXIT
    except ObserverFailure as error:
        print(f"observer blocked: {error}")
        return INTEGRITY_EXIT if error.integrity else TRANSIENT_EXIT
    except Exception as error:  # noqa: BLE001 - no unclassified automatic retry
        print(f"observer blocked: {type(error).__name__}")
        return INTEGRITY_EXIT
    if result:
        print(f"observer stopped: {result}")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
