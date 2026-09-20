from __future__ import annotations

import argparse
import json
import os
import signal
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.health import JsonRuntimeHealthStore
from market_signal_assistant.watchdog.runtime.liveness import (
    ConfirmedHealthyClock,
    RuntimeLiveness,
    SoakLivenessError,
)
from market_signal_assistant.watchdog.runtime.models import ShadowRuntimeConfig
from market_signal_assistant.watchdog.runtime.operator import inspect


class LivenessRuntime(Protocol):
    @property
    def running(self) -> bool: ...

    @property
    def fatal_error(self) -> Exception | None: ...


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a resumable Watchdog soak.")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--duration-hours", type=float, default=24.0)
    parser.add_argument("--symbols-per-loop", type=int, default=8)
    parser.add_argument("--api-calls-per-minute", type=int, default=60)
    parser.add_argument("--checkpoint-seconds", type=float, default=30.0)
    parser.add_argument("--probe-seconds", type=float, default=1.0)
    parser.add_argument("--liveness-timeout-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    if any(
        value <= 0
        for value in (
            args.duration_hours,
            args.checkpoint_seconds,
            args.probe_seconds,
            args.liveness_timeout_seconds,
        )
    ):
        parser.error("duration, checkpoint, probe, and liveness must be positive")

    checkpoint_path = args.data_root / "state" / "soak.json"
    checkpoint = _load_checkpoint(checkpoint_path)
    confirmed = ConfirmedHealthyClock(
        float(checkpoint.get("accumulated_seconds", 0.0)),
        stale_after_seconds=args.liveness_timeout_seconds,
    )
    target = args.duration_hours * 3600.0
    if confirmed.accumulated_seconds >= target:
        print(json.dumps(inspect(args.data_root), sort_keys=True, default=str))
        return 0

    bundle = build_bybit_shadow_runtime(
        args.data_root,
        config=ShadowRuntimeConfig(
            maximum_symbols_per_loop=args.symbols_per_loop,
            api_calls_per_minute=args.api_calls_per_minute,
        ),
    )
    runtime = bundle.runtime
    started_at = datetime.now(UTC)
    stop_requested = threading.Event()
    exit_reason = ["target_reached"]
    exit_code = 0
    last_checkpoint = time.monotonic()

    def request_stop(signum: int, _frame: object) -> None:
        exit_reason[0] = f"signal_{signum}"
        stop_requested.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    runtime.start()
    try:
        while confirmed.accumulated_seconds < target:
            if stop_requested.wait(args.probe_seconds):
                break
            monotonic_now = time.monotonic()
            try:
                advanced = confirmed.observe(
                    _runtime_liveness(
                        runtime,
                        datetime.now(UTC),
                        args.data_root / "state" / "health.json",
                        expected_started_at=started_at,
                    ),
                    monotonic_now=monotonic_now,
                )
            except SoakLivenessError as error:
                exit_reason[0] = f"failed:{error}"
                exit_code = 1
                break
            if advanced or monotonic_now - last_checkpoint >= args.checkpoint_seconds:
                _save_checkpoint(
                    checkpoint_path,
                    confirmed.accumulated_seconds,
                    target,
                    started_at,
                    "running",
                )
                last_checkpoint = monotonic_now
    finally:
        try:
            runtime.stop(timeout=45.0)
        except Exception as error:
            exit_reason[0] = f"failed:stop:{type(error).__name__}"
            exit_code = 1
        _save_checkpoint(
            checkpoint_path,
            confirmed.accumulated_seconds,
            target,
            started_at,
            exit_reason[0],
        )
    print(json.dumps(inspect(args.data_root), sort_keys=True, default=str))
    return exit_code


def _runtime_liveness(
    runtime: LivenessRuntime,
    observed_at: datetime,
    health_path: Path,
    *,
    expected_started_at: datetime | None = None,
) -> RuntimeLiveness:
    failure = runtime.fatal_error
    try:
        health = JsonRuntimeHealthStore(health_path).load()
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SoakLivenessError(
            f"runtime health is unreadable: {type(error).__name__}"
        ) from error
    health = health or {}
    persisted_started_at = _health_time(health.get("started_at"))
    if expected_started_at is not None and (
        persisted_started_at is None or persisted_started_at < expected_started_at
    ):
        return RuntimeLiveness(
            observed_at=observed_at,
            running=runtime.running,
            fatal_error=type(failure).__name__ if failure is not None else None,
            last_loop_at=None,
            last_market_progress_at=None,
            progress_marker=("startup-pending",),
        )
    return RuntimeLiveness(
        observed_at=observed_at,
        running=runtime.running,
        fatal_error=type(failure).__name__ if failure is not None else None,
        last_loop_at=_health_time(health.get("last_loop_at")),
        last_market_progress_at=_health_time(
            health.get("last_successful_market_update")
        ),
        progress_marker=(
            health.get("last_successful_market_update"),
            health.get("symbols_processed"),
            health.get("api_calls"),
        ),
        degraded=(
            health.get("acceptance_blocked", health.get("degraded")) is not False
        ),
        market_progress_due_at=_health_time(health.get("next_market_update_due_at")),
    )


def _health_time(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise SoakLivenessError("runtime health timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise SoakLivenessError("runtime health timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SoakLivenessError("runtime health timestamp is timezone-naive")
    return parsed.astimezone(UTC)


def _load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) and value.get("version") == 1 else {}


def _save_checkpoint(
    path: Path,
    accumulated_seconds: float,
    target_seconds: float,
    segment_started_at: datetime,
    status: str,
) -> None:
    payload = {
        "version": 1,
        "accumulated_seconds": accumulated_seconds,
        "target_seconds": target_seconds,
        "segment_started_at": segment_started_at.isoformat(),
        "checkpointed_at": datetime.now(UTC).isoformat(),
        "status": status,
        "pid": os.getpid(),
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


if __name__ == "__main__":
    raise SystemExit(main())
