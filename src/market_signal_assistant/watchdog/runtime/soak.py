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
from typing import Any

from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.models import ShadowRuntimeConfig
from market_signal_assistant.watchdog.runtime.operator import inspect


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a resumable Watchdog soak.")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--duration-hours", type=float, default=24.0)
    parser.add_argument("--symbols-per-loop", type=int, default=8)
    parser.add_argument("--api-calls-per-minute", type=int, default=60)
    parser.add_argument("--checkpoint-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    if args.duration_hours <= 0 or args.checkpoint_seconds <= 0:
        parser.error("duration and checkpoint interval must be positive")

    checkpoint_path = args.data_root / "state" / "soak.json"
    checkpoint = _load_checkpoint(checkpoint_path)
    accumulated = float(checkpoint.get("accumulated_seconds", 0.0))
    target = args.duration_hours * 3600.0
    if accumulated >= target:
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
    segment_started = time.monotonic()
    started_at = datetime.now(UTC)
    stop_requested = threading.Event()
    exit_reason = ["target_reached"]

    def request_stop(signum: int, _frame: object) -> None:
        exit_reason[0] = f"signal_{signum}"
        stop_requested.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    runtime.start()
    try:
        while accumulated + time.monotonic() - segment_started < target:
            if stop_requested.wait(min(args.checkpoint_seconds, 30.0)):
                break
            _save_checkpoint(
                checkpoint_path,
                accumulated + time.monotonic() - segment_started,
                target,
                started_at,
                "running",
            )
    finally:
        elapsed = accumulated + time.monotonic() - segment_started
        runtime.stop(timeout=45.0)
        _save_checkpoint(
            checkpoint_path,
            elapsed,
            target,
            started_at,
            exit_reason[0],
        )
    print(json.dumps(inspect(args.data_root), sort_keys=True, default=str))
    return 0


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
