from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import tracemalloc
from dataclasses import asdict
from pathlib import Path

from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.models import ShadowRuntimeConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a bounded public-data QTR Watchdog shadow observation."
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=90.0)
    parser.add_argument("--symbols-per-loop", type=int, default=8)
    parser.add_argument("--api-calls-per-minute", type=int, default=60)
    args = parser.parse_args(argv)
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be positive")

    config = ShadowRuntimeConfig(
        maximum_symbols_per_loop=args.symbols_per_loop,
        api_calls_per_minute=args.api_calls_per_minute,
    )
    bundle = build_bybit_shadow_runtime(args.data_root, config=config)
    runtime = bundle.runtime
    tracemalloc.start()
    cpu_started = time.process_time()
    wall_started = time.monotonic()
    runtime.start()
    try:
        time.sleep(args.duration_seconds)
    finally:
        runtime.stop(timeout=45.0)
    wall = max(time.monotonic() - wall_started, 1e-9)
    cpu = time.process_time() - cpu_started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    durations = runtime.loop_durations
    health = runtime.health_snapshot()
    snapshot = runtime.universe_snapshot
    baseline_ready = sum(
        item.tier.value != "COLD_START" for item in snapshot.eligible
    ) if snapshot is not None else 0
    payload = {
        "health": asdict(health),
        "shadow_run_seconds": wall,
        "universe_size": len(snapshot.eligible) if snapshot is not None else 0,
        "baseline_ready_symbols": baseline_ready,
        "api_calls_per_minute": health.api_calls / wall * 60.0,
        "symbols_processed_per_minute": health.symbols_processed / wall * 60.0,
        "average_loop_latency_seconds": (
            statistics.fmean(durations) if durations else 0.0
        ),
        "p95_loop_latency_seconds": _percentile(durations, 0.95),
        "python_peak_memory_bytes": peak,
        "process_cpu_percent": cpu / wall * 100.0,
    }
    print(json.dumps(payload, default=str, sort_keys=True))
    return 0


def _percentile(values: tuple[float, ...], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * quantile) - 1)
    return ordered[index]


if __name__ == "__main__":
    raise SystemExit(main())
