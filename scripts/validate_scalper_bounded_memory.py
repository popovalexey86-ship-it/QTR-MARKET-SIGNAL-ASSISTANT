"""Offline-only history audit/benchmark. Never starts a service or a collector.

Run this script against an immutable downloaded copy. --source-root selects
either the fixed checkout or a pristine exported baseline. --grow copies only
the trade journal before appending synthetic benchmark updates to that copy.
No supplied source journal is opened for writing.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import shutil
import sys
import threading
import time
from collections import Counter
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path


@cache
def _windows_api():
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("faults", wintypes.DWORD),
            ("peak", ctypes.c_size_t),
            ("rss", ctypes.c_size_t),
            ("quota_peak_paged", ctypes.c_size_t),
            ("quota_paged", ctypes.c_size_t),
            ("quota_peak_nonpaged", ctypes.c_size_t),
            ("quota_nonpaged", ctypes.c_size_t),
            ("pagefile", ctypes.c_size_t),
            ("peak_pagefile", ctypes.c_size_t),
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    return Counters, kernel, psapi


def memory_mib() -> tuple[float, float]:
    if sys.platform == "win32":
        Counters, kernel, psapi = _windows_api()
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        if not psapi.GetProcessMemoryInfo(
            kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            raise OSError(ctypes.get_last_error(), "GetProcessMemoryInfo")
        return counters.rss / 1048576, counters.peak / 1048576
    import resource

    rss = next(
        float(line.split()[1]) / 1024
        for line in Path("/proc/self/status").read_text().splitlines()
        if line.startswith("VmRSS:")
    )
    return rss, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def guard(limit: float, output: Path, stop: threading.Event) -> None:
    while not stop.wait(0.05):
        rss, peak = memory_mib()
        if rss > limit:
            output.write_text(
                json.dumps(
                    {
                        "status": "ABORTED_MEMORY_LIMIT",
                        "rss_mib": rss,
                        "peak_rss_mib": peak,
                        "limit_mib": limit,
                    }
                ),
                encoding="utf8",
            )
            os._exit(77)  # isolated offline worker only, never a production PID


def audit(decisions: Path, trades: Path) -> dict[str, object]:
    result: dict[str, object] = {}
    for label, path in (("decisions", decisions), ("trades", trades)):
        sha = hashlib.sha256()
        rows = malformed = 0
        events: Counter[str] = Counter()
        latest: dict[str, tuple[str, str]] = {}
        with path.open("rb") as stream:
            for raw in stream:
                sha.update(raw)
                if not raw.strip():
                    continue
                rows += 1
                try:
                    row = json.loads(raw)
                    if label == "decisions":
                        events[row["event_type"]] += 1
                    else:
                        latest[row["trade_id"]] = (row["stage"], row["symbol"])
                except (ValueError, KeyError, TypeError, UnicodeError):
                    malformed += 1
        result[label] = {
            "bytes": path.stat().st_size,
            "sha256": sha.hexdigest(),
            "rows": rows,
            "malformed": malformed,
            "event_counts": dict(events),
            "unique_trades": len(latest),
            "stages": dict(Counter(stage for stage, _ in latest.values())),
            "unfinished": {
                key: value
                for key, value in latest.items()
                if value[0] not in {"CLOSED", "EXPIRED"}
            },
        }
    return result


def benchmark(args: argparse.Namespace) -> dict[str, object]:
    sys.path.insert(0, str(args.source_root.resolve() / "src"))
    from market_signal_assistant.qtr_micro_scalper.analytics import (
        IncrementalDecisionAnalytics,
    )
    from market_signal_assistant.qtr_micro_scalper.cli import JournalObservationSource
    from market_signal_assistant.qtr_micro_scalper.decision_journal import (
        DecisionJournalIndex,
    )
    from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
        ShadowTradeJournal,
    )

    trade_path = args.trades.resolve()
    if args.grow:
        trade_path = args.output.resolve().with_suffix(".benchmark-trades.jsonl")
        assert trade_path != args.trades.resolve()
        shutil.copyfile(args.trades, trade_path)
    cache = IncrementalDecisionAnalytics()
    decisions = DecisionJournalIndex(
        args.decisions, on_record=cache.consume, on_reset=cache.reset
    )
    journal = ShadowTradeJournal(trade_path)
    source = JournalObservationSource(
        decision_journal_path=args.decisions,
        trade_journal_path=trade_path,
        decision_index=decisions,
        decision_analytics=cache,
        trade_journal=journal,
    )
    generated = datetime(2026, 11, 14, 12, tzinfo=UTC)
    first = source.observe(generated_at=generated)
    boot_memory = memory_mib()
    if hasattr(journal, "snapshot"):
        states = journal.snapshot().states
        latest = {s.last_appended.trade_id: s.last_appended for s in states}
    else:
        latest = {r.trade_id: r for r in journal.records()}
    stages = dict(Counter(r.stage.value for r in latest.values()))
    unfinished = [
        r.trade_id
        for r in latest.values()
        if r.stage.value not in {"CLOSED", "EXPIRED"}
    ]
    seed = next(r for r in latest.values() if r.stage.value == "CLOSED")
    snapshot_output = args.output.with_suffix(".snapshot.json")
    snapshot_output.write_text(
        json.dumps(
            {
                "metrics": asdict(first.metrics),
                "analytics": asdict(first.analytics),
                "bootstrap_rss_mib": boot_memory[0],
                "bootstrap_peak_rss_mib": boot_memory[1],
            },
            default=str,
            sort_keys=True,
            ensure_ascii=False,
        ),
        encoding="utf8",
    )
    samples = []
    for _ in range(args.cycles):
        current = source.observe(generated_at=generated)
        assert current.metrics == first.metrics and current.analytics == first.analytics
        samples.append(memory_mib()[0])
    growth = []
    for i in range(args.grow):
        row = replace(
            seed,
            recorded_at=generated + timedelta(seconds=i),
            mfe=seed.mfe + (i + 1) * 0.0001,
        )
        assert journal.append(row)
        current = source.observe(generated_at=generated)
        growth.append(
            {"records": current.metrics.journal_records, "rss_mib": memory_mib()[0]}
        )
    metrics = asdict(first.metrics)
    analytics = asdict(first.analytics)
    canonical = json.dumps(
        {"metrics": metrics, "analytics": analytics},
        default=str,
        sort_keys=True,
        ensure_ascii=False,
    )
    index = getattr(journal, "index_metrics", None)
    return {
        "source_root": str(args.source_root.resolve()),
        "source_module": sys.modules[ShadowTradeJournal.__module__].__file__,
        "metrics": metrics,
        "analytics": analytics,
        "snapshot_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
        "decision_rows": decisions.record_count,
        "unique_trades": len(latest),
        "stages": stages,
        "unfinished": unfinished,
        "trade_index": asdict(index) if index is not None else None,
        "same_shared_journal_after_growth": source._trades is journal,
        "decision_index": asdict(decisions.metrics()),
        "bootstrap_rss_mib": boot_memory[0],
        "bootstrap_peak_rss_mib": boot_memory[1],
        "observation_rss_mib": samples,
        "growth_cycles": growth,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--decisions", type=Path, required=True)
    parser.add_argument("--trades", type=Path, required=True)
    parser.add_argument(
        "--source-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit", action="store_true")
    parser.add_argument("--cycles", type=int, default=10)
    parser.add_argument("--grow", type=int, default=0)
    parser.add_argument("--limit-mib", type=float, default=1536)
    args = parser.parse_args()
    if args.cycles < 0 or args.grow < 0 or args.limit_mib <= 0:
        parser.error("cycles/grow must be non-negative and memory limit positive")
    source_paths = {args.decisions.resolve(), args.trades.resolve()}
    outputs = (
        args.output.resolve(),
        args.output.resolve().with_suffix(".snapshot.json"),
        args.output.resolve().with_suffix(".benchmark-trades.jsonl"),
    )
    if any(path in source_paths for path in outputs):
        parser.error("Validation outputs must never overwrite either source journal")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    monitor = threading.Thread(
        target=guard, args=(args.limit_mib, args.output, stop), daemon=True
    )
    monitor.start()
    start = time.monotonic()
    result = audit(args.decisions, args.trades) if args.audit else benchmark(args)
    rss, peak = memory_mib()
    result.update(
        {
            "status": "COMPLETE",
            "rss_mib": rss,
            "peak_rss_mib": peak,
            "elapsed_seconds": time.monotonic() - start,
        }
    )
    args.output.write_text(
        json.dumps(result, default=str, indent=2, ensure_ascii=False) + "\n",
        encoding="utf8",
    )
    stop.set()
    monitor.join()
    print(
        json.dumps(
            {
                key: result[key]
                for key in ["status", "rss_mib", "peak_rss_mib", "elapsed_seconds"]
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
