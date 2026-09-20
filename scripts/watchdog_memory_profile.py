from __future__ import annotations

import argparse
import json
import tracemalloc
from pathlib import Path

from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.runtime.storage import process_rss_bytes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("data_root", type=Path)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    # A single frame is sufficient for module attribution and avoids making
    # the diagnostic itself dominate RSS on large forensic datasets.
    tracemalloc.start(1)
    bundle = build_bybit_shadow_runtime(args.data_root)
    current, peak = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot()
    payload = {
        "data_root": str(args.data_root.resolve()),
        "traced_current_bytes": current,
        "traced_peak_bytes": peak,
        "process_rss_bytes": process_rss_bytes(),
        "top_allocations": [
            {
                "location": str(stat.traceback[0]),
                "bytes": stat.size,
                "count": stat.count,
            }
            for stat in snapshot.statistics("lineno")[: args.top]
        ],
        "top_modules": [
            {
                "location": str(stat.traceback[0]),
                "bytes": stat.size,
                "count": stat.count,
            }
            for stat in snapshot.statistics("filename")[: args.top]
        ],
    }
    del bundle
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
