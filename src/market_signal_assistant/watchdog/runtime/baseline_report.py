from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.statistics import (
    WatchdogDescriptiveStatistics,
)


def build_baseline_report(
    data_root: Path,
    *,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    minimum_samples: int = 20,
) -> dict[str, object]:
    events = WatchdogEventJournal(data_root / "events" / "events.jsonl").records()
    outcomes = WatchdogOutcomeJournal(
        data_root / "outcomes" / "outcomes.jsonl"
    ).records()
    statistics = WatchdogDescriptiveStatistics().analyze(events, outcomes)
    start = started_at or min((item.detected_at for item in events), default=None)
    end = ended_at or max((item.detected_at for item in events), default=None)
    hours = (
        max((end - start).total_seconds() / 3600.0, 0.0)
        if start is not None and end is not None
        else 0.0
    )
    combinations = Counter(
        "+".join(sorted(item.value for item in event.anomaly_types))
        for event in events
    )
    baseline = _json(data_root / "state" / "baselines.json")
    maturation = _maturation(baseline, minimum_samples)
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "observational_only": True,
        "thresholds_optimized": False,
        "window": {
            "started_at": start.isoformat() if start else None,
            "ended_at": end.isoformat() if end else None,
            "hours": hours,
        },
        "event_count": len(events),
        "event_rate_per_hour": len(events) / hours if hours > 0 else None,
        "anomaly_combinations": tuple(sorted(combinations.items())),
        "symbols_involved": tuple(sorted({item.symbol for item in events})),
        "universe_tiers": tuple(
            sorted(Counter(item.universe_tier for item in events).items())
        ),
        "statistics": asdict(statistics),
        "outcome_quality": {
            "completed": len(outcomes),
            "missing_rate": statistics.missing_outcome_rate,
            "late_rate": statistics.late_outcome_rate,
        },
        "baseline_maturation": maturation,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build an observational Watchdog shadow baseline report."
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    payload = build_baseline_report(args.data_root)
    encoded = json.dumps(payload, sort_keys=True, default=str, indent=2)
    if args.output is None:
        print(encoded)
    else:
        args.output.write_text(encoded + "\n", encoding="utf-8")
    return 0


def _maturation(
    payload: dict[str, Any], minimum_samples: int
) -> dict[str, object]:
    per_symbol: dict[str, list[datetime]] = defaultdict(list)
    observations = payload.get("observations", [])
    if isinstance(observations, list):
        for item in observations:
            if (
                not isinstance(item, dict)
                or item.get("feature") != "normalized_range_5"
            ):
                continue
            per_symbol[str(item["symbol"])].append(
                datetime.fromisoformat(str(item["available_at"])).astimezone(UTC)
            )
    times: list[float] = []
    ready: list[str] = []
    for symbol, values in per_symbol.items():
        ordered = sorted(values)
        if len(ordered) >= minimum_samples:
            ready.append(symbol)
            times.append((ordered[minimum_samples - 1] - ordered[0]).total_seconds())
    return {
        "minimum_samples": minimum_samples,
        "ready_symbols": len(ready),
        "symbols": tuple(sorted(ready)),
        "median_time_to_ready_seconds": median(times) if times else None,
    }


def _json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value: Any = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
