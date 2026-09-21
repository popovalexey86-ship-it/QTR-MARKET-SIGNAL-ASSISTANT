from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import fmean
from typing import Any


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
    return records


def _json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value: Any = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def build_report(root: Path) -> dict[str, object]:
    audits = _jsonl(root / "operational" / "runtime.jsonl")
    gaps = _jsonl(root / "operational" / "gaps.jsonl")
    storage = _jsonl(root / "operational" / "storage.jsonl")
    chronology = [
        item
        for item in audits
        if item.get("event_type") == "SYMBOL_FAILURE"
        and isinstance(item.get("details"), dict)
        and item["details"].get("error_message")
        == "State evaluations must be chronological."
    ]
    chronology_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in chronology:
        details = item["details"]
        key = (str(item.get("symbol")), str(details.get("interval")))
        chronology_by_key[key].append(item)
    correlated = []
    for gap in gaps:
        key = (str(gap.get("symbol")), str(gap.get("interval")))
        recorded_at = datetime.fromisoformat(str(gap["recorded_at"]))
        prior = [
            item
            for item in chronology_by_key[key]
            if datetime.fromisoformat(str(item["occurred_at"])) <= recorded_at
        ]
        correlated.append(
            {
                "gap_id": gap.get("gap_id"),
                "symbol": key[0],
                "interval": key[1],
                "prior_chronology_failures": len(prior),
                "first_prior_failure": (
                    prior[0].get("occurred_at") if prior else None
                ),
                "first_missing_boundary": gap.get("first_missing_boundary"),
                "last_missing_boundary": gap.get("last_missing_boundary"),
            }
        )
    rss = [
        float(item["process_rss_bytes"])
        for item in storage
        if isinstance(item.get("process_rss_bytes"), int)
    ]
    timestamps = [
        datetime.fromisoformat(str(item["recorded_at"]))
        for item in storage
        if isinstance(item.get("process_rss_bytes"), int)
    ]
    rss_slope = None
    if len(rss) > 1:
        hours = (timestamps[-1] - timestamps[0]).total_seconds() / 3600
        if hours > 0:
            rss_slope = (rss[-1] - rss[0]) / hours
    baselines = _json(root / "state" / "baselines.json").get("observations", [])
    cursors = _json(root / "state" / "buckets.json").get("cursors", [])
    states = _json(root / "state" / "symbols.json").get("symbols", [])
    return {
        "root": str(root.resolve()),
        "chronology": {
            "violations": len(chronology),
            "symbols": len({str(item.get("symbol")) for item in chronology}),
            "by_interval": dict(
                Counter(
                    str(item["details"].get("interval")) for item in chronology
                )
            ),
            "first_examples": chronology[:10],
        },
        "gaps": {
            "count": len(gaps),
            "with_prior_same_symbol_interval_chronology_failure": sum(
                1
                for item in correlated
                if isinstance(item["prior_chronology_failures"], int)
                and item["prior_chronology_failures"] > 0
            ),
            "dormant_stale_cursor_gaps": sum(
                1
                for item in correlated
                if item["prior_chronology_failures"] == 0
            ),
            "correlation": correlated,
        },
        "retained_population": {
            "baseline_observations": (
                len(baselines) if isinstance(baselines, list) else 0
            ),
            "symbol_states": len(states) if isinstance(states, list) else 0,
            "interval_cursors": len(cursors) if isinstance(cursors, list) else 0,
            "journal_records": {
                "events": len(_jsonl(root / "events" / "events.jsonl")),
                "outcomes": len(_jsonl(root / "outcomes" / "outcomes.jsonl")),
                "prices": len(
                    _jsonl(root / "outcomes" / "price_observations.jsonl")
                ),
                "runtime_audit": len(audits),
                "storage": len(storage),
                "gaps": len(gaps),
            },
        },
        "rss": {
            "samples": len(rss),
            "start_bytes": rss[0] if rss else None,
            "average_bytes": fmean(rss) if rss else None,
            "p95_bytes": _percentile(rss, 0.95),
            "peak_bytes": max(rss, default=None),
            "final_bytes": rss[-1] if rss else None,
            "linear_growth_bytes_per_hour": rss_slope,
            "projection_is_diagnostic_only": True,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("data_root", type=Path)
    args = parser.parse_args()
    print(json.dumps(build_report(args.data_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
