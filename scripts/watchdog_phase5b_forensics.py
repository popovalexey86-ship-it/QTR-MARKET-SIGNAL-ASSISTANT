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
    records: list[dict[str, Any]] = []
    if not path.exists():
        return records
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
    return records


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def _systemd_samples(path: Path | None) -> dict[str, object]:
    if path is None or not path.exists():
        return {"samples": 0, "request_rates_per_minute": {}}
    samples: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", errors="replace") as stream:
        for line in stream:
            marker = line.find("python[")
            payload_start = line.find("{", marker)
            if marker < 0 or payload_start < 0:
                continue
            try:
                payload = json.loads(line[payload_start:])
            except json.JSONDecodeError:
                continue
            health = payload.get("health")
            inspected = payload.get("inspected_at")
            if not isinstance(health, dict) or not isinstance(inspected, str):
                continue
            started = health.get("started_at")
            calls = health.get("api_calls")
            if not isinstance(started, str) or not isinstance(calls, int):
                continue
            seconds = max(
                0.001,
                (
                    datetime.fromisoformat(inspected) - datetime.fromisoformat(started)
                ).total_seconds(),
            )
            samples.append(
                {
                    "calls": calls,
                    "seconds": seconds,
                    "rpm": calls / seconds * 60,
                    "inspected_at": inspected,
                    "rss": health.get("process_rss_bytes"),
                }
            )
    rates = [float(item["rpm"]) for item in samples]
    total_seconds = sum(float(item["seconds"]) for item in samples)
    total_calls = sum(int(item["calls"]) for item in samples)
    rss_samples = [
        (datetime.fromisoformat(str(item["inspected_at"])), int(item["rss"]))
        for item in samples
        if isinstance(item.get("rss"), int)
    ]
    rss_growth_per_hour = None
    if len(rss_samples) > 1:
        wall_hours = (rss_samples[-1][0] - rss_samples[0][0]).total_seconds() / 3600
        if wall_hours > 0:
            rss_growth_per_hour = (rss_samples[-1][1] - rss_samples[0][1]) / wall_hours
    return {
        "samples": len(samples),
        "total_calls": total_calls,
        "covered_seconds": total_seconds,
        "request_rates_per_minute": {
            "weighted_average": total_calls / total_seconds * 60
            if total_seconds
            else None,
            "segment_p95": _percentile(rates, 0.95),
            "segment_peak": max(rates, default=None),
            "note": (
                "segment averages from per-process exit snapshots; exact "
                "per-minute burst p95 was not persisted"
            ),
        },
        "rss": {
            "first_bytes": rss_samples[0][1] if rss_samples else None,
            "last_bytes": rss_samples[-1][1] if rss_samples else None,
            "maximum_bytes": max((item[1] for item in rss_samples), default=None),
            "linear_growth_bytes_per_hour": rss_growth_per_hour,
            "projected_24h_bytes": (
                rss_samples[0][1] + rss_growth_per_hour * 24
                if rss_samples and rss_growth_per_hour is not None
                else None
            ),
            "projected_7d_bytes": (
                rss_samples[0][1] + rss_growth_per_hour * 24 * 7
                if rss_samples and rss_growth_per_hour is not None
                else None
            ),
            "projected_30d_bytes": (
                rss_samples[0][1] + rss_growth_per_hour * 24 * 30
                if rss_samples and rss_growth_per_hour is not None
                else None
            ),
            "note": "linear diagnostic extrapolation, not a capacity guarantee",
        },
    }


def build_report(root: Path, systemd_log: Path | None) -> dict[str, object]:
    runtime = _jsonl(root / "operational" / "runtime.jsonl")
    events = _jsonl(root / "events" / "events.jsonl")
    outcomes = _jsonl(root / "outcomes" / "outcomes.jsonl")
    prices = _jsonl(root / "outcomes" / "price_observations.jsonl")

    internal_waits: list[float] = []
    provider_rate_limits: list[dict[str, Any]] = []
    provider_failures: list[dict[str, Any]] = []
    symbol_degraded: Counter[str] = Counter()
    for item in runtime:
        details = item.get("details")
        details = details if isinstance(details, dict) else {}
        event_type = item.get("event_type")
        if event_type == "RATE_LIMIT" and "wait_seconds" in details:
            internal_waits.append(float(details["wait_seconds"]))
        elif event_type == "RATE_LIMIT":
            provider_rate_limits.append(item)
        elif event_type == "PROVIDER_FAILURE":
            provider_failures.append(item)
        if event_type == "DEGRADED_MODE":
            reason = str(details.get("reason", ""))
            if reason.startswith("symbol:"):
                symbol_degraded[reason] += 1

    horizon_quality: dict[str, Counter[str]] = defaultdict(Counter)
    lateness: dict[str, list[float]] = defaultdict(list)
    reused: dict[tuple[str, str], list[int]] = defaultdict(list)
    for item in outcomes:
        horizon = str(item.get("horizon_minutes"))
        quality = str(item.get("data_quality"))
        horizon_quality[horizon][quality] += 1
        if isinstance(item.get("lateness_seconds"), (int, float)):
            lateness[horizon].append(float(item["lateness_seconds"]))
        if quality != "MISSING":
            reused[
                (
                    str(item.get("event_id")),
                    str(item.get("observation_id", item.get("observed_at"))),
                )
            ].append(int(item["horizon_minutes"]))

    event_types: Counter[str] = Counter()
    transitions: Counter[str] = Counter()
    symbols: Counter[str] = Counter()
    scores: list[float] = []
    for item in events:
        event_types.update(str(value) for value in item.get("anomaly_types", []))
        transitions[f"{item.get('state_before')}->{item.get('state_after')}"] += 1
        symbols[str(item.get("symbol"))] += 1
        score = item.get("anomaly_score")
        if isinstance(score, (int, float)) and not isinstance(score, bool):
            scores.append(float(score))

    return {
        "root": str(root),
        "events": {
            "count": len(events),
            "anomaly_types": dict(event_types.most_common()),
            "state_transitions": dict(transitions.most_common()),
            "top_symbols": dict(symbols.most_common(20)),
            "unique_symbols": len(symbols),
            "anomaly_score": {
                "mean": fmean(scores) if scores else None,
                "p50": _percentile(scores, 0.50),
                "p95": _percentile(scores, 0.95),
                "maximum": max(scores, default=None),
            },
        },
        "outcomes": {
            "count": len(outcomes),
            "quality_by_horizon": {
                key: dict(value)
                for key, value in sorted(
                    horizon_quality.items(), key=lambda item: int(item[0])
                )
            },
            "lateness_seconds_by_horizon": {
                key: {
                    "mean": fmean(values),
                    "p50": _percentile(values, 0.50),
                    "p95": _percentile(values, 0.95),
                    "max": max(values),
                }
                for key, values in sorted(
                    lateness.items(), key=lambda item: int(item[0])
                )
            },
            "same_observation_multi_horizon_groups": sum(
                len(value) > 1 for value in reused.values()
            ),
            "same_observation_examples": [
                {"event_id": event_id, "observed_at": observed_at, "horizons": horizons}
                for (event_id, observed_at), horizons in reused.items()
                if len(horizons) > 1
            ][:20],
        },
        "prices": {
            "count": len(prices),
            "sources": dict(
                Counter(str(item.get("source")) for item in prices).most_common()
            ),
        },
        "runtime": {
            "audit_count": len(runtime),
            "audit_types": dict(
                Counter(str(item.get("event_type")) for item in runtime).most_common()
            ),
            "internal_throttle_waits": {
                "count": len(internal_waits),
                "mean_seconds": fmean(internal_waits) if internal_waits else None,
                "p95_seconds": _percentile(internal_waits, 0.95),
                "max_seconds": max(internal_waits, default=None),
            },
            "actual_provider_rate_limits": len(provider_rate_limits),
            "provider_failures": len(provider_failures),
            "symbol_degraded": dict(symbol_degraded.most_common()),
        },
        "systemd": _systemd_samples(systemd_log),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--systemd-log", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(build_report(args.root, args.systemd_log), indent=2, sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
