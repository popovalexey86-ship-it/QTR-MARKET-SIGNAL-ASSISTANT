"""Offline, read-only descriptive analysis of immutable Watchdog evidence.

This module never constructs runtime journals: their constructors may update
SQLite sidecars. A manifest freezes provenance and the pending cutoff.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar, cast

from market_signal_assistant.watchdog.events.journal import _event_from_payload
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.models import watchdog_event_id
from market_signal_assistant.watchdog.outcomes.journal import _from_payload
from market_signal_assistant.watchdog.outcomes.models import (
    OUTCOME_HORIZONS_MINUTES,
    ForwardOutcome,
    OutcomeDataQuality,
    PriceObservation,
    outcome_id,
    target_time,
)
from market_signal_assistant.watchdog.outcomes.price_journal import (
    _from_payload as _price_from_payload,
)
from market_signal_assistant.watchdog.outcomes.scheduler import _source_interval

ANALYZER_VERSION = "evidence-baseline-v1"
SMALL_SAMPLE_N = 30
_SOURCE_FILES = (
    Path("events/events.jsonl"),
    Path("outcomes/outcomes.jsonl"),
    Path("outcomes/price_observations.jsonl"),
)
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class DatasetProvenance:
    dataset_id: str
    path: Path
    known_commit_sha: str
    duration_seconds: int
    compatibility: str
    limitations: tuple[str, ...]
    analysis_as_of: datetime
    analyzer_version: str = ANALYZER_VERSION

    @classmethod
    def from_manifest(cls, path: Path) -> DatasetProvenance:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("Provenance manifest must be an object.")
        value = cast(dict[str, object], raw)
        dataset_id = _required_string(value, "dataset_id")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", dataset_id):
            raise ValueError("Invalid dataset_id.")
        dataset_path = Path(_required_string(value, "path")).resolve(strict=True)
        sha = _required_string(value, "known_commit_sha")
        if not re.fullmatch(r"[0-9a-f]{40}", sha):
            raise ValueError("known_commit_sha must be a full Git SHA.")
        duration = value.get("duration_seconds")
        if isinstance(duration, bool) or not isinstance(duration, int) or duration <= 0:
            raise ValueError("duration_seconds must be positive.")
        compatibility = _required_string(value, "compatibility")
        if compatibility not in {"COMPATIBLE", "PARTIALLY_COMPATIBLE", "INCOMPATIBLE"}:
            raise ValueError("Unknown compatibility class.")
        if compatibility == "INCOMPATIBLE":
            raise ValueError("INCOMPATIBLE datasets are excluded from V1 statistics.")
        limitations = value.get("limitations")
        if not isinstance(limitations, list) or not all(
            isinstance(item, str) and item for item in limitations
        ):
            raise ValueError("limitations must be a list of nonempty strings.")
        as_of = _utc(datetime.fromisoformat(_required_string(value, "analysis_as_of")))
        version = value.get("analyzer_version", ANALYZER_VERSION)
        if version != ANALYZER_VERSION:
            raise ValueError("Unsupported analyzer_version.")
        return cls(
            dataset_id,
            dataset_path,
            sha,
            duration,
            compatibility,
            tuple(cast(list[str], limitations)),
            as_of,
        )


@dataclass(frozen=True, slots=True)
class SourceFingerprint:
    relative_path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class AnalyticalRow:
    dataset_id: str
    event_id: str
    horizon_minutes: int
    status: str
    symbol: str
    target_time: datetime
    outcome: ForwardOutcome | None
    observation: PriceObservation | None

    @property
    def actual_lag_seconds(self) -> float | None:
        if self.outcome is None or self.status == "MISSING":
            return None
        return (self.outcome.observed_at - self.target_time).total_seconds()


@dataclass(frozen=True, slots=True)
class DatasetAnalysis:
    provenance: DatasetProvenance
    events: Mapping[str, WatchdogEventEvidence]
    rows: tuple[AnalyticalRow, ...]
    issues: tuple[str, ...]
    fingerprints: tuple[SourceFingerprint, ...]
    sqlite_checks: tuple[tuple[str, str], ...]

    @property
    def valid(self) -> bool:
        return not self.issues


def _required_string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a nonempty string.")
    return item


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamps must include a timezone.")
    return value.astimezone(UTC)


def _fingerprint(path: Path, relative: Path) -> SourceFingerprint:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return SourceFingerprint(relative.as_posix(), size, digest.hexdigest())


def _fingerprints(root: Path) -> tuple[SourceFingerprint, ...]:
    return tuple(
        _fingerprint(path, path.relative_to(root))
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    )


def _jsonl(path: Path, issues: list[str]) -> Iterator[tuple[int, dict[str, object]]]:
    """Read physical lines; never silently discard a corrupt tail or duplicate."""
    if not path.is_file():
        issues.append(f"missing source: {path.name}")
        return
    with path.open("rb") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.endswith(b"\n"):
                issues.append(f"{path.name}:{line_number}: unterminated JSONL line")
            try:
                raw = json.loads(line)
                if not isinstance(raw, dict):
                    raise ValueError("record is not an object")
                yield line_number, cast(dict[str, object], raw)
            except (ValueError, UnicodeDecodeError) as exc:
                issues.append(f"{path.name}:{line_number}: corrupt record: {exc}")


def _decode(
    path: Path,
    decoder: Callable[[Mapping[str, object]], T],
    id_field: str,
    issues: list[str],
) -> dict[str, T]:
    records: dict[str, T] = {}
    raw_by_id: dict[str, str] = {}
    for line_number, payload in _jsonl(path, issues):
        identity = payload.get(id_field)
        if not isinstance(identity, str) or not identity:
            issues.append(f"{path.name}:{line_number}: missing {id_field}")
            continue
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        payload_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if identity in raw_by_id:
            kind = (
                "conflicting payload"
                if raw_by_id[identity] != payload_hash
                else "duplicate ID"
            )
            issues.append(f"{path.name}:{line_number}: {kind}: {identity}")
            continue
        raw_by_id[identity] = payload_hash
        try:
            # Domain decoders validate schema and PIT; no journal is constructed.
            item = decoder(payload)
        except (ValueError, TypeError, KeyError) as exc:
            issues.append(f"{path.name}:{line_number}: invalid payload: {exc}")
            continue
        records[identity] = item
    return records


def _load_prices(
    path: Path,
    wanted: set[tuple[str, datetime, datetime, float]],
    issues: list[str],
) -> dict[tuple[str, datetime, datetime, float], PriceObservation]:
    matched: dict[tuple[str, datetime, datetime, float], PriceObservation] = {}
    raw_by_id: dict[str, str] = {}
    for line_number, payload in _jsonl(path, issues):
        identity = payload.get("observation_id")
        if not isinstance(identity, str) or not identity:
            issues.append(f"{path.name}:{line_number}: missing observation_id")
            continue
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        payload_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if identity in raw_by_id:
            kind = (
                "conflicting payload"
                if raw_by_id[identity] != payload_hash
                else "duplicate ID"
            )
            issues.append(f"{path.name}:{line_number}: {kind}: {identity}")
            continue
        raw_by_id[identity] = payload_hash
        try:
            point = _price_from_payload(payload)
        except (ValueError, TypeError, KeyError) as exc:
            issues.append(f"{path.name}:{line_number}: invalid payload: {exc}")
            continue
        key = (point.symbol, point.observed_at, point.available_at, point.price)
        if key not in wanted:
            continue
        if key in matched:
            issues.append(
                f"{path.name}:{line_number}: ambiguous raw observation join: {key}"
            )
        else:
            matched[key] = point
    return matched


def _sqlite_integrity(root: Path) -> tuple[tuple[str, str], ...]:
    checks: list[tuple[str, str]] = []
    for path in sorted(root.rglob("*.sqlite3")):
        uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
        try:
            with closing(sqlite3.connect(uri, uri=True)) as connection:
                result = connection.execute("PRAGMA quick_check").fetchone()
            checks.append(
                (str(path.relative_to(root)), str(result[0]) if result else "empty")
            )
        except sqlite3.Error as exc:
            checks.append((str(path.relative_to(root)), f"ERROR: {exc}"))
    return tuple(checks)


def analyze_dataset(
    provenance: DatasetProvenance,
    *,
    verify_sqlite: bool = False,
) -> DatasetAnalysis:
    if provenance.compatibility == "INCOMPATIBLE":
        raise ValueError("INCOMPATIBLE dataset rejected.")
    root = provenance.path
    issues: list[str] = []
    before = _fingerprints(root)
    events = _decode(root / _SOURCE_FILES[0], _event_from_payload, "event_id", issues)
    outcomes = _decode(root / _SOURCE_FILES[1], _from_payload, "outcome_id", issues)
    wanted: set[tuple[str, datetime, datetime, float]] = set()
    for identity, event in events.items():
        if identity != watchdog_event_id(event.symbol, event.detected_at):
            issues.append(f"event ID mismatch: {identity}")
        if event.detected_at > provenance.analysis_as_of:
            issues.append(f"event after frozen analysis cutoff: {identity}")
    for identity, outcome in outcomes.items():
        if outcome.available_at > provenance.analysis_as_of:
            issues.append(f"outcome after frozen analysis cutoff: {identity}")
        linked_event = events.get(outcome.event_id)
        if linked_event is None:
            issues.append(f"orphan outcome: {identity}")
            continue
        if identity != outcome_id(linked_event.event_id, outcome.horizon_minutes):
            issues.append(f"outcome ID mismatch: {identity}")
        if outcome.symbol != linked_event.symbol:
            issues.append(f"outcome symbol mismatch: {identity}")
        if outcome.target_time != target_time(
            linked_event.detected_at, outcome.horizon_minutes
        ):
            issues.append(f"outcome target mismatch: {identity}")
        if not math.isclose(
            outcome.reference_price, linked_event.price_at_detection, rel_tol=1e-12
        ):
            issues.append(f"outcome reference price mismatch: {identity}")
        if outcome.data_quality is OutcomeDataQuality.MISSING:
            continue
        assert outcome.horizon_price is not None
        wanted.add(
            (
                outcome.symbol,
                outcome.observed_at,
                outcome.available_at,
                outcome.horizon_price,
            )
        )
    prices = _load_prices(root / _SOURCE_FILES[2], wanted, issues)
    rows: list[AnalyticalRow] = []
    used: dict[str, set[datetime]] = defaultdict(set)
    for event in sorted(events.values(), key=lambda item: item.event_id):
        for horizon in OUTCOME_HORIZONS_MINUTES:
            target = target_time(event.detected_at, horizon)
            result = outcomes.get(outcome_id(event.event_id, horizon))
            status = (
                result.data_quality.value
                if result is not None
                else "PENDING_DUE_UNRESOLVED"
                if target <= provenance.analysis_as_of
                else "PENDING_NOT_DUE"
            )
            observation = None
            if (
                result is not None
                and result.data_quality is not OutcomeDataQuality.MISSING
            ):
                assert result.horizon_price is not None
                key = (
                    result.symbol,
                    result.observed_at,
                    result.available_at,
                    result.horizon_price,
                )
                observation = prices.get(key)
                if observation is None:
                    issues.append(
                        f"missing or ambiguous raw observation: {result.outcome_id}"
                    )
                if result.observed_at in used[event.event_id]:
                    issues.append(
                        f"observation reused across horizons: {event.event_id}"
                    )
                used[event.event_id].add(result.observed_at)
                if (
                    result.observed_at < target
                    or result.available_at < result.observed_at
                ):
                    issues.append(f"outcome PIT violation: {result.outcome_id}")
                lag = max(0.0, (result.available_at - target).total_seconds())
                if not math.isclose(result.lateness_seconds, lag, abs_tol=1e-6):
                    issues.append(f"outcome lateness mismatch: {result.outcome_id}")
                if observation is not None:
                    tolerance = (
                        _source_interval(observation.source).total_seconds() + 30
                    )
                    expected_quality = (
                        OutcomeDataQuality.ON_TIME
                        if lag <= tolerance
                        else OutcomeDataQuality.LATE
                    )
                    if result.data_quality is not expected_quality:
                        issues.append(
                            f"outcome timing class mismatch: {result.outcome_id}"
                        )
                expected_return = result.horizon_price / event.price_at_detection - 1
                if result.signed_return is None or not math.isclose(
                    result.signed_return, expected_return, rel_tol=1e-10, abs_tol=1e-12
                ):
                    issues.append(f"outcome return mismatch: {result.outcome_id}")
            rows.append(
                AnalyticalRow(
                    provenance.dataset_id,
                    event.event_id,
                    horizon,
                    status,
                    event.symbol,
                    target,
                    result,
                    observation,
                )
            )
    after = _fingerprints(root)
    if before != after:
        issues.append("source byte/hash changed during analysis")
    checks = _sqlite_integrity(root) if verify_sqlite else ()
    if any(result != "ok" for _, result in checks):
        issues.append("SQLite read-only integrity check failed")
    return DatasetAnalysis(
        provenance, events, tuple(rows), tuple(issues), after, checks
    )


def _quantile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * fraction
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def _fmt(value: float | None) -> str:
    return "—" if value is None else f"{value:.6g}"


def _score_bucket(score: float) -> str:
    lower = min(80, int(score // 20) * 20)
    upper = 100 if lower == 80 else lower + 19
    return f"{lower:02d}-{upper:03d}"


def _groups(
    events: Mapping[str, WatchdogEventEvidence],
) -> dict[str, dict[str, set[str]]]:
    groups: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for event in events.values():
        dimensions = {
            "anomaly_type": [item.value for item in event.anomaly_types],
            "combination": [
                "+".join(sorted(item.value for item in event.anomaly_types))
            ],
            "symbol": [event.symbol],
            "tier": [event.universe_tier],
            "utc_hour": [f"{event.detected_at.hour:02d}"],
            "state": [event.state_after.value],
            "score_bucket": [_score_bucket(event.anomaly_score)],
            "baseline_readiness": [
                "BASELINE_SAMPLES_PRESENT"
                if event.baseline_sample_counts
                else "UNKNOWN"
            ],
        }
        for dimension, labels in dimensions.items():
            for label in labels:
                groups[dimension][label].add(event.event_id)
    return groups


def _summary_line(label: str, horizon: int, rows: list[AnalyticalRow]) -> str:
    counts = {
        status: sum(row.status == status for row in rows)
        for status in (
            "ON_TIME",
            "LATE",
            "MISSING",
            "PENDING_NOT_DUE",
            "PENDING_DUE_UNRESOLVED",
        )
    }
    observed = [row.outcome for row in rows if row.status in {"ON_TIME", "LATE"}]
    signed = [
        item.signed_return
        for item in observed
        if item and item.signed_return is not None
    ]
    absolute = [abs(item) for item in signed]
    up = [item.mfe_up for item in observed if item and item.mfe_up is not None]
    down = [item.mfe_down for item in observed if item and item.mfe_down is not None]
    maximum = [
        item.max_abs_excursion
        for item in observed
        if item and item.max_abs_excursion is not None
    ]
    time_up = [
        item.time_to_mfe_up_seconds
        for item in observed
        if item and item.time_to_mfe_up_seconds is not None
    ]
    time_down = [
        item.time_to_mfe_down_seconds
        for item in observed
        if item and item.time_to_mfe_down_seconds is not None
    ]
    unique_symbols = len({row.symbol for row in rows})
    n = len(rows)
    warning = " SMALL_N" if n < SMALL_SAMPLE_N else ""
    coverage = len(observed) / n if n else 0.0
    positive = sum(value > 0 for value in signed) / len(signed) if signed else None
    negative = sum(value < 0 for value in signed) / len(signed) if signed else None
    zero = sum(value == 0 for value in signed) / len(signed) if signed else None
    rates = "/".join(f"{counts[key] / n:.1%}" if n else "—" for key in counts)
    return (
        f"| {label} | +{horizon}m | {n} | {unique_symbols} | {coverage:.1%} | "
        f"{counts['ON_TIME']}/{counts['LATE']}/{counts['MISSING']}/"
        f"{counts['PENDING_NOT_DUE']}/{counts['PENDING_DUE_UNRESOLVED']} | "
        f"{rates} | "
        f"{_fmt(_quantile(signed, 0.5))} | {_fmt(_quantile(signed, 0.05))}/"
        f"{_fmt(_quantile(signed, 0.95))} | {_fmt(_quantile(absolute, 0.5))} | "
        f"{_fmt(_quantile(absolute, 0.05))}/{_fmt(_quantile(absolute, 0.95))} | "
        f"{_fmt(positive)}/{_fmt(negative)}/{_fmt(zero)} | "
        f"{_fmt(_quantile(up, 0.5))}/"
        f"{_fmt(_quantile(down, 0.5))}/"
        f"{_fmt(_quantile(maximum, 0.5))} | "
        f"{_fmt(_quantile(time_up, 0.5))}/"
        f"{_fmt(_quantile(time_down, 0.5))} |{warning}"
    )


def _coverage_line(horizon: int, rows: list[AnalyticalRow]) -> str:
    names = ("ON_TIME", "LATE", "MISSING", "PENDING_NOT_DUE", "PENDING_DUE_UNRESOLVED")
    n = len(rows)
    cells = [
        f"{count} ({count / n:.1%})" if n else "0 (—)"
        for name in names
        for count in [sum(row.status == name for row in rows)]
    ]
    return (
        f"| +{horizon}m | {n} | {len({row.symbol for row in rows})} | "
        + " | ".join(cells)
        + " |"
    )


def render_report(analysis: DatasetAnalysis) -> str:
    """Produce stable Markdown; invalid datasets receive no descriptive statistics."""
    p = analysis.provenance
    lines = [
        "# QTR Watchdog Evidence Baseline V1",
        "",
        f"- Dataset: `{p.dataset_id}`",
        f"- Source: `{p.path}`",
        f"- Known commit (external provenance): `{p.known_commit_sha}`",
        f"- Duration: {p.duration_seconds} seconds",
        f"- Compatibility: {p.compatibility}; datasets are NOT merged",
        f"- Frozen analysis cutoff: {p.analysis_as_of.isoformat()}",
        f"- Analyzer: {p.analyzer_version}",
        f"- Validation: {'PASS' if analysis.valid else 'FAIL'}",
        f"- Total events: {len(analysis.events)}; completed/explicit outcomes: "
        f"{sum(row.outcome is not None for row in analysis.rows)}",
        "- Interpretation: descriptive association only; no causality "
        "or trading inference.",
        "",
        "## Provenance and contamination warnings",
        *[f"- {item}" for item in p.limitations],
        "",
        "## Immutable source fingerprints (before = after)",
        *[
            f"- `{item.relative_path}`: {item.size_bytes} bytes, "
            f"SHA-256 `{item.sha256}`"
            for item in analysis.fingerprints
        ],
    ]
    if analysis.sqlite_checks:
        lines.extend(
            (
                "",
                "## SQLite integrity (read-only)",
                *[f"- `{path}`: {status}" for path, status in analysis.sqlite_checks],
            )
        )
    if analysis.issues:
        lines.extend(
            ("", "## Validation issues", *[f"- {issue}" for issue in analysis.issues])
        )
        lines.append(
            "\nNo descriptive statistics were generated from invalid evidence.\n"
        )
        return "\n".join(lines)
    header = (
        "| Cohort | Horizon | N events | Symbols | Completed coverage | "
        "ON_TIME/LATE/MISSING/pending not due/pending due unresolved | "
        "Status rates in same order | Median signed | Signed p05/p95 | "
        "Median abs | Abs p05/p95 | Positive/negative/zero | "
        "Median MFE-up/MAE-down/max excursion | "
        "Median time-to-up/down (s) | Warning |"
    )
    separator = "|---|---:|---:|---:|---:|---|---|---:|---|---:|---|---|---|---|---|"
    by_event_horizon = {
        (row.event_id, row.horizon_minutes): row for row in analysis.rows
    }
    groups = _groups(analysis.events)
    sections = (
        ("Overall movement by horizon", {"ALL": set(analysis.events)}),
        (
            "By anomaly type (overlapping multi-label memberships)",
            groups["anomaly_type"],
        ),
        ("By exact observed combinations", groups["combination"]),
        ("By symbol", groups["symbol"]),
        ("By tier", groups["tier"]),
        ("By UTC hour", groups["utc_hour"]),
        ("By Watchdog state", groups["state"]),
        ("By existing anomaly score bucket", groups["score_bucket"]),
        ("By baseline sample-count presence", groups["baseline_readiness"]),
    )
    lines.extend(
        (
            "",
            "## Outcome coverage",
            "",
            "| Horizon | N events | Symbols | ON_TIME | LATE | MISSING | "
            "Pending not due | Pending due unresolved |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        )
    )
    for horizon in OUTCOME_HORIZONS_MINUTES:
        lines.append(
            _coverage_line(
                horizon,
                [
                    by_event_horizon[(event_id, horizon)]
                    for event_id in sorted(analysis.events)
                ],
            )
        )
    lines.extend(
        (
            "",
            "## Observation timing and source",
            "",
            "| Horizon | Source | N completed | Median actual lag from "
            "target (s) | Median availability lateness (s) |",
            "|---:|---|---:|---:|---:|",
        )
    )
    source_rows: dict[tuple[int, str], list[AnalyticalRow]] = defaultdict(list)
    for row in analysis.rows:
        if row.observation is not None:
            source_rows[(row.horizon_minutes, row.observation.source)].append(row)
    for (horizon, source), rows in sorted(source_rows.items()):
        lags = [
            row.actual_lag_seconds for row in rows if row.actual_lag_seconds is not None
        ]
        lateness = [
            row.outcome.lateness_seconds for row in rows if row.outcome is not None
        ]
        lines.append(
            f"| +{horizon}m | {source} | {len(rows)} | "
            f"{_fmt(_quantile(lags, 0.5))} | "
            f"{_fmt(_quantile(lateness, 0.5))} |"
        )
    lines.extend(("", "## Overall movement by horizon", "", header, separator))
    for horizon in OUTCOME_HORIZONS_MINUTES:
        lines.append(
            _summary_line(
                "ALL",
                horizon,
                [
                    by_event_horizon[(event_id, horizon)]
                    for event_id in sorted(analysis.events)
                ],
            )
        )
    for title, group in sections[1:]:
        lines.extend(("", f"## {title}", "", header, separator))
        for label, event_ids in sorted(group.items()):
            for horizon in OUTCOME_HORIZONS_MINUTES:
                lines.append(
                    _summary_line(
                        label,
                        horizon,
                        [
                            by_event_horizon[(event_id, horizon)]
                            for event_id in sorted(event_ids)
                        ],
                    )
                )
    lines.extend(
        (
            "",
            "## TOP OBSERVED ASSOCIATIONS FOR REVIEW",
            "",
            f"Only cohorts with N ≥ {SMALL_SAMPLE_N}; sorted by cohort size, "
            "not movement.",
            "| Cohort | Horizon | N events | N completed | Median absolute return |",
            "|---|---:|---:|---:|---:|",
        )
    )
    review: list[tuple[int, str, int, int, float | None]] = []
    for label, ids in groups["anomaly_type"].items():
        if len(ids) < SMALL_SAMPLE_N:
            continue
        for horizon in OUTCOME_HORIZONS_MINUTES:
            vals = [
                row.outcome.abs_return
                for event_id in ids
                if (row := by_event_horizon[(event_id, horizon)]).outcome is not None
                and row.outcome.abs_return is not None
            ]
            if len(vals) >= SMALL_SAMPLE_N:
                review.append(
                    (len(ids), label, horizon, len(vals), _quantile(vals, 0.5))
                )
    for n, label, horizon, completed, value in sorted(
        review, key=lambda item: (-item[0], item[1], item[2])
    ):
        lines.append(f"| {label} | +{horizon}m | {n} | {completed} | {_fmt(value)} |")
    lines.extend(
        (
            "",
            "## Limitations",
            "",
            "- Nominal horizons are not exact-minute observations; actual "
            "source and lag appear above.",
            "- Pending due unresolved is NOT MISSING; no backfill or "
            "interpolation was performed.",
            "- Detector memberships overlap; symbol/time events are not "
            "independent samples.",
            "- Sparse cohorts carry SMALL_N; descriptive statistics do not "
            "establish causality.",
            "- Event baseline sample counts do not prove a separate "
            "baseline-ready flag; only presence is reported.",
            "- This dataset is not merged with any other commit or cohort.",
            "- No control group, parameter sweep, detector tuning, or "
            "trading evaluation.",
            "",
        )
    )
    return "\n".join(lines)


def render_rows_jsonl(analysis: DatasetAnalysis) -> str:
    """Deterministic per-horizon evidence rows, including actual source and lag."""
    if not analysis.valid:
        raise ValueError("Cannot export analytical rows from invalid evidence.")
    records: list[str] = []
    for row in analysis.rows:
        outcome = row.outcome
        point = row.observation
        payload = {
            "dataset_id": row.dataset_id,
            "event_id": row.event_id,
            "symbol": row.symbol,
            "nominal_horizon_minutes": row.horizon_minutes,
            "status": row.status,
            "target_time": row.target_time.isoformat(),
            "observation_id": point.observation_id if point else None,
            "observed_at": point.observed_at.isoformat() if point else None,
            "available_at": point.available_at.isoformat() if point else None,
            "missing_recorded_at": (
                outcome.available_at.isoformat()
                if outcome and outcome.data_quality is OutcomeDataQuality.MISSING
                else None
            ),
            "actual_lag_seconds": row.actual_lag_seconds,
            "raw_source": point.source if point else None,
            "source_interval_seconds": (
                _source_interval(point.source).total_seconds() if point else None
            ),
            "availability_lateness_seconds": (
                outcome.lateness_seconds if outcome else None
            ),
            "signed_return": outcome.signed_return if outcome else None,
            "abs_return": outcome.abs_return if outcome else None,
        }
        records.append(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return "\n".join(records) + ("\n" if records else "")
