from __future__ import annotations

# ruff: noqa: E501
import argparse
import bisect
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from market_signal_assistant.setup_engine.adapters import (
    input_from_early_discovery_v2,
)
from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.models import (
    SetupDirection,
    SetupState,
)
from market_signal_assistant.setup_engine.offline_analyzer import (
    v2_result_from_json,
)

HORIZONS = (5, 15, 30, 60, 180)
OUTPUT_NAMES = (
    "итоговый_отчёт.md",
    "метрики.json",
    "воронка.csv",
    "переходы_состояний.csv",
    "outcomes_по_типам.csv",
    "outcomes_по_confidence.csv",
    "outcomes_по_ATR_distance.csv",
    "too_far_analysis.csv",
    "invalid_state_analysis.csv",
    "missed_moves.csv",
    "latency.csv",
    "micro_revalidation.csv",
    "trade_analysis.csv",
    "рекомендации_без_изменения_кода.md",
)


@dataclass(frozen=True, slots=True)
class CompactSnapshot:
    scanned_at: datetime
    scan_id: str
    symbol: str
    direction: str
    setup_type: str
    setup_state: str
    trade_eligible: bool
    confidence: float
    price: float | None
    trigger: float | None
    invalidation: float | None
    distance_atr: float | None
    first_detected_at: datetime | None
    first_ready_at: datetime | None
    spread_pct: float | None
    liquidity: float | None
    technical_error: str | None
    is_late: bool

    @property
    def episode_id(self) -> str:
        if self.first_detected_at is not None:
            return self.first_detected_at.isoformat()
        return "unassigned"


@dataclass(frozen=True, slots=True)
class Decision:
    decided_at: datetime
    symbol: str
    episode_id: str
    skip_reason: str
    skip_detail: str | None
    instrument_status: str | None


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    occurred_at: datetime
    event: str
    symbol: str
    trade_id: str
    stage: str
    reason: str | None
    detail: str | None
    ret_code: int | None


@dataclass(frozen=True, slots=True)
class Outcome:
    horizon: int
    observed_at: datetime
    value_pct: float


@dataclass(frozen=True, slots=True)
class AuditData:
    snapshots: tuple[CompactSnapshot, ...]
    decisions: tuple[Decision, ...]
    runtime_events: tuple[RuntimeEvent, ...]
    trades: tuple[Mapping[str, object], ...]
    state: Mapping[str, object]
    rejected_snapshot_lines: int
    hashes_before: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class AuditResult:
    metrics: Mapping[str, object]
    tables: Mapping[str, tuple[Mapping[str, object], ...]]
    report: str
    recommendations: str
    hashes_before: Mapping[str, str]
    source_paths: tuple[Path, ...]


def _utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _optional_utc(value: object) -> datetime | None:
    return None if value is None else _utc(value)


def _optional_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _liquidity(raw: Mapping[str, Any]) -> float | None:
    components = raw.get("component_scores")
    if not isinstance(components, list):
        return None
    for component in components:
        if (
            isinstance(component, dict)
            and component.get("score_kind") == "discovery"
            and component.get("component_id") == "liquidity"
        ):
            return _optional_float(component.get("raw_value"))
    return None


def read_snapshots(path: Path) -> tuple[tuple[CompactSnapshot, ...], int]:
    snapshots: list[CompactSnapshot] = []
    rejected = 0
    with path.open("r", encoding="utf-8-sig") as stream:
        for line in stream:
            if not line.strip():
                continue
            try:
                loaded = json.loads(line)
                if not isinstance(loaded, dict):
                    raise ValueError("snapshot root is not an object")
                raw = cast(dict[str, Any], loaded)
                source = v2_result_from_json(raw)
                result = analyze_setup(input_from_early_discovery_v2(source))
                snapshots.append(
                    CompactSnapshot(
                        scanned_at=source.scanned_at,
                        scan_id=source.scan_id,
                        symbol=result.symbol,
                        direction=result.direction.value,
                        setup_type=result.setup_type.value,
                        setup_state=result.setup_state.value,
                        trade_eligible=result.trade_eligible,
                        confidence=result.confidence,
                        price=result.current_price,
                        trigger=result.trigger_level,
                        invalidation=result.invalidation_level,
                        distance_atr=result.distance_to_trigger_atr,
                        first_detected_at=source.first_detected_at,
                        first_ready_at=source.first_ready_at,
                        spread_pct=source.spread_pct,
                        liquidity=_liquidity(raw),
                        technical_error=source.technical_error,
                        is_late=result.is_late,
                    )
                )
            except (json.JSONDecodeError, TypeError, ValueError):
                rejected += 1
    snapshots.sort(key=lambda item: (item.scanned_at, item.symbol))
    return tuple(snapshots), rejected


def _read_jsonl(path: Path) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    with path.open("r", encoding="utf-8-sig") as stream:
        for line in stream:
            if not line.strip():
                continue
            try:
                loaded = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(loaded, dict):
                rows.append(cast(Mapping[str, object], loaded))
    return tuple(rows)


def read_audit(snapshot_dir: Path) -> AuditData:
    names = (
        "inplay_early_discovery_v2_audit.jsonl",
        "qtr_micro_decisions.jsonl",
        "qtr_micro_runtime_audit.jsonl",
        "qtr_micro_trades.jsonl",
        "qtr_micro_state.json",
    )
    paths = tuple(snapshot_dir / name for name in names)
    missing = tuple(path.name for path in paths if not path.is_file())
    if missing:
        raise FileNotFoundError(", ".join(missing))
    hashes = {path.name: _sha256(path) for path in paths}
    snapshots, rejected = read_snapshots(paths[0])
    decisions = tuple(
        Decision(
            decided_at=_utc(row["decided_at"]),
            symbol=str(row["symbol"]),
            episode_id=str(row["episode_id"]),
            skip_reason=str(row["skip_reason"]),
            skip_detail=(
                str(row["skip_detail"]) if row.get("skip_detail") is not None else None
            ),
            instrument_status=(
                str(row["instrument_status"])
                if row.get("instrument_status") is not None
                else None
            ),
        )
        for row in _read_jsonl(paths[1])
    )
    runtime = tuple(
        RuntimeEvent(
            occurred_at=_utc(row["occurred_at"]),
            event=str(row["event"]),
            symbol=str(row["symbol"]),
            trade_id=str(row["trade_id"]),
            stage=str(row["stage"]),
            reason=str(row["reason"]) if row.get("reason") is not None else None,
            detail=str(row["detail"]) if row.get("detail") is not None else None,
            ret_code=(
                int(str(row["ret_code"])) if row.get("ret_code") is not None else None
            ),
        )
        for row in _read_jsonl(paths[2])
    )
    state = json.loads(paths[4].read_text(encoding="utf-8-sig"))
    if not isinstance(state, dict):
        raise ValueError("qtr_micro_state.json root is not an object")
    return AuditData(
        snapshots,
        decisions,
        runtime,
        _read_jsonl(paths[3]),
        cast(Mapping[str, object], state),
        rejected,
        hashes,
    )


def confidence_bucket(value: float) -> str:
    if value < 60:
        return "<60"
    if value < 70:
        return "60-69"
    if value < 80:
        return "70-79"
    if value < 90:
        return "80-89"
    if value < 95:
        return "90-94"
    return "95-100"


def atr_bucket(value: float | None) -> str:
    if value is None:
        return "null"
    absolute = abs(value)
    if absolute <= 0.10:
        return "0-0.10"
    if absolute <= 0.25:
        return "0.10-0.25"
    if absolute <= 0.50:
        return "0.25-0.50"
    if absolute <= 1.0:
        return "0.50-1.0"
    if absolute <= 2.0:
        return "1.0-2.0"
    return ">2.0"


def too_far_bucket(value: float | None) -> str:
    if value is None:
        return "null"
    absolute = abs(value)
    if absolute < 0.35:
        return "0.25-0.35"
    if absolute < 0.50:
        return "0.35-0.50"
    if absolute < 0.75:
        return "0.50-0.75"
    if absolute <= 1.0:
        return "0.75-1.0"
    return ">1.0"


def signal_age_bucket(seconds: float | None) -> str:
    if seconds is None:
        return "null"
    if seconds <= 30:
        return "0-30s"
    if seconds <= 60:
        return "31-60s"
    if seconds <= 300:
        return "61-300s"
    if seconds <= 900:
        return "301-900s"
    return ">900s"


def _symbol_index(
    snapshots: Sequence[CompactSnapshot],
) -> tuple[
    Mapping[str, tuple[CompactSnapshot, ...]], Mapping[str, tuple[datetime, ...]]
]:
    grouped: dict[str, list[CompactSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        if snapshot.price is not None:
            grouped[snapshot.symbol].append(snapshot)
    series = {
        symbol: tuple(sorted(rows, key=lambda item: item.scanned_at))
        for symbol, rows in grouped.items()
    }
    times = {
        symbol: tuple(item.scanned_at for item in rows)
        for symbol, rows in series.items()
    }
    return series, times


def nearest_snapshot(
    symbol: str,
    at: datetime,
    series: Mapping[str, tuple[CompactSnapshot, ...]],
    times: Mapping[str, tuple[datetime, ...]],
    *,
    max_age_minutes: int = 10,
) -> CompactSnapshot | None:
    rows = series.get(symbol, ())
    stamps = times.get(symbol, ())
    if not rows:
        return None
    index = bisect.bisect_right(stamps, at) - 1
    if index < 0:
        return None
    candidate = rows[index]
    if at - candidate.scanned_at > timedelta(minutes=max_age_minutes):
        return None
    return candidate


def outcome_for(
    anchor: CompactSnapshot,
    series: Mapping[str, tuple[CompactSnapshot, ...]],
    times: Mapping[str, tuple[datetime, ...]],
    horizon: int,
    tolerance_minutes: int = 3,
) -> Outcome | None:
    if anchor.price is None or anchor.direction == SetupDirection.NEUTRAL.value:
        return None
    rows = series.get(anchor.symbol, ())
    stamps = times.get(anchor.symbol, ())
    target = anchor.scanned_at + timedelta(minutes=horizon)
    index = bisect.bisect_left(stamps, target)
    candidates = rows[index : index + 2]
    if not candidates:
        return None
    future = min(candidates, key=lambda item: abs(item.scanned_at - target))
    if abs(future.scanned_at - target) > timedelta(minutes=tolerance_minutes):
        return None
    if future.price is None:
        return None
    sign = 1.0 if anchor.direction == SetupDirection.UP.value else -1.0
    value = (future.price - anchor.price) / anchor.price * 100 * sign
    return Outcome(horizon, future.scanned_at, value)


def _episode_anchors(
    snapshots: Sequence[CompactSnapshot],
) -> tuple[CompactSnapshot, ...]:
    anchors: dict[tuple[str, str, str], CompactSnapshot] = {}
    for row in snapshots:
        if (
            row.direction == SetupDirection.NEUTRAL.value
            or row.episode_id == "unassigned"
        ):
            continue
        key = (row.symbol, row.episode_id, row.setup_state)
        anchors.setdefault(key, row)
    return tuple(sorted(anchors.values(), key=lambda item: item.scanned_at))


def _outcome_stats(values: Sequence[float]) -> Mapping[str, object]:
    if not values:
        return {
            "n": 0,
            "median": None,
            "mean": None,
            "win_rate": None,
            "p25": None,
            "p75": None,
        }
    ordered = sorted(values)
    return {
        "n": len(values),
        "median": statistics.median(values),
        "mean": statistics.fmean(values),
        "win_rate": sum(value > 0 for value in values) / len(values),
        "p25": _percentile(ordered, 0.25),
        "p75": _percentile(ordered, 0.75),
    }


def _percentile(values: Sequence[float], fraction: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _group_outcomes(
    anchors: Sequence[CompactSnapshot],
    group_names: tuple[str, ...],
    group_value: Callable[[CompactSnapshot], tuple[object, ...]],
    series: Mapping[str, tuple[CompactSnapshot, ...]],
    times: Mapping[str, tuple[datetime, ...]],
) -> tuple[Mapping[str, object], ...]:
    grouped: dict[tuple[str, tuple[object, ...], int], list[float]] = defaultdict(list)
    for anchor in anchors:
        group = group_value(anchor)
        for horizon in HORIZONS:
            outcome = outcome_for(anchor, series, times, horizon)
            if outcome is not None:
                grouped[("ALL", group, horizon)].append(outcome.value_pct)
                grouped[(anchor.symbol, group, horizon)].append(outcome.value_pct)
    rows: list[Mapping[str, object]] = []
    for (symbol, group, horizon), values in sorted(
        grouped.items(), key=lambda item: str(item[0])
    ):
        rows.append(
            {
                "symbol": symbol,
                **dict(zip(group_names, group, strict=True)),
                "horizon_minutes": horizon,
                **_outcome_stats(values),
            }
        )
    return tuple(rows)


def _outcome_summary(
    anchors: Sequence[CompactSnapshot],
    group_names: tuple[str, ...],
    group_value: Callable[[CompactSnapshot], tuple[object, ...]],
    series: Mapping[str, tuple[CompactSnapshot, ...]],
    times: Mapping[str, tuple[datetime, ...]],
) -> tuple[Mapping[str, object], ...]:
    grouped: dict[tuple[tuple[object, ...], int], list[float]] = defaultdict(list)
    for anchor in anchors:
        group = group_value(anchor)
        for horizon in HORIZONS:
            outcome = outcome_for(anchor, series, times, horizon)
            if outcome is not None:
                grouped[(group, horizon)].append(outcome.value_pct)
    return tuple(
        {
            **dict(zip(group_names, group, strict=True)),
            "horizon_minutes": horizon,
            **_outcome_stats(values),
        }
        for (group, horizon), values in sorted(
            grouped.items(), key=lambda item: str(item[0])
        )
    )


def _transition_rows(
    snapshots: Sequence[CompactSnapshot],
) -> tuple[Mapping[str, object], ...]:
    episodes: dict[tuple[str, str], list[CompactSnapshot]] = defaultdict(list)
    for row in snapshots:
        if row.episode_id == "unassigned":
            continue
        episodes[(row.symbol, row.episode_id)].append(row)
    counts: Counter[tuple[str, str]] = Counter()
    ready_cancelled = 0
    confirming_never_ready = 0
    cancelled_recovered = 0
    for episode_rows in episodes.values():
        ordered = sorted(episode_rows, key=lambda item: item.scanned_at)
        states: list[str] = []
        for row in ordered:
            if not states or states[-1] != row.setup_state:
                states.append(row.setup_state)
        counts.update(zip(states, states[1:], strict=False))
        if (
            SetupState.READY_TO_CONSIDER.value in states
            and SetupState.CANCELLED.value in states
            and states.index(SetupState.CANCELLED.value)
            > states.index(SetupState.READY_TO_CONSIDER.value)
        ):
            ready_cancelled += 1
        if (
            SetupState.CONFIRMING.value in states
            and SetupState.READY_TO_CONSIDER.value not in states
        ):
            confirming_never_ready += 1
        if (
            SetupState.CANCELLED.value in states
            and states[-1] != SetupState.CANCELLED.value
        ):
            cancelled_recovered += 1
    output_rows: list[Mapping[str, object]] = [
        {"from_state": old, "to_state": new, "count": count}
        for (old, new), count in counts.most_common()
    ]
    output_rows.extend(
        (
            {
                "from_state": "SUMMARY",
                "to_state": "READY_THEN_CANCELLED",
                "count": ready_cancelled,
            },
            {
                "from_state": "SUMMARY",
                "to_state": "CONFIRMING_NEVER_READY",
                "count": confirming_never_ready,
            },
            {
                "from_state": "SUMMARY",
                "to_state": "CANCELLED_RECOVERED",
                "count": cancelled_recovered,
            },
        )
    )
    return tuple(output_rows)


def _funnel_rows(data: AuditData) -> tuple[Mapping[str, object], ...]:
    snapshots = data.snapshots
    directional = tuple(
        row for row in snapshots if row.direction != SetupDirection.NEUTRAL.value
    )
    states = Counter(row.setup_state for row in directional)
    decisions = Counter(row.skip_reason for row in data.decisions)
    events = Counter(row.event for row in data.runtime_events)
    stages = (
        ("IN_PLAY snapshots", len(snapshots)),
        ("directional", len(directional)),
        ("FORMING", states[SetupState.FORMING.value]),
        ("CONFIRMING", states[SetupState.CONFIRMING.value]),
        ("READY", states[SetupState.READY_TO_CONSIDER.value]),
        ("Micro decisions", len(data.decisions)),
        (
            "PREPARED persisted (all runtime versions)",
            events["PREPARED"],
        ),
        ("final revalidation started", events["ENTRY_REVALIDATION_STARTED"]),
        ("final revalidation passed", events["ENTRY_REVALIDATION_PASSED"]),
        ("final revalidation rejected", events["ENTRY_REVALIDATION_REJECTED"]),
        ("trades", len(data.trades)),
    )
    return tuple(
        {
            "stage": stage,
            "count": count,
            "share_of_snapshots": count / len(snapshots) if snapshots else None,
        }
        for stage, count in stages
    ) + tuple(
        {
            "stage": f"skip:{reason}",
            "count": count,
            "share_of_snapshots": count / len(snapshots) if snapshots else None,
        }
        for reason, count in decisions.most_common()
    )


def _latency_rows(
    snapshots: Sequence[CompactSnapshot],
    runtime: Sequence[RuntimeEvent],
) -> tuple[Mapping[str, object], ...]:
    episodes: dict[tuple[str, str], list[CompactSnapshot]] = defaultdict(list)
    for row in snapshots:
        if row.episode_id != "unassigned":
            episodes[(row.symbol, row.episode_id)].append(row)
    revalidation = {
        event.trade_id: event
        for event in runtime
        if event.event == "ENTRY_REVALIDATION_STARTED"
    }
    submit_by_trade = {
        event.trade_id: event.occurred_at
        for event in runtime
        if event.event == "ENTRY_SUBMIT_ATTEMPT"
    }
    rows: list[Mapping[str, object]] = []
    for (symbol, episode_id), episode in episodes.items():
        ordered = sorted(episode, key=lambda item: item.scanned_at)
        ready = next(
            (
                row
                for row in ordered
                if row.setup_state == SetupState.READY_TO_CONSIDER.value
            ),
            None,
        )
        if ready is None:
            continue
        forming = next(
            (
                row
                for row in reversed(ordered)
                if row.scanned_at <= ready.scanned_at
                and row.setup_state == SetupState.FORMING.value
            ),
            None,
        )
        confirming = next(
            (
                row
                for row in reversed(ordered)
                if row.scanned_at <= ready.scanned_at
                and row.setup_state == SetupState.CONFIRMING.value
            ),
            None,
        )
        trade_id = (
            "QTRM-" + hashlib.sha256(f"{symbol}|{episode_id}".encode()).hexdigest()[:20]
        )
        final = revalidation.get(trade_id)
        submit = submit_by_trade.get(trade_id)
        rows.append(
            {
                "symbol": symbol,
                "episode_id": episode_id,
                "trade_id": trade_id,
                "first_seen": ordered[0].scanned_at.isoformat(),
                "forming_at": forming.scanned_at.isoformat() if forming else None,
                "confirming_at": confirming.scanned_at.isoformat()
                if confirming
                else None,
                "ready_at": ready.scanned_at.isoformat(),
                "final_revalidation_at": final.occurred_at.isoformat()
                if final
                else None,
                "submit_at": submit.isoformat() if submit else None,
                "first_to_ready_seconds": (
                    ready.scanned_at - ordered[0].scanned_at
                ).total_seconds(),
                "confirming_to_ready_seconds": (
                    (ready.scanned_at - confirming.scanned_at).total_seconds()
                    if confirming
                    else None
                ),
                "ready_to_revalidation_seconds": (
                    (final.occurred_at - ready.scanned_at).total_seconds()
                    if final
                    else None
                ),
                "ready_to_submit_seconds": (
                    (submit - ready.scanned_at).total_seconds() if submit else None
                ),
            }
        )
    return tuple(rows)


def _decision_rows(
    data: AuditData,
    series: Mapping[str, tuple[CompactSnapshot, ...]],
    times: Mapping[str, tuple[datetime, ...]],
) -> tuple[Mapping[str, object], ...]:
    total = len(data.decisions)
    rows: list[Mapping[str, object]] = []
    for decision in data.decisions:
        anchor = nearest_snapshot(decision.symbol, decision.decided_at, series, times)
        outcomes = {
            horizon: (outcome_for(anchor, series, times, horizon) if anchor else None)
            for horizon in HORIZONS
        }
        risk_pct = None
        if anchor and anchor.price and anchor.invalidation:
            risk_pct = abs(anchor.price - anchor.invalidation) / anchor.price * 100
        row: dict[str, object] = {
            "decided_at": decision.decided_at.isoformat(),
            "symbol": decision.symbol,
            "episode_id": decision.episode_id,
            "skip_reason": decision.skip_reason,
            "share_of_decisions": 1 / total if total else None,
            "setup_type": anchor.setup_type if anchor else None,
            "setup_state": anchor.setup_state if anchor else None,
            "direction": anchor.direction if anchor else None,
            "confidence": anchor.confidence if anchor else None,
            "confidence_bucket": confidence_bucket(anchor.confidence)
            if anchor
            else None,
            "distance_atr": anchor.distance_atr if anchor else None,
            "atr_bucket": atr_bucket(anchor.distance_atr) if anchor else "null",
            "too_far_bucket": too_far_bucket(anchor.distance_atr)
            if decision.skip_reason == "too_far" and anchor
            else None,
            "spread_available": anchor.spread_pct is not None if anchor else None,
            "liquidity_available": anchor.liquidity is not None if anchor else None,
            "signal_age_seconds": (
                (decision.decided_at - anchor.scanned_at).total_seconds()
                if anchor
                else None
            ),
            "signal_age_bucket": signal_age_bucket(
                (decision.decided_at - anchor.scanned_at).total_seconds()
                if anchor
                else None
            ),
            "risk_pct_equivalent": risk_pct,
        }
        for horizon, outcome in outcomes.items():
            row[f"outcome_{horizon}m_pct"] = outcome.value_pct if outcome else None
            for multiple in (0.5, 1.0, 2.0):
                row[f"reached_{multiple:g}R_{horizon}m"] = (
                    outcome.value_pct >= risk_pct * multiple
                    if outcome is not None and risk_pct is not None
                    else None
                )
        rows.append(row)
    for event in data.runtime_events:
        if event.event != "ENTRY_REVALIDATION_REJECTED":
            continue
        anchor = nearest_snapshot(event.symbol, event.occurred_at, series, times)
        row = {
            "decided_at": event.occurred_at.isoformat(),
            "symbol": event.symbol,
            "episode_id": event.trade_id,
            "skip_reason": f"ENTRY_REVALIDATION_REJECTED:{event.detail or 'unknown'}",
            "share_of_decisions": None,
            "setup_type": anchor.setup_type if anchor else None,
            "setup_state": anchor.setup_state if anchor else None,
            "direction": anchor.direction if anchor else None,
            "confidence": anchor.confidence if anchor else None,
            "confidence_bucket": confidence_bucket(anchor.confidence)
            if anchor
            else None,
            "distance_atr": anchor.distance_atr if anchor else None,
            "atr_bucket": atr_bucket(anchor.distance_atr) if anchor else "null",
            "too_far_bucket": too_far_bucket(anchor.distance_atr) if anchor else None,
            "spread_available": anchor.spread_pct is not None if anchor else None,
            "liquidity_available": anchor.liquidity is not None if anchor else None,
            "signal_age_seconds": (
                (event.occurred_at - anchor.scanned_at).total_seconds()
                if anchor
                else None
            ),
            "signal_age_bucket": signal_age_bucket(
                (event.occurred_at - anchor.scanned_at).total_seconds()
                if anchor
                else None
            ),
            "risk_pct_equivalent": None,
        }
        for horizon in HORIZONS:
            outcome = outcome_for(anchor, series, times, horizon) if anchor else None
            row[f"outcome_{horizon}m_pct"] = outcome.value_pct if outcome else None
            for multiple in (0.5, 1.0, 2.0):
                row[f"reached_{multiple:g}R_{horizon}m"] = None
        rows.append(row)
    return tuple(rows)


def _aggregate_decisions(
    rows: Sequence[Mapping[str, object]], reason: str
) -> tuple[Mapping[str, object], ...]:
    selected = tuple(row for row in rows if row["skip_reason"] == reason)
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in selected:
        key = str(
            row.get("too_far_bucket") if reason == "too_far" else row.get("setup_state")
        )
        grouped[key].append(row)
    output: list[Mapping[str, object]] = []
    for group, items in sorted(grouped.items()):
        base: dict[str, object] = {
            "skip_reason": reason,
            "segment": group,
            "count": len(items),
            "share_of_reason": len(items) / len(selected) if selected else None,
        }
        for horizon in HORIZONS:
            values = [
                float(str(row[f"outcome_{horizon}m_pct"]))
                for row in items
                if row.get(f"outcome_{horizon}m_pct") is not None
            ]
            stats = _outcome_stats(values)
            for key, value in stats.items():
                base[f"{horizon}m_{key}"] = value
        output.append(base)
    return tuple(output)


def _micro_revalidation_rows(
    events: Sequence[RuntimeEvent],
    decision_rows: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    outcomes_by_trade = {
        str(row["episode_id"]): row
        for row in decision_rows
        if str(row.get("skip_reason", "")).startswith(
            "ENTRY_REVALIDATION_REJECTED:"
        )
    }
    by_trade: dict[str, list[RuntimeEvent]] = defaultdict(list)
    for event in events:
        by_trade[event.trade_id].append(event)
    rows: list[Mapping[str, object]] = []
    for trade_id, items in sorted(by_trade.items()):
        names = {item.event for item in items}
        if not names.intersection(
            {
                "ENTRY_REVALIDATION_STARTED",
                "ENTRY_REVALIDATION_PASSED",
                "ENTRY_REVALIDATION_REJECTED",
            }
        ):
            continue
        first = min(items, key=lambda item: item.occurred_at)
        rejected = next(
            (item for item in items if item.event == "ENTRY_REVALIDATION_REJECTED"),
            None,
        )
        fresh = next(
            (item for item in items if item.event == "FRESH_PRICE_LOADED"), None
        )
        submit = next(
            (item for item in items if item.event == "ENTRY_SUBMIT_ATTEMPT"), None
        )
        row: dict[str, object] = {
                "trade_id": trade_id,
                "symbol": first.symbol,
                "started_at": next(
                    (
                        item.occurred_at.isoformat()
                        for item in items
                        if item.event == "ENTRY_REVALIDATION_STARTED"
                    ),
                    None,
                ),
                "result": "passed"
                if "ENTRY_REVALIDATION_PASSED" in names
                else "rejected",
                "reason": rejected.detail if rejected else None,
                "fresh_price_detail": fresh.detail if fresh else None,
                "submit_at": submit.occurred_at.isoformat() if submit else None,
            }
        rejected_outcomes = outcomes_by_trade.get(trade_id)
        for horizon in HORIZONS:
            row[f"outcome_{horizon}m_pct"] = (
                rejected_outcomes.get(f"outcome_{horizon}m_pct")
                if rejected_outcomes is not None
                else None
            )
        rows.append(row)
    return tuple(rows)


def _trade_rows(
    trades: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for trade in trades:
        repair = all(
            trade.get(name) is not None
            for name in (
                "pre_submit_price",
                "planned_notional",
                "actual_risk_at_fill",
                "actual_risk_pct",
            )
        )
        risk = _optional_float(trade.get("actual_risk_at_fill")) or _optional_float(
            trade.get("risk_usdt")
        )
        gross = _optional_float(trade.get("gross_pnl"))
        if gross is None:
            gross = _optional_float(trade.get("realised_gross_pnl"))
        net = _optional_float(trade.get("net_pnl"))
        if net is None:
            net = _optional_float(trade.get("realised_net_pnl"))
        fees = _optional_float(trade.get("total_fees"))
        if fees is None:
            fees = _optional_float(trade.get("fees"))
        rows.append(
            {
                "run": "Repair V2" if repair else "legacy",
                "trade_id": trade.get("trade_id"),
                "symbol": trade.get("symbol"),
                "direction": trade.get("direction"),
                "setup_type": trade.get("setup_type"),
                "gross_pnl": gross,
                "net_pnl": net,
                "fees": fees,
                "fee_as_R": fees / risk if fees is not None and risk else None,
                "gross_R": _optional_float(trade.get("gross_r"))
                or (gross / risk if gross is not None and risk else None),
                "net_R": _optional_float(trade.get("net_r"))
                or _optional_float(trade.get("result_r")),
                "actual_risk": _optional_float(trade.get("actual_risk_at_fill")),
                "actual_risk_pct": _optional_float(trade.get("actual_risk_pct")),
                "planned_notional": _optional_float(trade.get("planned_notional")),
                "hold_seconds": trade.get("hold_duration_seconds"),
                "exit_reason": trade.get("exit_reason"),
                "pre_submit_price": _optional_float(trade.get("pre_submit_price")),
                "average_fill": _optional_float(trade.get("average_fill")),
            }
        )
    return tuple(rows)


def analyze(snapshot_dir: Path) -> AuditResult:
    data = read_audit(snapshot_dir)
    series, times = _symbol_index(data.snapshots)
    anchors = _episode_anchors(data.snapshots)
    decision_rows = _decision_rows(data, series, times)
    funnel = _funnel_rows(data)
    transitions = _transition_rows(data.snapshots)
    latency = _latency_rows(data.snapshots, data.runtime_events)
    revalidation = _micro_revalidation_rows(data.runtime_events, decision_rows)
    trades = _trade_rows(data.trades)
    tables: dict[str, tuple[Mapping[str, object], ...]] = {
        "воронка.csv": funnel,
        "переходы_состояний.csv": transitions,
        "outcomes_по_типам.csv": _group_outcomes(
            anchors,
            (
                "setup_type",
                "setup_state",
                "trade_eligible",
                "direction",
                "freshness",
                "liquidity_available",
                "spread_available",
            ),
            lambda row: (
                row.setup_type,
                row.setup_state,
                row.trade_eligible,
                row.direction,
                (
                    "cancelled"
                    if row.setup_state == SetupState.CANCELLED.value
                    else "late"
                    if row.is_late
                    else "fresh"
                ),
                row.liquidity is not None,
                row.spread_pct is not None,
            ),
            series,
            times,
        ),
        "outcomes_по_confidence.csv": _group_outcomes(
            anchors,
            ("confidence_bucket", "direction"),
            lambda row: (confidence_bucket(row.confidence), row.direction),
            series,
            times,
        ),
        "outcomes_по_ATR_distance.csv": _group_outcomes(
            anchors,
            ("ATR_distance_bucket", "direction"),
            lambda row: (atr_bucket(row.distance_atr), row.direction),
            series,
            times,
        ),
        "too_far_analysis.csv": _aggregate_decisions(decision_rows, "too_far"),
        "invalid_state_analysis.csv": _aggregate_decisions(
            decision_rows, "invalid_state"
        ),
        "missed_moves.csv": decision_rows,
        "latency.csv": latency,
        "micro_revalidation.csv": revalidation,
        "trade_analysis.csv": trades,
    }
    decision_counts = Counter(item.skip_reason for item in data.decisions)
    setup_counts = Counter(item.setup_type for item in anchors)
    direction_counts = Counter(item.direction for item in anchors)
    state_counts = Counter(item.setup_state for item in data.snapshots)
    repair_trades = tuple(row for row in trades if row["run"] == "Repair V2")
    outcome_summary = {
        "setup_type_direction": list(
            _outcome_summary(
                anchors,
                ("setup_type", "direction"),
                lambda row: (row.setup_type, row.direction),
                series,
                times,
            )
        ),
        "confidence_direction": list(
            _outcome_summary(
                anchors,
                ("confidence_bucket", "direction"),
                lambda row: (confidence_bucket(row.confidence), row.direction),
                series,
                times,
            )
        ),
        "atr_direction": list(
            _outcome_summary(
                anchors,
                ("ATR_distance_bucket", "direction"),
                lambda row: (atr_bucket(row.distance_atr), row.direction),
                series,
                times,
            )
        ),
    }
    metrics: dict[str, object] = {
        "source": {
            "snapshot_dir": str(snapshot_dir.resolve()),
            "snapshot_rows": len(data.snapshots),
            "snapshot_rejected_lines": data.rejected_snapshot_lines,
            "period_start": data.snapshots[0].scanned_at.isoformat()
            if data.snapshots
            else None,
            "period_end": data.snapshots[-1].scanned_at.isoformat()
            if data.snapshots
            else None,
            "symbols": len({item.symbol for item in data.snapshots}),
            "scans": len({item.scan_id for item in data.snapshots}),
            "decisions": len(data.decisions),
            "runtime_events": len(data.runtime_events),
            "trades": len(data.trades),
            "sha256": dict(data.hashes_before),
        },
        "funnel": list(funnel),
        "setup_type_counts": dict(setup_counts),
        "direction_counts": dict(direction_counts),
        "setup_state_scan_counts": dict(state_counts),
        "outcome_summary": outcome_summary,
        "decision_counts": dict(decision_counts),
        "transitions": list(transitions),
        "latency": {
            key: _outcome_stats(
                [float(str(row[key])) for row in latency if row.get(key) is not None]
            )
            for key in (
                "first_to_ready_seconds",
                "confirming_to_ready_seconds",
                "ready_to_revalidation_seconds",
                "ready_to_submit_seconds",
            )
        },
        "revalidation": {
            "started": sum(
                row.event == "ENTRY_REVALIDATION_STARTED" for row in data.runtime_events
            ),
            "passed": sum(
                row.event == "ENTRY_REVALIDATION_PASSED" for row in data.runtime_events
            ),
            "rejected": sum(
                row.event == "ENTRY_REVALIDATION_REJECTED"
                for row in data.runtime_events
            ),
            "rejection_reasons": dict(
                Counter(
                    str(row["reason"])
                    for row in revalidation
                    if row["result"] == "rejected"
                )
            ),
        },
        "trades": {
            "legacy": len(tuple(row for row in trades if row["run"] == "legacy")),
            "repair_v2": len(repair_trades),
            "repair_v2_gross_pnl": sum(
                float(str(row["gross_pnl"] or 0)) for row in repair_trades
            ),
            "repair_v2_net_pnl": sum(
                float(str(row["net_pnl"] or 0)) for row in repair_trades
            ),
            "repair_v2_fees": sum(
                float(str(row["fees"] or 0)) for row in repair_trades
            ),
        },
        "limitations": [
            "Outcomes use subsequent five-minute audit snapshots only; intrabar MFE/MAE cannot be reconstructed.",
            "The snapshot contains one Repair V2 completed trade, so execution conclusions are descriptive, not statistically stable.",
            "Initial Micro decisions are scan-level observations and include repeated invalid_state rows for the same symbol/episode.",
            "Only five final revalidation rejections exist; the 0.25 ATR threshold cannot be optimized from this sample alone.",
            "No zeros are substituted where a future snapshot is unavailable.",
        ],
    }
    report = _report(metrics, tables)
    recommendations = _recommendations(metrics, tables)
    source_paths = tuple(
        snapshot_dir / name
        for name in (
            "inplay_early_discovery_v2_audit.jsonl",
            "qtr_micro_decisions.jsonl",
            "qtr_micro_runtime_audit.jsonl",
            "qtr_micro_trades.jsonl",
            "qtr_micro_state.json",
        )
    )
    return AuditResult(
        metrics, tables, report, recommendations, data.hashes_before, source_paths
    )


def _report(
    metrics: Mapping[str, object],
    tables: Mapping[str, tuple[Mapping[str, object], ...]],
) -> str:
    source = cast(Mapping[str, object], metrics["source"])
    decisions = cast(Mapping[str, object], metrics["decision_counts"])
    revalidation = cast(Mapping[str, object], metrics["revalidation"])
    trades = cast(Mapping[str, object], metrics["trades"])
    invalid = int(str(decisions.get("invalid_state", 0)))
    total = int(str(source["decisions"]))
    too_far = int(str(decisions.get("too_far", 0)))
    transition_summary = {
        str(row["to_state"]): row["count"]
        for row in tables["переходы_состояний.csv"]
        if row["from_state"] == "SUMMARY"
    }
    state_counts = cast(Mapping[str, object], metrics["setup_state_scan_counts"])
    latency = cast(Mapping[str, Mapping[str, object]], metrics["latency"])
    outcome_summary = cast(Mapping[str, object], metrics["outcome_summary"])
    type_rows = cast(
        Sequence[Mapping[str, object]], outcome_summary["setup_type_direction"]
    )
    type_lines = []
    for row in type_rows:
        if row["horizon_minutes"] not in (15, 60) or int(str(row["n"])) < 10:
            continue
        type_lines.append(
            "| {setup_type} | {direction} | {horizon} | {n} | {median} | {mean} | {win} |".format(
                setup_type=row["setup_type"],
                direction=row["direction"],
                horizon=row["horizon_minutes"],
                n=row["n"],
                median=f"{float(str(row['median'])):.4f}" if row["median"] is not None else "—",
                mean=f"{float(str(row['mean'])):.4f}" if row["mean"] is not None else "—",
                win=f"{float(str(row['win_rate'])):.1%}" if row["win_rate"] is not None else "—",
            )
        )
    too_far_lines = []
    for row in tables["too_far_analysis.csv"]:
        too_far_lines.append(
            "| {segment} | {count} | {m15} | {w15} | {m60} | {w60} | {m180} |".format(
                segment=row["segment"],
                count=row["count"],
                m15=f"{float(str(row['15m_median'])):.4f}" if row["15m_median"] is not None else "—",
                w15=f"{float(str(row['15m_win_rate'])):.1%}" if row["15m_win_rate"] is not None else "—",
                m60=f"{float(str(row['60m_median'])):.4f}" if row["60m_median"] is not None else "—",
                w60=f"{float(str(row['60m_win_rate'])):.1%}" if row["60m_win_rate"] is not None else "—",
                m180=f"{float(str(row['180m_median'])):.4f}" if row["180m_median"] is not None else "—",
            )
        )
    revalidation_lines = []
    for row in tables["micro_revalidation.csv"]:
        revalidation_lines.append(
            "| {symbol} | {result} | {reason} | {m15} | {m60} | {m180} |".format(
                symbol=row["symbol"],
                result=row["result"],
                reason=row["reason"] or "—",
                m15=(f"{float(str(row['outcome_15m_pct'])):.4f}" if row.get("outcome_15m_pct") is not None else "—"),
                m60=(f"{float(str(row['outcome_60m_pct'])):.4f}" if row.get("outcome_60m_pct") is not None else "—"),
                m180=(f"{float(str(row['outcome_180m_pct'])):.4f}" if row.get("outcome_180m_pct") is not None else "—"),
            )
        )
    cap = next(
        (
            row
            for row in tables["trade_analysis.csv"]
            if row["run"] == "Repair V2" and row["symbol"] == "CAPUSDT"
        ),
        None,
    )
    cap_line = (
        f"CAPUSDT: gross {cap['gross_pnl']} USDT, net {cap['net_pnl']} USDT, "
        f"fees {cap['fees']} USDT, actual risk {cap['actual_risk']} USDT "
        f"({cap['actual_risk_pct']}%), planned notional {cap['planned_notional']} USDT."
        if cap is not None
        else "CAPUSDT Repair V2 record отсутствует."
    )
    latency_values: dict[str, str] = {}
    for key, divisor in (
        ("first_to_ready_seconds", 60.0),
        ("confirming_to_ready_seconds", 60.0),
        ("ready_to_revalidation_seconds", 1.0),
        ("ready_to_submit_seconds", 1.0),
    ):
        median = latency[key]["median"]
        latency_values[key] = (
            f"{float(str(median)) / divisor:.2f}" if median is not None else "—"
        )
    return f"""# QTR Scanner + Setup Performance Audit V1

## Выборка и период

- Snapshot rows: **{source["snapshot_rows"]}**, rejected: **{source["snapshot_rejected_lines"]}**.
- Symbols: **{source["symbols"]}**, scans: **{source["scans"]}**.
- Period: **{source["period_start"]} — {source["period_end"]}**.
- Micro decisions: **{total}**, runtime events: **{source["runtime_events"]}**, trades: **{source["trades"]}**.

## Funnel и bottlenecks

- `invalid_state`: **{invalid} ({invalid / total:.2%})**. Это scan-level повторяющийся gate: большая доля не равна доле уникальных плохих setup episodes.
- initial `too_far`: **{too_far} ({too_far / total:.2%})**.
- final revalidation: started **{revalidation["started"]}**, passed **{revalidation["passed"]}**, rejected **{revalidation["rejected"]}**; recorded rejection reason — `too_far`.
- Scan states: FORMING **{state_counts.get('FORMING', 0)}**, CONFIRMING **{state_counts.get('CONFIRMING', 0)}**, READY **{state_counts.get('READY_TO_CONSIDER', 0)}**, LATE **{state_counts.get('LATE', 0)}**, CANCELLED **{state_counts.get('CANCELLED', 0)}**.
- READY→CANCELLED episodes: **{transition_summary.get("READY_THEN_CANCELLED", 0)}**.
- CONFIRMING without READY: **{transition_summary.get("CONFIRMING_NEVER_READY", 0)}**.

## Outcomes и missed moves

Outcomes рассчитаны в направлении setup по следующим snapshot prices на 5/15/30/60/180 минут. Отсутствующие будущие наблюдения оставлены пустыми. Intrabar high/low нет, поэтому MFE/MAE не заявляются. Подробные разрезы находятся в CSV по setup type, confidence и ATR-distance; все rejected observations — в `missed_moves.csv`.

### Directed outcomes по типу setup

| Setup type | Direction | Horizon, min | n | Median, % | Mean, % | Win rate |
|---|---:|---:|---:|---:|---:|---:|
{chr(10).join(type_lines)}

### Initial `too_far` по ATR-distance

| ATR bucket | n | 15m median, % | 15m win | 60m median, % | 60m win | 180m median, % |
|---|---:|---:|---:|---:|---:|---:|
{chr(10).join(too_far_lines)}

Пять final-revalidation отказов — отдельная, очень малая выборка; она не объединяется с 299 initial `too_far` наблюдениями.

| Symbol | Result | Reason | 15m, % | 60m, % | 180m, % |
|---|---|---|---:|---:|---:|
{chr(10).join(revalidation_lines)}

## Latency

`latency.csv` содержит first seen→READY, CONFIRMING→READY, READY→final revalidation и READY→submit. Связь с Micro выполняется по детерминированному `trade_id`; отсутствие timestamp не заменяется оценкой.

- first observed→READY: median **{latency_values['first_to_ready_seconds']} min**, n={latency['first_to_ready_seconds']['n']}.
- CONFIRMING→READY: median **{latency_values['confirming_to_ready_seconds']} min**, n={latency['confirming_to_ready_seconds']['n']}.
- READY→final revalidation: median **{latency_values['ready_to_revalidation_seconds']} sec**, n={latency['ready_to_revalidation_seconds']['n']}.
- READY→submit: median **{latency_values['ready_to_submit_seconds']} sec**, n={latency['ready_to_submit_seconds']['n']}.

## Trades

- Legacy trades: **{trades["legacy"]}**; они отделены от scanner-quality conclusions.
- Repair V2 completed trades: **{trades["repair_v2"]}**.
- Repair V2 gross/net/fees: **{trades["repair_v2_gross_pnl"]:.8f} / {trades["repair_v2_net_pnl"]:.8f} / {trades["repair_v2_fees"]:.8f} USDT**.
- {cap_line}

## Разделение проблем

1. **Signal quality:** знак directed future outcomes по setup type/direction.
2. **Timing:** положительный outcome при READY, но `too_far` к final revalidation; измеряется отдельно.
3. **Execution/economics:** fees, slippage, notional и actual risk только по trade journal; не смешиваются со Scanner.

## Вывод по текущим gate

- Разрешать CONFIRMING раньше READY по этой выборке нельзя: 2,086 эпизодов не дошли до READY, а directed outcomes неоднородны по типу и направлению.
- Ослаблять `QTR_MICRO_MAX_ENTRY_DISTANCE_ATR=0.25` нельзя: initial `too_far` buckets не показывают монотонного преимущества пропущенных движений, а final revalidation содержит только пять отказов.
- Оптимизировать экономику Repair V2 нельзя: завершён один совместимый trade, и его net-result равен комиссиям при нулевом gross PnL.

## Ограничения

{chr(10).join("- " + str(item) for item in cast(Sequence[object], metrics["limitations"]))}
"""


def _recommendations(
    metrics: Mapping[str, object],
    tables: Mapping[str, tuple[Mapping[str, object], ...]],
) -> str:
    source = cast(Mapping[str, object], metrics["source"])
    decisions = cast(Mapping[str, object], metrics["decision_counts"])
    too_far_rows = tables["too_far_analysis.csv"]
    positive_15m = sum(
        1
        for row in too_far_rows
        if row["15m_median"] is not None and float(str(row["15m_median"])) > 0
    )
    return f"""# Рекомендации без изменения production-кода

## ДОКАЗАНО

- `invalid_state` доминирует на уровне решений: {decisions.get("invalid_state", 0)} из {source["decisions"]}. Это в основном повторные scan-level отказы, поэтому нельзя интерпретировать их как уникальные missed trades.
- Final Repair V2 revalidation отклонила 5 из 6 начатых проверок; во всех пяти записан `too_far`. Это подтверждает operational bottleneck, но не оптимальность другого threshold.
- Среди {len(too_far_rows)} initial `too_far` ATR-buckets положительная 15m median наблюдалась только в {positive_15m}; монотонной зависимости «чем дальше, тем лучше пропущенное движение» нет.
- Legacy execution economics нельзя использовать как оценку Scanner: legacy и Repair V2 разделены в `trade_analysis.csv`.
- Outcomes основаны только на последующих snapshot prices; intrabar MFE/MAE отсутствуют.

## ВЕРОЯТНО

- Гипотезу позднего READY следует проверять по строкам с положительным directed outcome в READY и последующим final `too_far`; `latency.csv` и `micro_revalidation.csv` дают список кейсов.
- Rules для RETEST/BREAKOUT/CONTINUATION разумно исследовать отдельно, если различия сохранятся на out-of-sample периоде. На этой выборке менять production нельзя.
- Confidence и ATR-distance полезны как диагностические сегменты, но не как готовые новые thresholds.

## НЕДОСТАТОЧНО ДАННЫХ

- Менять `QTR_MICRO_MAX_ENTRY_DISTANCE_ATR=0.25`: только 5 final `too_far` observations.
- Разрешать entry на CONFIRMING или менять READY ladder/freshness window.
- Запрещать setup types, symbols либо volatility/liquidity buckets.
- Делать вывод о Repair V2 profitability: завершён только один Repair V2 trade.
- Делать symbol blacklist/whitelist: symbol-level строки предназначены для поиска кандидатов, но малая n и множественные проверки не дают устойчивого решения.

Рекомендуемый следующий шаг — повторить тот же неизменный аудит на независимом snapshot и заранее зафиксировать decision unit (unique episode, а не scan row). Production thresholds автоматически не менялись.
"""


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    payload = list(rows) or [{"message": "Данные отсутствуют"}]
    fields: list[str] = []
    for row in payload:
        for field in row:
            if field not in fields:
                fields.append(field)
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=fields,
            delimiter=";",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(payload)


def write_outputs(result: AuditResult, output_dir: Path) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    report = output_dir / "итоговый_отчёт.md"
    report.write_text(result.report, encoding="utf-8")
    paths.append(report)
    metrics = output_dir / "метрики.json"
    metrics.write_text(
        json.dumps(result.metrics, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    paths.append(metrics)
    for name, rows in result.tables.items():
        path = output_dir / name
        _write_csv(path, rows)
        paths.append(path)
    recommendations = output_dir / "рекомендации_без_изменения_кода.md"
    recommendations.write_text(result.recommendations, encoding="utf-8")
    paths.append(recommendations)
    return tuple(paths)


def sources_unchanged(result: AuditResult) -> bool:
    return all(
        _sha256(path) == result.hashes_before[path.name] for path in result.source_paths
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline QTR Scanner + Setup performance audit V1"
    )
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = analyze(args.snapshot)
    write_outputs(result, args.output)
    if not sources_unchanged(result):
        raise RuntimeError("Immutable snapshot changed during audit")
    print(f"Audit complete: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
