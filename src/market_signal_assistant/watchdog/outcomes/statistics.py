from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import median

from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.outcomes.models import (
    OUTCOME_HORIZONS_MINUTES,
    ForwardOutcome,
    OutcomeDataQuality,
)


@dataclass(frozen=True, slots=True)
class StatisticsBreakdown:
    dimension: str
    key: str
    event_count: int
    outcome_count: int
    median_abs_return: float | None


@dataclass(frozen=True, slots=True)
class WatchdogStatisticsReport:
    event_count: int
    events_by_anomaly_type: tuple[tuple[str, int], ...]
    events_by_state: tuple[tuple[str, int], ...]
    events_by_score_bucket: tuple[tuple[str, int], ...]
    median_abs_return_by_horizon: tuple[tuple[int, float | None], ...]
    median_max_abs_excursion_by_horizon: tuple[tuple[int, float | None], ...]
    median_mfe_up_by_horizon: tuple[tuple[int, float | None], ...]
    median_mfe_down_by_horizon: tuple[tuple[int, float | None], ...]
    expansion_rate: float | None
    median_time_to_expansion_seconds: float | None
    missing_outcome_rate: float
    late_outcome_rate: float
    breakdowns: tuple[StatisticsBreakdown, ...]


class WatchdogDescriptiveStatistics:
    """Descriptive evidence summary; it performs no threshold optimization."""

    def analyze(
        self,
        events: tuple[WatchdogEventEvidence, ...],
        outcomes: tuple[ForwardOutcome, ...],
    ) -> WatchdogStatisticsReport:
        event_by_id = {item.event_id: item for item in events}
        if len(event_by_id) != len(events):
            raise ValueError("Statistics input contains duplicate events.")
        if any(item.event_id not in event_by_id for item in outcomes):
            raise ValueError("Outcome references an unknown Watchdog event.")
        anomaly_counts: Counter[str] = Counter()
        state_counts: Counter[str] = Counter()
        score_counts: Counter[str] = Counter()
        for event in events:
            anomaly_counts.update(item.value for item in event.anomaly_types)
            state_counts[event.state_after.value] += 1
            score_counts[_score_bucket(event.anomaly_score)] += 1

        observed = tuple(
            item
            for item in outcomes
            if item.data_quality is not OutcomeDataQuality.MISSING
        )
        expansion_by_event: dict[str, ForwardOutcome] = {}
        for item in outcomes:
            if item.did_expansion_occur is None:
                continue
            previous = expansion_by_event.get(item.event_id)
            if previous is None or item.horizon_minutes > previous.horizon_minutes:
                expansion_by_event[item.event_id] = item
        expansion_values = tuple(
            item.did_expansion_occur for item in expansion_by_event.values()
        )
        expansion_times = tuple(
            item.time_to_expansion_seconds
            for item in expansion_by_event.values()
            if item.time_to_expansion_seconds is not None
        )
        total = len(outcomes)
        return WatchdogStatisticsReport(
            event_count=len(events),
            events_by_anomaly_type=tuple(sorted(anomaly_counts.items())),
            events_by_state=tuple(sorted(state_counts.items())),
            events_by_score_bucket=tuple(sorted(score_counts.items())),
            median_abs_return_by_horizon=_horizon_medians(observed, "abs_return"),
            median_max_abs_excursion_by_horizon=_horizon_medians(
                observed, "max_abs_excursion"
            ),
            median_mfe_up_by_horizon=_horizon_medians(observed, "mfe_up"),
            median_mfe_down_by_horizon=_horizon_medians(observed, "mfe_down"),
            expansion_rate=(
                sum(bool(item) for item in expansion_values) / len(expansion_values)
                if expansion_values
                else None
            ),
            median_time_to_expansion_seconds=(
                median(expansion_times) if expansion_times else None
            ),
            missing_outcome_rate=(
                sum(
                    item.data_quality is OutcomeDataQuality.MISSING
                    for item in outcomes
                )
                / total
                if total
                else 0.0
            ),
            late_outcome_rate=(
                sum(item.data_quality is OutcomeDataQuality.LATE for item in outcomes)
                / total
                if total
                else 0.0
            ),
            breakdowns=_breakdowns(events, outcomes),
        )


def _horizon_medians(
    outcomes: tuple[ForwardOutcome, ...],
    field: str,
) -> tuple[tuple[int, float | None], ...]:
    rows = []
    for horizon in OUTCOME_HORIZONS_MINUTES:
        values = tuple(
            value
            for item in outcomes
            if item.horizon_minutes == horizon
            and (value := getattr(item, field)) is not None
        )
        rows.append((horizon, median(values) if values else None))
    return tuple(rows)


def _breakdowns(
    events: tuple[WatchdogEventEvidence, ...],
    outcomes: tuple[ForwardOutcome, ...],
) -> tuple[StatisticsBreakdown, ...]:
    event_groups: dict[tuple[str, str], set[str]] = defaultdict(set)
    for event in events:
        keys: set[tuple[str, str]] = {
            ("anomaly_type", item.value) for item in event.anomaly_types
        }
        keys.update(
            {
            (
                "anomaly_combination",
                "+".join(sorted(item.value for item in event.anomaly_types)),
            ),
            ("score_bucket", _score_bucket(event.anomaly_score)),
            ("symbol", event.symbol),
            ("universe_tier", event.universe_tier),
            }
        )
        for group_key in keys:
            event_groups[group_key].add(event.event_id)
    rows = []
    for (dimension, label), event_ids in sorted(event_groups.items()):
        related = tuple(item for item in outcomes if item.event_id in event_ids)
        values = tuple(
            item.abs_return for item in related if item.abs_return is not None
        )
        rows.append(
            StatisticsBreakdown(
                dimension,
                label,
                len(event_ids),
                len(related),
                median(values) if values else None,
            )
        )
    return tuple(rows)


def _score_bucket(score: float) -> str:
    lower = min(80, int(score // 20) * 20)
    upper = 100 if lower == 80 else lower + 19
    return f"{lower:02d}-{upper:03d}"
