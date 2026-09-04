from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from market_signal_assistant.qtr_signal_outcome.models import (
    BarrierOrder,
    OutcomeStatus,
    SignalOutcome,
)


@dataclass(frozen=True, slots=True)
class SegmentMetrics:
    n: int
    median_mfe_atr: float | None
    median_mae_atr: float | None
    favorable_first_rates: Mapping[str, float | None]
    ambiguous_count: int
    median_time_to_plus_1_atr: float | None
    median_time_to_minus_1_atr: float | None
    invalidation_hit_rate: float | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "favorable_first_rates",
            MappingProxyType(dict(self.favorable_first_rates)),
        )


@dataclass(frozen=True, slots=True)
class OutcomeSummary:
    total_delivered_signals: int
    complete: int
    partial: int
    failed_market_data: int
    invalid_source_records: int
    segments: Mapping[str, SegmentMetrics]

    def __post_init__(self) -> None:
        object.__setattr__(self, "segments", MappingProxyType(dict(self.segments)))


def build_summary(
    outcomes: Iterable[SignalOutcome],
    *,
    invalid_source_records: int = 0,
) -> OutcomeSummary:
    values = tuple(outcomes)
    groups: dict[str, list[SignalOutcome]] = defaultdict(list)
    for outcome in values:
        groups["all"].append(outcome)
        groups[f"direction:{outcome.signal.direction.value}"].append(outcome)
        groups[f"setup:{outcome.signal.setup_type}"].append(outcome)
        groups[f"quality:{_quality_bucket(outcome)}"].append(outcome)
    return OutcomeSummary(
        total_delivered_signals=len(values),
        complete=sum(item.status is OutcomeStatus.COMPLETE for item in values),
        partial=sum(item.status is OutcomeStatus.PARTIAL for item in values),
        failed_market_data=sum(
            item.status is OutcomeStatus.FAILED_MARKET_DATA for item in values
        ),
        invalid_source_records=invalid_source_records,
        segments=MappingProxyType(
            {key: _segment(items) for key, items in sorted(groups.items())}
        ),
    )


def format_summary(summary: OutcomeSummary) -> str:
    lines = [
        f"Total delivered signals: {summary.total_delivered_signals}",
        f"Complete: {summary.complete}",
        f"Partial: {summary.partial}",
        f"Failed market data: {summary.failed_market_data}",
        f"Invalid source records: {summary.invalid_source_records}",
    ]
    for key, segment in summary.segments.items():
        lines.append(
            f"{key}: N={segment.n}, median MFE ATR={_show(segment.median_mfe_atr)}, "
            f"median MAE ATR={_show(segment.median_mae_atr)}, "
            f"ambiguous={segment.ambiguous_count}"
        )
        lines.append(
            "  barrier order: "
            + ", ".join(
                f"{name}={_show_rate(rate)}"
                for name, rate in segment.favorable_first_rates.items()
            )
        )
        lines.append(
            "  median minutes: "
            f"+1 ATR={_show(segment.median_time_to_plus_1_atr)}, "
            f"-1 ATR={_show(segment.median_time_to_minus_1_atr)}; "
            f"invalidation hit rate={_show_rate(segment.invalidation_hit_rate)}"
        )
    return "\n".join(lines)


def _segment(outcomes: list[SignalOutcome]) -> SegmentMetrics:
    final_horizons = [
        item.horizons[-1]
        for item in outcomes
        if item.horizons and item.horizons[-1].mfe_atr is not None
    ]
    orders = [pair for item in outcomes for pair in item.barrier_orders]
    labels = {
        (0.5, -0.5): "+0.5_before_-0.5",
        (1.0, -1.0): "+1_before_-1",
        (1.5, -1.0): "+1.5_before_-1",
        (2.0, -1.0): "+2_before_-1",
        (3.0, -1.0): "+3_before_-1",
    }
    rates: dict[str, float | None] = {}
    for thresholds, label in labels.items():
        relevant = [
            item
            for item in orders
            if (item.favorable_atr, item.adverse_atr) == thresholds
        ]
        rates[label] = (
            sum(item.order is BarrierOrder.FAVORABLE_FIRST for item in relevant)
            / len(relevant)
            if relevant
            else None
        )
    plus_times = _barrier_times(outcomes, favorable=True)
    minus_times = _barrier_times(outcomes, favorable=False)
    eligible_invalidation = [
        item for item in outcomes if item.signal.invalidation_price is not None
    ]
    return SegmentMetrics(
        n=len(outcomes),
        median_mfe_atr=_median(
            item.mfe_atr for item in final_horizons if item.mfe_atr is not None
        ),
        median_mae_atr=_median(
            item.mae_atr for item in final_horizons if item.mae_atr is not None
        ),
        favorable_first_rates=rates,
        ambiguous_count=sum(
            item.order is BarrierOrder.AMBIGUOUS_SAME_CANDLE for item in orders
        ),
        median_time_to_plus_1_atr=_median(plus_times),
        median_time_to_minus_1_atr=_median(minus_times),
        invalidation_hit_rate=(
            sum(item.invalidation_hit for item in eligible_invalidation)
            / len(eligible_invalidation)
            if eligible_invalidation
            else None
        ),
    )


def _barrier_times(outcomes: list[SignalOutcome], *, favorable: bool) -> list[float]:
    result: list[float] = []
    for outcome in outcomes:
        barriers = outcome.favorable_barriers if favorable else outcome.adverse_barriers
        for item in barriers:
            expected = 1.0 if favorable else -1.0
            if (
                item.threshold_atr == expected
                and item.first_hit_minutes_from_signal is not None
            ):
                result.append(item.first_hit_minutes_from_signal)
    return result


def _quality_bucket(outcome: SignalOutcome) -> str:
    score = outcome.signal.telegram_quality_score
    if score is None:
        return "UNKNOWN"
    if score >= 100:
        return "100"
    if score >= 90:
        return "90-99"
    if score >= 80:
        return "80-89"
    return "BELOW-80"


def _median(values: Iterable[float]) -> float | None:
    materialized = tuple(values)
    return statistics.median(materialized) if materialized else None


def _show(value: float | None) -> str:
    return "UNKNOWN" if value is None else f"{value:.3f}"


def _show_rate(value: float | None) -> str:
    return "UNKNOWN" if value is None else f"{value * 100.0:.1f}%"
