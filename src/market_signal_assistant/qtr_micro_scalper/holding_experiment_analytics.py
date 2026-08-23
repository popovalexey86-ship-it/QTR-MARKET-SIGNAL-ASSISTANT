from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper.holding_experiment import (
    HoldingExperimentOutcome,
    HoldingExperimentRecord,
    HoldingVariant,
    deserialize_holding_experiment_record,
)

_THRESHOLDS = (0.05, 0.10, 0.25, 0.50, 1.00)
_SAMPLE_CAPACITY = 4_096


@dataclass(frozen=True, slots=True)
class HoldingPerformance:
    total: int
    wins: int
    losses: int
    breakeven: int
    not_triggered: int
    win_rate: float
    total_r: float
    average_r: float
    average_win: float | None
    average_loss: float | None
    payoff: float | None
    profit_factor: float | None
    median_mfe: float | None
    p90_mfe: float | None
    median_mae: float | None
    p90_mae: float | None
    average_holding_bars: float | None
    median_holding_bars: float | None
    average_holding_seconds: float | None
    median_holding_seconds: float | None
    exit_counts: tuple[tuple[str, int], ...]
    mfe_threshold_counts: tuple[tuple[float, int], ...]
    mae_threshold_counts: tuple[tuple[float, int], ...]


@dataclass(frozen=True, slots=True)
class HoldingAnalyticsRow:
    scope: str
    scope_value: str
    variant: HoldingVariant
    performance: HoldingPerformance


@dataclass(frozen=True, slots=True)
class HoldingPairedComparison:
    direction: str
    baseline: HoldingVariant
    challenger: HoldingVariant
    pairs: int
    improved: int
    worsened: int
    unchanged: int
    mean_delta_r: float
    median_delta_r: float
    total_delta_r: float


@dataclass(frozen=True, slots=True)
class HoldingExperimentAnalytics:
    rows: tuple[HoldingAnalyticsRow, ...]
    paired_comparisons: tuple[HoldingPairedComparison, ...]
    terminal_variants: int
    incomplete_variants: int
    malformed_lines: int


@dataclass(slots=True)
class _BoundedSeries:
    values: list[float] = field(default_factory=list)
    count: int = 0
    total: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        if len(self.values) < _SAMPLE_CAPACITY:
            self.values.append(value)
            return
        slot = (
            (self.count * 2_654_435_761) & 0xFFFFFFFF
        ) % self.count
        if slot < _SAMPLE_CAPACITY:
            self.values[slot] = value

    def average(self) -> float | None:
        return self.total / self.count if self.count else None


@dataclass(slots=True)
class _Aggregate:
    records: int = 0
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    not_triggered: int = 0
    total_r: float = 0.0
    wins_total_r: float = 0.0
    losses_total_r: float = 0.0
    mfe: _BoundedSeries = field(default_factory=_BoundedSeries)
    mae: _BoundedSeries = field(default_factory=_BoundedSeries)
    holding_bars: _BoundedSeries = field(default_factory=_BoundedSeries)
    holding_seconds: _BoundedSeries = field(default_factory=_BoundedSeries)
    exits: dict[str, int] = field(default_factory=dict)
    mfe_thresholds: dict[float, int] = field(
        default_factory=lambda: {threshold: 0 for threshold in _THRESHOLDS}
    )
    mae_thresholds: dict[float, int] = field(
        default_factory=lambda: {threshold: 0 for threshold in _THRESHOLDS}
    )

    def consume(self, record: HoldingExperimentRecord) -> None:
        self.records += 1
        self.total_r += record.result_r
        self.mfe.add(record.mfe)
        self.mae.add(record.mae)
        self.holding_bars.add(float(record.holding_completed_bars))
        if record.holding_wall_clock_seconds is not None:
            self.holding_seconds.add(record.holding_wall_clock_seconds)
        for threshold in _THRESHOLDS:
            self.mfe_thresholds[threshold] += int(record.mfe >= threshold)
            self.mae_thresholds[threshold] += int(record.mae >= threshold)
        if record.tp1_hit:
            self.exits["TP1"] = self.exits.get("TP1", 0) + 1
        if record.tp2_hit:
            self.exits["TP2"] = self.exits.get("TP2", 0) + 1
        exit_reason = record.exit_reason or "OTHER"
        if exit_reason not in {"TP1", "TP2"}:
            self.exits[exit_reason] = self.exits.get(exit_reason, 0) + 1
        if record.outcome is HoldingExperimentOutcome.WIN:
            self.wins += 1
            self.wins_total_r += record.result_r
        elif record.outcome is HoldingExperimentOutcome.LOSS:
            self.losses += 1
            self.losses_total_r += record.result_r
        elif record.outcome is HoldingExperimentOutcome.BREAKEVEN:
            self.breakeven += 1
        elif record.outcome is HoldingExperimentOutcome.NOT_TRIGGERED:
            self.not_triggered += 1

    def freeze(self) -> HoldingPerformance:
        decided = self.wins + self.losses + self.breakeven
        average_win = self.wins_total_r / self.wins if self.wins else None
        average_loss = self.losses_total_r / self.losses if self.losses else None
        payoff = (
            average_win / abs(average_loss)
            if average_win is not None
            and average_loss is not None
            and average_loss != 0
            else None
        )
        gross_profit = self.wins_total_r
        gross_loss = abs(self.losses_total_r)
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
        return HoldingPerformance(
            total=self.records,
            wins=self.wins,
            losses=self.losses,
            breakeven=self.breakeven,
            not_triggered=self.not_triggered,
            win_rate=self.wins / decided * 100.0 if decided else 0.0,
            total_r=self.total_r,
            average_r=self.total_r / self.records if self.records else 0.0,
            average_win=average_win,
            average_loss=average_loss,
            payoff=payoff,
            profit_factor=profit_factor,
            median_mfe=_median(self.mfe.values),
            p90_mfe=_percentile(self.mfe.values, 0.90),
            median_mae=_median(self.mae.values),
            p90_mae=_percentile(self.mae.values, 0.90),
            average_holding_bars=self.holding_bars.average(),
            median_holding_bars=_median(self.holding_bars.values),
            average_holding_seconds=self.holding_seconds.average(),
            median_holding_seconds=_median(self.holding_seconds.values),
            exit_counts=tuple(sorted(self.exits.items())),
            mfe_threshold_counts=tuple(self.mfe_thresholds.items()),
            mae_threshold_counts=tuple(self.mae_thresholds.items()),
        )


@dataclass(slots=True)
class _PairAggregate:
    values: _BoundedSeries = field(default_factory=_BoundedSeries)
    improved: int = 0
    worsened: int = 0
    unchanged: int = 0

    def add(self, value: float) -> None:
        self.values.add(value)
        if value > 1e-12:
            self.improved += 1
        elif value < -1e-12:
            self.worsened += 1
        else:
            self.unchanged += 1


class HoldingExperimentAnalyticsEngine:
    """Streaming offline aggregation with paired A30 comparisons."""

    def analyze(self, path: Path) -> HoldingExperimentAnalytics:
        aggregates: dict[tuple[str, str, HoldingVariant], _Aggregate] = {}
        pending_pairs: dict[
            str,
            dict[HoldingVariant, HoldingExperimentRecord],
        ] = {}
        pair_deltas: dict[tuple[str, HoldingVariant], _PairAggregate] = {}
        seen_terminal_variants: set[str] = set()
        incomplete = 0
        malformed = 0
        terminal = 0
        if path.exists():
            with path.open("rb") as stream:
                for raw_line in stream:
                    if not raw_line.strip() or not raw_line.endswith(b"\n"):
                        malformed += int(bool(raw_line.strip()))
                        continue
                    try:
                        record = deserialize_holding_experiment_record(
                            raw_line.decode("utf-8")
                        )
                    except (UnicodeDecodeError, ValueError):
                        malformed += 1
                        continue
                    if not record.terminal:
                        continue
                    if record.variant_trade_id in seen_terminal_variants:
                        continue
                    seen_terminal_variants.add(record.variant_trade_id)
                    if record.outcome is HoldingExperimentOutcome.INCOMPLETE:
                        incomplete += 1
                        continue
                    terminal += 1
                    for scope, value in _scopes(record):
                        aggregates.setdefault(
                            (scope, value, record.variant),
                            _Aggregate(),
                        ).consume(record)
                    variants = pending_pairs.setdefault(
                        record.experiment_group_id,
                        {},
                    )
                    variants[record.variant] = record
                    if len(variants) == len(HoldingVariant):
                        self._consume_pairs(variants, pair_deltas)
                        pending_pairs.pop(record.experiment_group_id, None)
        rows = tuple(
            HoldingAnalyticsRow(scope, value, variant, aggregate.freeze())
            for (scope, value, variant), aggregate in sorted(
                aggregates.items(),
                key=lambda item: (item[0][0], item[0][1], item[0][2].value),
            )
        )
        comparisons = tuple(
            _paired(direction, variant, values)
            for (direction, variant), values in sorted(
                pair_deltas.items(),
                key=lambda item: (item[0][0], item[0][1].value),
            )
        )
        return HoldingExperimentAnalytics(
            rows=rows,
            paired_comparisons=comparisons,
            terminal_variants=terminal,
            incomplete_variants=incomplete,
            malformed_lines=malformed,
        )

    @staticmethod
    def _consume_pairs(
        variants: dict[HoldingVariant, HoldingExperimentRecord],
        deltas: dict[tuple[str, HoldingVariant], _PairAggregate],
    ) -> None:
        baseline = variants[HoldingVariant.A30]
        for challenger in (
            HoldingVariant.B60,
            HoldingVariant.C120,
            HoldingVariant.D300,
        ):
            delta = variants[challenger].result_r - baseline.result_r
            deltas.setdefault(("ALL", challenger), _PairAggregate()).add(delta)
            deltas.setdefault(
                (baseline.direction.value, challenger),
                _PairAggregate(),
            ).add(delta)


def format_holding_experiment_report(
    analytics: HoldingExperimentAnalytics,
) -> str:
    lines = ["QTR MICRO SCALPER V2 — HOLDING EXPERIMENT", ""]
    for scope, value in (
        ("ALL", "ALL"),
        ("DIRECTION", "LONG"),
        ("DIRECTION", "SHORT"),
    ):
        lines.extend((f"=== {value} ===", _table_header()))
        rows = [
            row
            for row in analytics.rows
            if row.scope == scope and row.scope_value == value
        ]
        lines.extend(_format_row(row) for row in rows)
        lines.append("")
    lines.append("=== SCORE BANDS ===")
    for row in analytics.rows:
        if row.scope == "SCORE_BAND":
            lines.append(f"{row.scope_value} | {_format_row(row)}")
    lines.extend(("", "=== PAIRED A30 COMPARISON ==="))
    for item in analytics.paired_comparisons:
        lines.append(
            f"{item.direction}: A30 vs {item.challenger.value} | "
            f"N={item.pairs} improved={item.improved} worsened={item.worsened} "
            f"unchanged={item.unchanged} mean ΔR={item.mean_delta_r:+.4f} "
            f"median ΔR={item.median_delta_r:+.4f} "
            f"ΣΔR={item.total_delta_r:+.4f}"
        )
    lines.append(
        f"INCOMPLETE excluded: {analytics.incomplete_variants}; "
        f"malformed lines: {analytics.malformed_lines}"
    )
    return "\n".join(lines)


def _scopes(record: HoldingExperimentRecord) -> tuple[tuple[str, str], ...]:
    return (
        ("ALL", "ALL"),
        ("DIRECTION", record.direction.value),
        ("SCORE_BAND", _score_band(record.score)),
    )


def _score_band(score: float) -> str:
    if score < 70:
        return "65-69.99"
    if score < 75:
        return "70-74.99"
    if score < 80:
        return "75-79.99"
    if score < 85:
        return "80-84.99"
    return "85+"


def _paired(
    direction: str,
    challenger: HoldingVariant,
    aggregate: _PairAggregate,
) -> HoldingPairedComparison:
    return HoldingPairedComparison(
        direction=direction,
        baseline=HoldingVariant.A30,
        challenger=challenger,
        pairs=aggregate.values.count,
        improved=aggregate.improved,
        worsened=aggregate.worsened,
        unchanged=aggregate.unchanged,
        mean_delta_r=aggregate.values.average() or 0.0,
        median_delta_r=(
            float(statistics.median(aggregate.values.values))
            if aggregate.values.values
            else 0.0
        ),
        total_delta_r=aggregate.values.total,
    )


def _table_header() -> str:
    return "Variant | N | WIN | LOSS | WR | ΣR | AvgR | AvgWin | AvgLoss | Payoff | PF"


def _format_row(row: HoldingAnalyticsRow) -> str:
    item = row.performance
    return " | ".join(
        (
            row.variant.value,
            str(item.total),
            str(item.wins),
            str(item.losses),
            f"{item.win_rate:.2f}%",
            f"{item.total_r:+.4f}",
            f"{item.average_r:+.4f}",
            _optional(item.average_win),
            _optional(item.average_loss),
            _optional(item.payoff),
            _optional(item.profit_factor),
        )
    )


def _optional(value: float | None) -> str:
    return "N/A" if value is None else f"{value:.4f}"


def _median(values: list[float]) -> float | None:
    return float(statistics.median(values)) if values else None


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight
