from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    MicroExperimentRecordType,
    MicroProfitRecord,
    MicroTarget,
    deserialize_micro_profit_record,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection

_SAMPLE_CAPACITY = 4_096
_MAX_EPISODES = 1_000
_MAX_GROUP_TRACKING = 5_000
_MAX_SYMBOL_BREAKDOWNS = 10_000


@dataclass(frozen=True, slots=True)
class ProfitFactor:
    gross_profit: float
    gross_loss: float
    value: float | None


@dataclass(frozen=True, slots=True)
class MicroTargetPerformance:
    plans: int
    triggered: int
    trigger_rate: float
    total: int
    hits: int
    hit_rate: float
    gross_total_r: float
    gross_average_r: float
    gross_profit_factor: ProfitFactor
    fees: float
    spread_cost: float
    slippage_cost: float
    funding_cost: float
    total_cost_r: float
    net_total_r: float
    net_average_r: float
    net_profit_factor: ProfitFactor
    net_expectancy_r: float
    signal_net_expectancy_r: float
    economically_viable: int
    economically_viable_pct: float


@dataclass(frozen=True, slots=True)
class MicroAnalyticsRow:
    scope: str
    key: str
    target: MicroTarget
    performance: MicroTargetPerformance


@dataclass(frozen=True, slots=True)
class CostFloorDistribution:
    count: int
    p10: float | None
    median: float | None
    p90: float | None


@dataclass(frozen=True, slots=True)
class TrendEpisode:
    episode_id: str
    symbol: str
    direction: ShadowDirection
    started_at: datetime
    ended_at: datetime
    setup_confidence_min: float
    setup_confidence_max: float
    setup_states: tuple[str, ...]
    baseline_entries: int
    baseline_gross_r: float
    baseline_net_r: float
    maximum_favorable_price_move_pct: float
    micro_gross_r: tuple[tuple[MicroTarget, float], ...]
    micro_net_r: tuple[tuple[MicroTarget, float], ...]
    runner_net_r: float
    profit_left_uncaptured_r: float


@dataclass(frozen=True, slots=True)
class MicroProfitAnalytics:
    rows: tuple[MicroAnalyticsRow, ...]
    cost_floor: CostFloorDistribution
    episodes: tuple[TrendEpisode, ...]
    records_processed: int
    malformed_lines_ignored: int
    retained_samples: int
    retained_episodes: int
    retained_group_states: int


@dataclass(slots=True)
class _PerformanceAccumulator:
    plans: int = 0
    triggered: int = 0
    total: int = 0
    hits: int = 0
    gross_total_r: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    fees: float = 0.0
    spread_cost: float = 0.0
    slippage_cost: float = 0.0
    funding_cost: float = 0.0
    total_cost_r: float = 0.0
    net_total_r: float = 0.0
    net_profit: float = 0.0
    net_loss: float = 0.0
    viability_observations: int = 0
    economically_viable: int = 0

    def observe_plan(self, record: MicroProfitRecord) -> None:
        self.plans += 1
        self.observe_viability(record)

    def observe_trigger(self) -> None:
        self.triggered += 1

    def observe_result(self, record: MicroProfitRecord) -> None:
        self.total += 1
        self.hits += int(
            record.record_type is MicroExperimentRecordType.TARGET_REACHED
        )
        gross = record.costs.gross_r
        net = record.costs.net_r
        self.gross_total_r += gross
        self.gross_profit += max(gross, 0.0)
        self.gross_loss += min(gross, 0.0)
        self.fees += (
            record.costs.entry_fee + record.costs.exit_fee
        ) / record.risk_per_unit
        self.spread_cost += record.costs.spread_cost / record.risk_per_unit
        self.slippage_cost += record.costs.slippage_cost / record.risk_per_unit
        self.funding_cost += record.costs.funding_cost / record.risk_per_unit
        self.total_cost_r += record.costs.total_cost_r
        self.net_total_r += net
        self.net_profit += max(net, 0.0)
        self.net_loss += min(net, 0.0)

    def observe_viability(self, record: MicroProfitRecord) -> None:
        self.viability_observations += 1
        self.economically_viable += int(
            record.target.target_r > record.costs.cost_floor_r
        )

    def freeze(self) -> MicroTargetPerformance:
        return MicroTargetPerformance(
            plans=self.plans,
            triggered=self.triggered,
            trigger_rate=(
                self.triggered / self.plans * 100 if self.plans else 0.0
            ),
            total=self.total,
            hits=self.hits,
            hit_rate=self.hits / self.total * 100 if self.total else 0.0,
            gross_total_r=self.gross_total_r,
            gross_average_r=(
                self.gross_total_r / self.total if self.total else 0.0
            ),
            gross_profit_factor=_profit_factor(
                self.gross_profit,
                self.gross_loss,
            ),
            fees=self.fees,
            spread_cost=self.spread_cost,
            slippage_cost=self.slippage_cost,
            funding_cost=self.funding_cost,
            total_cost_r=self.total_cost_r,
            net_total_r=self.net_total_r,
            net_average_r=self.net_total_r / self.total if self.total else 0.0,
            net_profit_factor=_profit_factor(self.net_profit, self.net_loss),
            net_expectancy_r=self.net_total_r / self.total if self.total else 0.0,
            signal_net_expectancy_r=(
                self.net_total_r / self.plans if self.plans else 0.0
            ),
            economically_viable=self.economically_viable,
            economically_viable_pct=(
                self.economically_viable / self.viability_observations * 100
                if self.viability_observations
                else 0.0
            ),
        )


@dataclass(slots=True)
class _EpisodeAccumulator:
    episode_id: str
    symbol: str
    direction: ShadowDirection
    started_at: datetime
    ended_at: datetime
    confidence_min: float
    confidence_max: float
    states: set[str] = field(default_factory=set)
    baseline_entries: int = 0
    baseline_gross_r: float = 0.0
    baseline_net_r: float = 0.0
    maximum_favorable_price_move_pct: float = 0.0
    micro_gross_r: dict[MicroTarget, float] = field(default_factory=dict)
    micro_net_r: dict[MicroTarget, float] = field(default_factory=dict)
    runner_net_r: float = 0.0
    profit_left_uncaptured_r: float = 0.0

    def observe(self, record: MicroProfitRecord) -> None:
        self.ended_at = max(self.ended_at, record.recorded_at)
        self.confidence_min = min(self.confidence_min, record.setup_confidence)
        self.confidence_max = max(self.confidence_max, record.setup_confidence)
        self.states.add(record.setup_state)
        favorable_pct = (
            max(
                record.maximum_excursion_before_r,
                record.maximum_excursion_after_r,
            )
            * record.risk_per_unit
            / record.entry_price
            * 100
        )
        self.maximum_favorable_price_move_pct = max(
            self.maximum_favorable_price_move_pct,
            favorable_pct,
        )
        if (
            record.target is MicroTarget.M05
            and record.record_type is MicroExperimentRecordType.ENTRY_OPENED
        ):
            self.baseline_entries += 1
        if record.record_type is MicroExperimentRecordType.BASELINE_CLOSED:
            self.baseline_gross_r += record.baseline_gross_r or 0.0
            self.baseline_net_r += record.baseline_net_r or 0.0
        if record.record_type in {
            MicroExperimentRecordType.TARGET_REACHED,
            MicroExperimentRecordType.TARGET_CLOSED,
        }:
            self.micro_gross_r[record.target] = (
                self.micro_gross_r.get(record.target, 0.0) + record.costs.gross_r
            )
            self.micro_net_r[record.target] = (
                self.micro_net_r.get(record.target, 0.0) + record.costs.net_r
            )
        if record.record_type is MicroExperimentRecordType.RUNNER_EXITED:
            self.runner_net_r += record.costs.net_r

    def freeze(self) -> TrendEpisode:
        return TrendEpisode(
            episode_id=self.episode_id,
            symbol=self.symbol,
            direction=self.direction,
            started_at=self.started_at,
            ended_at=self.ended_at,
            setup_confidence_min=self.confidence_min,
            setup_confidence_max=self.confidence_max,
            setup_states=tuple(sorted(self.states)),
            baseline_entries=self.baseline_entries,
            baseline_gross_r=self.baseline_gross_r,
            baseline_net_r=self.baseline_net_r,
            maximum_favorable_price_move_pct=(
                self.maximum_favorable_price_move_pct
            ),
            micro_gross_r=tuple(
                (target, self.micro_gross_r.get(target, 0.0))
                for target in MicroTarget
            ),
            micro_net_r=tuple(
                (target, self.micro_net_r.get(target, 0.0))
                for target in MicroTarget
            ),
            runner_net_r=self.runner_net_r,
            profit_left_uncaptured_r=self.profit_left_uncaptured_r,
        )


@dataclass(slots=True)
class _GroupOutcome:
    episode_id: str
    baseline_net_r: float | None = None
    best_runner_net_r: float | None = None
    terminal_variants: int = 0


class MicroProfitAnalyticsEngine:
    """Streaming analytics; no JSONL history is materialized in memory."""

    def __init__(
        self,
        *,
        episode_gap: timedelta = timedelta(minutes=15),
        maximum_episodes: int = _MAX_EPISODES,
        maximum_group_tracking: int = _MAX_GROUP_TRACKING,
        maximum_symbol_breakdowns: int = _MAX_SYMBOL_BREAKDOWNS,
    ) -> None:
        if episode_gap <= timedelta(0):
            raise ValueError("Episode gap must be positive.")
        if (
            maximum_episodes < 1
            or maximum_group_tracking < 1
            or maximum_symbol_breakdowns < 1
        ):
            raise ValueError("Analytics memory bounds must be positive.")
        self._episode_gap = episode_gap
        self._maximum_episodes = maximum_episodes
        self._maximum_group_tracking = maximum_group_tracking
        self._maximum_symbol_breakdowns = maximum_symbol_breakdowns

    def analyze(self, path: Path) -> MicroProfitAnalytics:
        accumulators: dict[
            tuple[str, str, MicroTarget], _PerformanceAccumulator
        ] = {}
        cost_floor = _BoundedSamples(_SAMPLE_CAPACITY)
        active_episodes: dict[
            tuple[str, ShadowDirection], _EpisodeAccumulator
        ] = {}
        episodes: deque[_EpisodeAccumulator] = deque(maxlen=self._maximum_episodes)
        group_outcomes: dict[str, _GroupOutcome] = {}
        retained_symbols: dict[str, None] = {}
        records_processed = 0
        malformed = [0]
        for record in _iter_records(path, malformed):
            records_processed += 1
            episode = _episode_for(
                record,
                active_episodes,
                episodes,
                gap=self._episode_gap,
                maximum_active=self._maximum_episodes,
            )
            episode.observe(record)
            group = group_outcomes.setdefault(
                record.experiment_group_id,
                _GroupOutcome(episode.episode_id),
            )
            if len(group_outcomes) > self._maximum_group_tracking:
                group_outcomes.pop(next(iter(group_outcomes)))
            if (
                record.record_type is MicroExperimentRecordType.CREATED
                and record.target is MicroTarget.M05
            ):
                cost_floor.add(record.costs.cost_floor_r)
            if (
                record.symbol in retained_symbols
                or len(retained_symbols) < self._maximum_symbol_breakdowns
            ):
                retained_symbols[record.symbol] = None
                include_symbol = True
            else:
                include_symbol = False
            for key in _row_keys(record, include_symbol=include_symbol):
                accumulator = accumulators.setdefault(
                    (*key, record.target),
                    _PerformanceAccumulator(),
                )
                if record.record_type is MicroExperimentRecordType.CREATED:
                    accumulator.observe_plan(record)
                elif record.record_type is MicroExperimentRecordType.ENTRY_OPENED:
                    accumulator.observe_trigger()
                if record.record_type in {
                    MicroExperimentRecordType.TARGET_REACHED,
                    MicroExperimentRecordType.TARGET_CLOSED,
                }:
                    accumulator.observe_result(record)
            if record.record_type is MicroExperimentRecordType.BASELINE_CLOSED:
                group.baseline_net_r = record.baseline_net_r
            elif record.record_type is MicroExperimentRecordType.RUNNER_EXITED:
                group.best_runner_net_r = max(
                    group.best_runner_net_r or -math.inf,
                    record.costs.net_r,
                )
            if record.record_type in {
                MicroExperimentRecordType.RUNNER_EXITED,
                MicroExperimentRecordType.TARGET_CLOSED,
                MicroExperimentRecordType.EXPIRED,
                MicroExperimentRecordType.INTERRUPTED,
            }:
                group.terminal_variants += 1
            if (
                group.baseline_net_r is not None
                and group.best_runner_net_r is not None
                and group.terminal_variants >= len(MicroTarget)
            ):
                owner = _find_episode(
                    group.episode_id,
                    active_episodes,
                    episodes,
                )
                if owner is not None:
                    owner.profit_left_uncaptured_r += max(
                        0.0,
                        group.best_runner_net_r - group.baseline_net_r,
                    )
                group_outcomes.pop(record.experiment_group_id, None)

        all_episodes = tuple(
            sorted(
                (*episodes, *active_episodes.values()),
                key=lambda item: (item.started_at, item.episode_id),
            )
        )[-self._maximum_episodes :]
        rows = tuple(
            MicroAnalyticsRow(scope, key, target, accumulator.freeze())
            for (scope, key, target), accumulator in sorted(
                accumulators.items(),
                key=lambda item: (item[0][0], item[0][1], item[0][2].value),
            )
        )
        return MicroProfitAnalytics(
            rows=rows,
            cost_floor=CostFloorDistribution(
                count=cost_floor.total_count,
                p10=cost_floor.quantile(0.10),
                median=cost_floor.quantile(0.50),
                p90=cost_floor.quantile(0.90),
            ),
            episodes=tuple(item.freeze() for item in all_episodes),
            records_processed=records_processed,
            malformed_lines_ignored=malformed[0],
            retained_samples=cost_floor.retained_count,
            retained_episodes=len(all_episodes),
            retained_group_states=len(group_outcomes),
        )


class _BoundedSamples:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._values: list[float] = []
        self._total = 0

    @property
    def total_count(self) -> int:
        return self._total

    @property
    def retained_count(self) -> int:
        return len(self._values)

    def add(self, value: float) -> None:
        self._total += 1
        if len(self._values) < self._capacity:
            self._values.append(value)
            return
        slot = (self._total * 2_654_435_761) % self._total
        if slot < self._capacity:
            self._values[slot] = value

    def quantile(self, fraction: float) -> float | None:
        if not self._values:
            return None
        ordered = sorted(self._values)
        index = round((len(ordered) - 1) * fraction)
        return ordered[index]


def format_micro_profit_report(analytics: MicroProfitAnalytics) -> str:
    lines = [
        "QTR MICRO SCALPER V2 — MICRO PROFIT SHADOW",
        (
            "Target | Plans | Trigger% | Opened N | Hit% | Gross ΣR | "
            "Net ΣR | Net PF | Viable%"
        ),
    ]
    for row in analytics.rows:
        if row.scope != "ALL":
            continue
        item = row.performance
        net_pf = (
            "∞"
            if item.net_profit_factor.value is None
            else f"{item.net_profit_factor.value:.2f}"
        )
        lines.append(
            f"{row.target.value} | {item.plans} | {item.trigger_rate:.1f} | "
            f"{item.total} | {item.hit_rate:.1f} | {item.gross_total_r:+.3f} | "
            f"{item.net_total_r:+.3f} | {net_pf} | "
            f"{item.economically_viable_pct:.1f}"
        )
    floor = analytics.cost_floor
    lines.append(
        "Cost floor R P10/Median/P90: "
        f"{_display(floor.p10)}/{_display(floor.median)}/{_display(floor.p90)}"
    )
    lines.append("Shadow-only; no production winner is selected.")
    return "\n".join(lines)


def _row_keys(
    record: MicroProfitRecord,
    *,
    include_symbol: bool,
) -> tuple[tuple[str, str], ...]:
    common = (
        ("ALL", "ALL"),
        ("DIRECTION", record.direction.value),
        ("SCORE_BAND", _score_band(record.score)),
    )
    return (*common, ("SYMBOL", record.symbol)) if include_symbol else common


def _score_band(score: float) -> str:
    if score < 50:
        return "0-49"
    if score < 65:
        return "50-64"
    if score < 80:
        return "65-79"
    return "80-100"


def _profit_factor(profit: float, loss: float) -> ProfitFactor:
    absolute_loss = abs(loss)
    value = profit / absolute_loss if absolute_loss > 0 else None
    return ProfitFactor(profit, absolute_loss, value)


def _episode_for(
    record: MicroProfitRecord,
    active: dict[tuple[str, ShadowDirection], _EpisodeAccumulator],
    completed: deque[_EpisodeAccumulator],
    *,
    gap: timedelta,
    maximum_active: int,
) -> _EpisodeAccumulator:
    key = (record.symbol, record.direction)
    current = active.get(key)
    if current is not None and record.recorded_at - current.ended_at <= gap:
        return current
    if current is not None:
        completed.append(current)
    elif len(active) >= maximum_active:
        oldest_key = min(
            active,
            key=lambda item: active[item].ended_at,
        )
        completed.append(active.pop(oldest_key))
    episode_id = (
        f"{record.symbol}-{record.direction.value}-"
        f"{record.recorded_at.isoformat()}"
    )
    current = _EpisodeAccumulator(
        episode_id=episode_id,
        symbol=record.symbol,
        direction=record.direction,
        started_at=record.recorded_at,
        ended_at=record.recorded_at,
        confidence_min=record.setup_confidence,
        confidence_max=record.setup_confidence,
    )
    active[key] = current
    return current


def _find_episode(
    episode_id: str,
    active: dict[tuple[str, ShadowDirection], _EpisodeAccumulator],
    completed: deque[_EpisodeAccumulator],
) -> _EpisodeAccumulator | None:
    for item in active.values():
        if item.episode_id == episode_id:
            return item
    return next((item for item in completed if item.episode_id == episode_id), None)


def _iter_records(
    path: Path,
    malformed: list[int],
) -> Iterator[MicroProfitRecord]:
    if not path.exists():
        return
    with path.open("rb") as stream:
        for raw_line in stream:
            if not raw_line.strip():
                continue
            if not raw_line.endswith(b"\n"):
                malformed[0] += 1
                continue
            try:
                yield deserialize_micro_profit_record(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                malformed[0] += 1


def _display(value: float | None) -> str:
    return "Нет данных" if value is None else f"{value:.4f}"
