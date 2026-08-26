from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    MicroExperimentRecordType,
    MicroProfitRecord,
    MicroTarget,
    iter_micro_profit_records,
)
from market_signal_assistant.qtr_micro_scalper.protected_runner_experiment import (
    ProtectedRunnerExitReason,
    ProtectedRunnerRecord,
    ProtectedRunnerRecordType,
    iter_protected_runner_records,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection

_SAMPLE_CAPACITY = 4_096
_PAIR_CAPACITY = 10_000


@dataclass(frozen=True, slots=True)
class PairedProfitFactor:
    gross_profit: float
    gross_loss: float
    value: float | None


@dataclass(frozen=True, slots=True)
class ProtectedRunnerPair:
    baseline_trade_id: str
    target: MicroTarget
    symbol: str
    direction: ShadowDirection
    score: float
    control_final_net_r: float
    protected_final_net_r: float
    delta_net_r: float
    control_maximum_net_r_estimated: float
    protected_maximum_net_r_observed: float
    control_profit_giveback_r_estimated: float
    protected_profit_giveback_r: float
    profit_saved_by_protection_r: float
    floor_armed: bool
    floor_exit: bool
    floor_breach_amount_r: float
    non_floor_divergence: bool
    non_floor_abs_delta_r: float


@dataclass(frozen=True, slots=True)
class ProtectedRunnerPerformance:
    paired_n: int
    control_net_total_r: float
    protected_net_total_r: float
    delta_net_total_r: float
    average_paired_delta_net_r: float
    median_paired_delta_net_r: float | None
    control_net_profit_factor: PairedProfitFactor
    protected_net_profit_factor: PairedProfitFactor
    control_final_net_negative_rate: float
    protected_final_net_negative_rate: float
    net_floor_armed_pct: float
    net_floor_exit_pct: float
    average_floor_breach_r: float
    average_control_maximum_net_r_estimated: float
    average_protected_maximum_net_r_observed: float
    average_control_profit_giveback_r_estimated: float
    average_protected_profit_giveback_r: float
    average_upside_sacrificed_r: float
    control_beats_protected: int
    protected_beats_control: int
    same_result: int
    non_floor_divergence_count: int
    non_floor_max_abs_delta_r: float


@dataclass(frozen=True, slots=True)
class ProtectedRunnerAnalyticsRow:
    scope: str
    key: str
    target: MicroTarget
    performance: ProtectedRunnerPerformance


@dataclass(frozen=True, slots=True)
class ProtectedRunnerAnalytics:
    overall: ProtectedRunnerPerformance
    rows: tuple[ProtectedRunnerAnalyticsRow, ...]
    pairs: tuple[ProtectedRunnerPair, ...]
    control_records_processed: int
    protected_records_processed: int
    interrupted_excluded: int
    unmatched_control: int
    unmatched_protected: int
    retained_pair_states: int


@dataclass(slots=True)
class _PairState:
    control: MicroProfitRecord | None = None
    protected: ProtectedRunnerRecord | None = None
    armed: bool = False


@dataclass(slots=True)
class _Accumulator:
    paired_n: int = 0
    control_total: float = 0.0
    protected_total: float = 0.0
    delta_total: float = 0.0
    control_profit: float = 0.0
    control_loss: float = 0.0
    protected_profit: float = 0.0
    protected_loss: float = 0.0
    control_negative: int = 0
    protected_negative: int = 0
    armed: int = 0
    floor_exits: int = 0
    breach_total: float = 0.0
    control_max_total: float = 0.0
    protected_max_total: float = 0.0
    control_giveback_total: float = 0.0
    protected_giveback_total: float = 0.0
    upside_sacrificed_total: float = 0.0
    control_beats: int = 0
    protected_beats: int = 0
    same: int = 0
    non_floor_divergences: int = 0
    non_floor_max_abs_delta: float = 0.0
    deltas: _BoundedSamples = field(default_factory=lambda: _BoundedSamples())

    def observe(self, pair: ProtectedRunnerPair) -> None:
        self.paired_n += 1
        control = pair.control_final_net_r
        protected = pair.protected_final_net_r
        delta = pair.delta_net_r
        self.control_total += control
        self.protected_total += protected
        self.delta_total += delta
        self.control_profit += max(control, 0.0)
        self.control_loss += min(control, 0.0)
        self.protected_profit += max(protected, 0.0)
        self.protected_loss += min(protected, 0.0)
        self.control_negative += int(control < 0)
        self.protected_negative += int(protected < 0)
        self.armed += int(pair.floor_armed)
        self.floor_exits += int(pair.floor_exit)
        self.breach_total += pair.floor_breach_amount_r
        self.control_max_total += pair.control_maximum_net_r_estimated
        self.protected_max_total += pair.protected_maximum_net_r_observed
        self.control_giveback_total += pair.control_profit_giveback_r_estimated
        self.protected_giveback_total += pair.protected_profit_giveback_r
        self.upside_sacrificed_total += max(0.0, control - protected)
        if math.isclose(control, protected, abs_tol=1e-12):
            self.same += 1
        elif control > protected:
            self.control_beats += 1
        else:
            self.protected_beats += 1
        self.non_floor_divergences += int(pair.non_floor_divergence)
        self.non_floor_max_abs_delta = max(
            self.non_floor_max_abs_delta,
            pair.non_floor_abs_delta_r,
        )
        self.deltas.add(delta)

    def freeze(self) -> ProtectedRunnerPerformance:
        count = self.paired_n
        return ProtectedRunnerPerformance(
            paired_n=count,
            control_net_total_r=self.control_total,
            protected_net_total_r=self.protected_total,
            delta_net_total_r=self.delta_total,
            average_paired_delta_net_r=self.delta_total / count if count else 0.0,
            median_paired_delta_net_r=self.deltas.median(),
            control_net_profit_factor=_profit_factor(
                self.control_profit, self.control_loss
            ),
            protected_net_profit_factor=_profit_factor(
                self.protected_profit, self.protected_loss
            ),
            control_final_net_negative_rate=(
                self.control_negative / count * 100 if count else 0.0
            ),
            protected_final_net_negative_rate=(
                self.protected_negative / count * 100 if count else 0.0
            ),
            net_floor_armed_pct=self.armed / count * 100 if count else 0.0,
            net_floor_exit_pct=self.floor_exits / count * 100 if count else 0.0,
            average_floor_breach_r=self.breach_total / count if count else 0.0,
            average_control_maximum_net_r_estimated=(
                self.control_max_total / count if count else 0.0
            ),
            average_protected_maximum_net_r_observed=(
                self.protected_max_total / count if count else 0.0
            ),
            average_control_profit_giveback_r_estimated=(
                self.control_giveback_total / count if count else 0.0
            ),
            average_protected_profit_giveback_r=(
                self.protected_giveback_total / count if count else 0.0
            ),
            average_upside_sacrificed_r=(
                self.upside_sacrificed_total / count if count else 0.0
            ),
            control_beats_protected=self.control_beats,
            protected_beats_control=self.protected_beats,
            same_result=self.same,
            non_floor_divergence_count=self.non_floor_divergences,
            non_floor_max_abs_delta_r=self.non_floor_max_abs_delta,
        )


class ProtectedRunnerAnalyticsEngine:
    """Streaming bounded A/B join by baseline trade and Micro target."""

    def __init__(
        self,
        *,
        maximum_pair_states: int = _PAIR_CAPACITY,
        maximum_pairs: int = _SAMPLE_CAPACITY,
    ) -> None:
        if maximum_pair_states < 1 or maximum_pairs < 1:
            raise ValueError("Protected analytics capacities must be positive.")
        self._maximum_pair_states = maximum_pair_states
        self._maximum_pairs = maximum_pairs

    def analyze(
        self,
        control_journal: Path,
        protected_journal: Path,
    ) -> ProtectedRunnerAnalytics:
        states: dict[tuple[str, MicroTarget], _PairState] = {}
        control_processed = 0
        protected_processed = 0
        interrupted = 0
        for protected_record in iter_protected_runner_records(protected_journal):
            protected_processed += 1
            if (
                protected_record.record_type
                is ProtectedRunnerRecordType.INTERRUPTED
            ):
                interrupted += 1
                continue
            state = _state_for(
                states,
                (protected_record.baseline_trade_id, protected_record.target),
                capacity=self._maximum_pair_states,
            )
            if (
                protected_record.record_type
                is ProtectedRunnerRecordType.NET_FLOOR_ARMED
            ):
                state.armed = True
            elif (
                protected_record.record_type
                is ProtectedRunnerRecordType.PROTECTED_RUNNER_EXITED
            ):
                state.protected = protected_record
        unmatched_control = 0
        for record in iter_micro_profit_records(control_journal):
            control_processed += 1
            if record.record_type is not MicroExperimentRecordType.RUNNER_EXITED:
                continue
            control_state = states.get((record.baseline_trade_id, record.target))
            if control_state is None:
                unmatched_control += 1
                continue
            control_state.control = record
        pairs = tuple(
            _pair(state.control, state.protected, armed=state.armed)
            for state in states.values()
            if state.control is not None and state.protected is not None
        )[-self._maximum_pairs :]
        overall = _Accumulator()
        breakdowns: dict[tuple[str, str, MicroTarget], _Accumulator] = {}
        for pair in pairs:
            overall.observe(pair)
            for scope, key in _breakdown_keys(pair):
                breakdowns.setdefault(
                    (scope, key, pair.target), _Accumulator()
                ).observe(pair)
        rows = tuple(
            ProtectedRunnerAnalyticsRow(scope, key, target, accumulator.freeze())
            for (scope, key, target), accumulator in sorted(
                breakdowns.items(),
                key=lambda item: (item[0][0], item[0][1], item[0][2].value),
            )
        )
        return ProtectedRunnerAnalytics(
            overall=overall.freeze(),
            rows=rows,
            pairs=pairs,
            control_records_processed=control_processed,
            protected_records_processed=protected_processed,
            interrupted_excluded=interrupted,
            unmatched_control=unmatched_control,
            unmatched_protected=sum(
                state.control is None and state.protected is not None
                for state in states.values()
            ),
            retained_pair_states=len(states),
        )


def format_protected_runner_report(analytics: ProtectedRunnerAnalytics) -> str:
    item = analytics.overall
    checkpoint = (
        "Engineering: first 3 Protected exits"
        if item.paired_n < 3
        else (
            "Preliminary research: 20 paired branches"
            if item.paired_n < 20
            else (
                "Stronger decision: 50+ paired branches"
                if item.paired_n >= 50
                else "Collecting toward 50 paired branches"
            )
        )
    )
    return "\n".join(
        (
            "QTR MICRO SCALPER V2 — PROTECTED NET RUNNER A/B",
            f"Paired N: {item.paired_n}",
            f"CONTROL Net ΣR: {item.control_net_total_r:+.4f}",
            f"PROTECTED Net ΣR: {item.protected_net_total_r:+.4f}",
            f"ΔNet R: {item.delta_net_total_r:+.4f}",
            f"CONTROL final Net < 0: {item.control_final_net_negative_rate:.1f}%",
            (
                "PROTECTED final Net < 0: "
                f"{item.protected_final_net_negative_rate:.1f}%"
            ),
            f"Net floor armed: {item.net_floor_armed_pct:.1f}%",
            f"Net floor exits: {item.net_floor_exit_pct:.1f}%",
            f"Non-floor divergences: {item.non_floor_divergence_count}",
            f"Non-floor max |ΔR|: {item.non_floor_max_abs_delta_r:.12g}",
            f"Checkpoint: {checkpoint}",
            "Shadow-only; no winner is selected automatically.",
        )
    )


def _state_for(
    states: dict[tuple[str, MicroTarget], _PairState],
    key: tuple[str, MicroTarget],
    *,
    capacity: int,
) -> _PairState:
    current = states.get(key)
    if current is not None:
        return current
    if len(states) >= capacity:
        states.pop(next(iter(states)))
    current = _PairState()
    states[key] = current
    return current


def _pair(
    control: MicroProfitRecord,
    protected: ProtectedRunnerRecord,
    *,
    armed: bool,
) -> ProtectedRunnerPair:
    if protected.actual_net_r is None:
        raise ValueError("Protected paired result requires terminal Net R.")
    control_net = control.costs.net_r
    protected_net = protected.actual_net_r
    floor_exit = (
        protected.exit_reason == ProtectedRunnerExitReason.NET_PROFIT_FLOOR.value
    )
    non_floor_divergence = not floor_exit and (
        protected.actual_exit_price != control.current_price
        or protected.actual_gross_r != control.costs.gross_r
        or protected.actual_total_cost_r != control.costs.total_cost_r
        or protected.actual_net_r != control.costs.net_r
        or protected.recorded_at != control.recorded_at
        or protected.exit_reason != control.runner_exit_reason
    )
    non_floor_abs_delta = 0.0 if floor_exit else abs(protected_net - control_net)
    control_max_estimate = max(
        control_net,
        control.maximum_excursion_after_r - control.costs.cost_floor_r,
    )
    protected_max = max(protected.maximum_net_r_observed, protected_net)
    return ProtectedRunnerPair(
        baseline_trade_id=control.baseline_trade_id,
        target=control.target,
        symbol=control.symbol,
        direction=control.direction,
        score=control.score,
        control_final_net_r=control_net,
        protected_final_net_r=protected_net,
        delta_net_r=protected_net - control_net,
        control_maximum_net_r_estimated=control_max_estimate,
        protected_maximum_net_r_observed=protected_max,
        control_profit_giveback_r_estimated=max(0.0, control_max_estimate-control_net),
        protected_profit_giveback_r=max(0.0, protected_max - protected_net),
        profit_saved_by_protection_r=protected_net - control_net,
        floor_armed=armed or protected.floor_armed_at is not None,
        floor_exit=floor_exit,
        floor_breach_amount_r=protected.floor_breach_amount_r or 0.0,
        non_floor_divergence=non_floor_divergence,
        non_floor_abs_delta_r=non_floor_abs_delta,
    )


def _breakdown_keys(
    pair: ProtectedRunnerPair,
) -> tuple[tuple[str, str], ...]:
    return (
        ("TARGET", pair.target.value),
        ("DIRECTION", pair.direction.value),
        ("SYMBOL", pair.symbol),
        ("SCORE_BAND", _score_band(pair.score)),
    )


def _score_band(score: float) -> str:
    if score < 50:
        return "0-49"
    if score < 65:
        return "50-64"
    if score < 80:
        return "65-79"
    return "80-100"


def _profit_factor(profit: float, loss: float) -> PairedProfitFactor:
    absolute_loss = abs(loss)
    return PairedProfitFactor(
        gross_profit=profit,
        gross_loss=absolute_loss,
        value=profit / absolute_loss if absolute_loss > 0 else None,
    )


class _BoundedSamples:
    def __init__(self, capacity: int = _SAMPLE_CAPACITY) -> None:
        self._capacity = capacity
        self._values: list[float] = []
        self._total = 0

    def add(self, value: float) -> None:
        self._total += 1
        if len(self._values) < self._capacity:
            self._values.append(value)
            return
        slot = (self._total * 2_654_435_761) % self._total
        if slot < self._capacity:
            self._values[slot] = value

    def median(self) -> float | None:
        if not self._values:
            return None
        ordered = sorted(self._values)
        middle = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) / 2
