from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import cast

from market_signal_assistant.qtr_micro_scalper.data.models import PublicTradeEvent
from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    ContinuationEvidence,
    CostAccrual,
    CostBreakdown,
    MicroCostModelConfig,
    MicroExperimentRecordType,
    MicroProfitExperimentConfig,
    MicroProfitRecord,
    MicroTarget,
    calculate_cost_breakdown,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection

DEFAULT_PROTECTED_RUNNER_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_micro_scalper_protected_runner_experiment.jsonl"
)
_SCHEMA_VERSION = 1
_RECENT_ID_CAPACITY = 100_000
_RECOVERED_ACTIVE_CAPACITY = 10_000
_MAX_RECOVERY_WARNINGS = 1_000


class ProtectedRunnerRecordType(StrEnum):
    PROTECTED_RUNNER_CREATED = "PROTECTED_RUNNER_CREATED"
    NET_FLOOR_ARMED = "NET_FLOOR_ARMED"
    PROTECTED_RUNNER_EXITED = "PROTECTED_RUNNER_EXITED"
    INTERRUPTED = "INTERRUPTED"


class ProtectedRunnerStage(StrEnum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    INTERRUPTED = "INTERRUPTED"


class ProtectedRunnerOutcome(StrEnum):
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


class ProtectedRunnerExitReason(StrEnum):
    NET_PROFIT_FLOOR = "NET_PROFIT_FLOOR"
    DIRECTIONAL_EVIDENCE_LOST = "DIRECTIONAL_EVIDENCE_LOST"
    OPPOSITE_MARKET_STATE = "OPPOSITE_MARKET_STATE"
    STRUCTURAL_INVALIDATION = "STRUCTURAL_INVALIDATION"
    TRAILING_EXCURSION = "TRAILING_EXCURSION"
    MAXIMUM_SAFETY_HORIZON = "MAXIMUM_SAFETY_HORIZON"
    STOP = "STOP"
    INTERRUPTED = "INTERRUPTED"


@dataclass(frozen=True, slots=True)
class ProtectedRunnerConfig:
    enabled: bool = False
    protected_min_net_r: float = 0.0
    maximum_active_branches: int = 1_000
    runner_trailing_r: float = 0.10
    maximum_safety_bars: int = 300
    cost_model: MicroCostModelConfig = field(default_factory=MicroCostModelConfig)

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("Protected runner enabled flag must be boolean.")
        if not _finite(self.protected_min_net_r):
            raise ValueError("Protected runner Net floor must be finite.")
        if (
            isinstance(self.maximum_active_branches, bool)
            or not 1 <= self.maximum_active_branches <= 10_000
        ):
            raise ValueError(
                "Protected runner active-branch capacity must be within 1..10000."
            )
        if not _positive(self.runner_trailing_r):
            raise ValueError("Protected runner trailing R must be positive.")
        if (
            isinstance(self.maximum_safety_bars, bool)
            or self.maximum_safety_bars < 1
        ):
            raise ValueError("Protected runner safety horizon must be positive.")

    @classmethod
    def from_environment(
        cls,
        micro_config: MicroProfitExperimentConfig,
    ) -> ProtectedRunnerConfig:
        return cls(
            enabled=_environment_bool(
                "QTR_SCALPER_V2_PROTECTED_RUNNER_ENABLED",
                default=False,
            ),
            protected_min_net_r=_environment_float(
                "QTR_SCALPER_V2_PROTECTED_RUNNER_MIN_NET_R",
                default=0.0,
            ),
            maximum_active_branches=_environment_int(
                "QTR_SCALPER_V2_PROTECTED_RUNNER_MAX_ACTIVE_BRANCHES",
                default=1_000,
            ),
            runner_trailing_r=micro_config.runner_trailing_r,
            maximum_safety_bars=micro_config.maximum_safety_bars,
            cost_model=micro_config.cost_model,
        )


@dataclass(frozen=True, slots=True)
class ProtectedRunnerRecord:
    record_id: str = field(init=False)
    recorded_at: datetime
    record_type: ProtectedRunnerRecordType
    branch_id: str
    experiment_group_id: str
    baseline_trade_id: str
    source_variant_id: str
    symbol: str
    direction: ShadowDirection
    target: MicroTarget
    score: float
    setup_state: str
    market_state: str
    entry_at: datetime
    entry_price: float
    initial_stop: float
    risk_per_unit: float
    target_reached_at: datetime
    target_reached_price: float
    stage: ProtectedRunnerStage
    outcome: ProtectedRunnerOutcome
    requested_net_floor_r: float
    estimated_net_r_before_exit: float
    current_price: float
    actual_exit_price: float | None
    actual_gross_r: float | None
    actual_total_cost_r: float | None
    actual_net_r: float | None
    floor_breach_amount_r: float | None
    floor_armed_at: datetime | None
    net_r_at_floor_arm: float | None
    maximum_net_r_observed: float
    maximum_excursion_after_target_r: float
    wall_clock_seconds: float
    completed_bars: int
    exit_reason: str | None
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in (
            "branch_id",
            "experiment_group_id",
            "baseline_trade_id",
            "source_variant_id",
            "setup_state",
            "market_state",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"Protected runner {name} cannot be empty.")
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Protected runner symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        for name in (
            "recorded_at",
            "entry_at",
            "target_reached_at",
            "floor_armed_at",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value))
        for name, value in (
            ("entry price", self.entry_price),
            ("initial stop", self.initial_stop),
            ("risk per unit", self.risk_per_unit),
            ("target reached price", self.target_reached_price),
            ("current price", self.current_price),
        ):
            if not _positive(value):
                raise ValueError(f"Protected runner {name} must be positive.")
        if self.actual_exit_price is not None and not _positive(
            self.actual_exit_price
        ):
            raise ValueError("Protected runner actual exit price must be positive.")
        for name in (
            "score",
            "requested_net_floor_r",
            "estimated_net_r_before_exit",
            "maximum_net_r_observed",
            "maximum_excursion_after_target_r",
            "wall_clock_seconds",
        ):
            if not _finite(float(getattr(self, name))):
                raise ValueError(f"Protected runner {name} must be finite.")
        if not 0 <= self.score <= 100:
            raise ValueError("Protected runner score must be within 0..100.")
        for name in (
            "actual_gross_r",
            "actual_total_cost_r",
            "actual_net_r",
            "floor_breach_amount_r",
            "net_r_at_floor_arm",
        ):
            value = getattr(self, name)
            if value is not None and not _finite(value):
                raise ValueError(f"Protected runner {name} must be finite.")
        if self.actual_total_cost_r is not None and self.actual_total_cost_r < 0:
            raise ValueError("Protected runner actual costs cannot be negative.")
        if self.wall_clock_seconds < 0:
            raise ValueError("Protected runner wall-clock duration cannot be negative.")
        if isinstance(self.completed_bars, bool) or self.completed_bars < 0:
            raise ValueError("Protected runner completed bars cannot be negative.")
        if (
            self.record_type
            is ProtectedRunnerRecordType.PROTECTED_RUNNER_EXITED
            and None
            in (
                self.actual_exit_price,
                self.actual_gross_r,
                self.actual_total_cost_r,
                self.actual_net_r,
            )
        ):
            raise ValueError("Protected runner exit requires actual economics.")
        if self.stage is ProtectedRunnerStage.INTERRUPTED:
            if self.outcome is not ProtectedRunnerOutcome.INCOMPLETE:
                raise ValueError("Interrupted protected runner must be incomplete.")
        elif self.outcome is ProtectedRunnerOutcome.INCOMPLETE:
            raise ValueError("Only interrupted protected runner may be incomplete.")
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("Protected runner schema version is unsupported.")
        object.__setattr__(self, "record_id", _record_id(self))

    @property
    def terminal(self) -> bool:
        return self.stage in {
            ProtectedRunnerStage.CLOSED,
            ProtectedRunnerStage.INTERRUPTED,
        }


@dataclass(frozen=True, slots=True)
class ProtectedRunnerRecovery:
    active_records: tuple[ProtectedRunnerRecord, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProtectedRunnerJournalMetrics:
    bootstrap_scans: int
    bytes_read: int
    records_processed: int
    recent_record_ids: int
    malformed_lines: int
    active_recovered_branches: int


class ProtectedRunnerJournal:
    """Append-only transition journal with streaming bounded recovery."""

    def __init__(
        self,
        path: Path = DEFAULT_PROTECTED_RUNNER_JOURNAL_PATH,
        *,
        maximum_recent_record_ids: int = _RECENT_ID_CAPACITY,
        maximum_recovered_active: int = _RECOVERED_ACTIVE_CAPACITY,
    ) -> None:
        if (
            isinstance(maximum_recent_record_ids, bool)
            or maximum_recent_record_ids < 1
        ):
            raise ValueError("Protected journal recent-ID capacity must be positive.")
        if isinstance(maximum_recovered_active, bool) or maximum_recovered_active < 1:
            raise ValueError(
                "Protected journal recovered-active capacity must be positive."
            )
        self._path = path.resolve()
        self._maximum_recent_record_ids = maximum_recent_record_ids
        self._maximum_recovered_active = maximum_recovered_active
        self._recent_record_ids: dict[str, None] = {}
        self._active_recovered: dict[str, ProtectedRunnerRecord] = {}
        self._warnings: list[str] = []
        self._bootstrap_scans = 0
        self._bytes_read = 0
        self._records_processed = 0
        self._malformed_lines = 0
        self._lock = Lock()
        self._recover_streaming()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> ProtectedRunnerRecovery:
        with self._lock:
            return ProtectedRunnerRecovery(
                active_records=tuple(
                    sorted(
                        self._active_recovered.values(),
                        key=lambda item: item.branch_id,
                    )
                ),
                warnings=tuple(self._warnings),
            )

    @property
    def metrics(self) -> ProtectedRunnerJournalMetrics:
        with self._lock:
            return ProtectedRunnerJournalMetrics(
                bootstrap_scans=self._bootstrap_scans,
                bytes_read=self._bytes_read,
                records_processed=self._records_processed,
                recent_record_ids=len(self._recent_record_ids),
                malformed_lines=self._malformed_lines,
                active_recovered_branches=len(self._active_recovered),
            )

    def append(self, record: ProtectedRunnerRecord) -> bool:
        line = serialize_protected_runner_record(record)
        with self._lock:
            if record.record_id in self._recent_record_ids:
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            needs_separator = self._needs_separator()
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                if needs_separator:
                    stream.write("\n")
                stream.write(line)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._remember(record.record_id)
            self._records_processed += 1
            self._track_active(record)
            return True

    def clear_recovered_active(self) -> None:
        with self._lock:
            self._active_recovered.clear()

    def flush(self) -> None:
        with self._lock:
            if not self._path.exists():
                return
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.flush()
                os.fsync(stream.fileno())

    def _recover_streaming(self) -> None:
        if not self._path.exists():
            return
        self._bootstrap_scans += 1
        with self._path.open("rb") as stream:
            for line_number, raw_line in enumerate(stream, start=1):
                self._bytes_read += len(raw_line)
                if not raw_line.strip():
                    continue
                if not raw_line.endswith(b"\n"):
                    self._warn(line_number, "incomplete trailing line")
                    continue
                try:
                    record = deserialize_protected_runner_record(
                        raw_line.decode("utf-8")
                    )
                except (UnicodeDecodeError, ValueError) as exc:
                    self._warn(line_number, str(exc))
                    continue
                self._records_processed += 1
                self._remember(record.record_id)
                self._track_active(record)

    def _track_active(self, record: ProtectedRunnerRecord) -> None:
        if record.terminal:
            self._active_recovered.pop(record.branch_id, None)
        else:
            self._active_recovered[record.branch_id] = record
            if len(self._active_recovered) > self._maximum_recovered_active:
                evicted = next(iter(self._active_recovered))
                self._active_recovered.pop(evicted)
                if len(self._warnings) < _MAX_RECOVERY_WARNINGS:
                    self._warnings.append(
                        "Protected recovery active-branch capacity exceeded; "
                        f"oldest branch {evicted} was not recovered."
                    )

    def _remember(self, record_id: str) -> None:
        self._recent_record_ids[record_id] = None
        if len(self._recent_record_ids) > self._maximum_recent_record_ids:
            del self._recent_record_ids[next(iter(self._recent_record_ids))]

    def _warn(self, line_number: int, reason: str) -> None:
        self._malformed_lines += 1
        if len(self._warnings) < _MAX_RECOVERY_WARNINGS:
            self._warnings.append(
                f"Protected runner line {line_number} ignored: {reason}"
            )

    def _needs_separator(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with self._path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            return stream.read(1) != b"\n"


@dataclass(frozen=True, slots=True)
class ProtectedRunnerRuntimeMetrics:
    active_branches: int
    protected_symbols: int
    branches_created: int
    branches_armed: int
    branches_completed: int
    capacity_rejections: int
    duplicate_targets: int
    interrupted_branches: int
    retained_branch_ids: int
    last_warning: str | None


@dataclass(slots=True)
class _ProtectedBranch:
    source: MicroProfitRecord
    evidence: ContinuationEvidence
    branch_id: str
    target_reached_price: float
    current_price: float
    last_event_at: datetime
    last_event_id: str
    last_bucket: int
    completed_bars: int
    armed_at: datetime | None = None
    net_r_at_floor_arm: float | None = None
    maximum_net_r: float = -math.inf
    maximum_gross_r: float = 0.0


class ProtectedRunnerRuntime:
    """Net-floor observer whose non-floor terminal authority is CONTROL."""

    def __init__(
        self,
        journal: ProtectedRunnerJournal,
        config: ProtectedRunnerConfig,
    ) -> None:
        self._journal = journal
        self._config = config
        self._branches: dict[str, _ProtectedBranch] = {}
        self._branches_by_symbol: dict[str, set[str]] = {}
        self._branches_by_source: dict[tuple[str, MicroTarget, str], str] = {}
        self._seen_branches: dict[str, None] = {}
        self._branches_created = 0
        self._branches_armed = 0
        self._branches_completed = 0
        self._capacity_rejections = 0
        self._duplicate_targets = 0
        self._interrupted_branches = 0
        self._last_warning: str | None = None
        self._started = False
        self._lock = Lock()

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def start(self, *, at: datetime) -> tuple[ProtectedRunnerRecord, ...]:
        normalized = _utc(at)
        with self._lock:
            if self._started or not self.enabled:
                return ()
            interrupted = tuple(
                replace(
                    record,
                    recorded_at=normalized,
                    record_type=ProtectedRunnerRecordType.INTERRUPTED,
                    stage=ProtectedRunnerStage.INTERRUPTED,
                    outcome=ProtectedRunnerOutcome.INCOMPLETE,
                    exit_reason=ProtectedRunnerExitReason.INTERRUPTED.value,
                )
                for record in self._journal.recovery.active_records
            )
            persisted = tuple(
                record for record in interrupted if self._journal.append(record)
            )
            self._journal.clear_recovered_active()
            self._interrupted_branches += len(persisted)
            self._started = True
            return persisted

    def observe_micro_records(
        self,
        records: Iterable[MicroProfitRecord],
        *,
        evidence: ContinuationEvidence | None,
        event: PublicTradeEvent | None,
    ) -> tuple[ProtectedRunnerRecord, ...]:
        if not self.enabled:
            return ()
        with self._lock:
            persisted: list[ProtectedRunnerRecord] = []
            for source in records:
                if source.record_type is MicroExperimentRecordType.RUNNER_EXITED:
                    mirrored = self._mirror_control_exit(source)
                    if mirrored is not None:
                        persisted.append(mirrored)
                    continue
                if source.record_type is not MicroExperimentRecordType.TARGET_REACHED:
                    continue
                if evidence is None or event is None:
                    continue
                if source.entry_at is None or source.symbol != event.symbol:
                    continue
                branch_id = _branch_id(source)
                if branch_id in self._branches or branch_id in self._seen_branches:
                    self._duplicate_targets += 1
                    continue
                if len(self._branches) >= self._config.maximum_active_branches:
                    self._capacity_rejections += 1
                    self._last_warning = (
                        "Protected runner capacity reached; CONTROL remains unaffected."
                    )
                    continue
                branch = self._new_branch(source, evidence=evidence, event=event)
                self._branches[branch_id] = branch
                self._branches_by_symbol.setdefault(source.symbol, set()).add(
                    branch_id
                )
                self._branches_by_source[_source_key(source)] = branch_id
                self._remember_branch(branch_id)
                self._branches_created += 1
                created = self._record(
                    branch,
                    record_type=(
                        ProtectedRunnerRecordType.PROTECTED_RUNNER_CREATED
                    ),
                    at=event.exchange_at,
                    price=event.price,
                )
                if self._journal.append(created):
                    persisted.append(created)
                if created.estimated_net_r_before_exit > (
                    self._config.protected_min_net_r
                ):
                    persisted.extend(self._arm(branch, created))
            return tuple(persisted)

    def process_event(
        self,
        event: PublicTradeEvent,
    ) -> tuple[ProtectedRunnerRecord, ...]:
        if not self.enabled:
            return ()
        with self._lock:
            persisted: list[ProtectedRunnerRecord] = []
            for branch_id in tuple(
                sorted(self._branches_by_symbol.get(event.symbol, ()))
            ):
                branch = self._branches.get(branch_id)
                if branch is None or not _newer_event(branch, event):
                    continue
                self._observe_event(branch, event)
                gross_r = _gross_r(branch.source, event.price)
                costs = self._costs(branch, at=event.exchange_at, price=event.price)
                branch.maximum_gross_r = max(branch.maximum_gross_r, gross_r)
                branch.maximum_net_r = max(branch.maximum_net_r, costs.net_r)
                if (
                    branch.armed_at is not None
                    and costs.net_r <= self._config.protected_min_net_r
                ):
                    record = self._exit(
                        branch,
                        at=event.exchange_at,
                        price=event.price,
                        reason=ProtectedRunnerExitReason.NET_PROFIT_FLOOR,
                    )
                    if self._journal.append(record):
                        persisted.append(record)
                    self._remove_branch(branch)
                    self._branches_completed += 1
                elif (
                    branch.armed_at is None
                    and costs.net_r > self._config.protected_min_net_r
                ):
                    snapshot = self._record(
                        branch,
                        record_type=(
                            ProtectedRunnerRecordType.PROTECTED_RUNNER_CREATED
                        ),
                        at=event.exchange_at,
                        price=event.price,
                    )
                    persisted.extend(self._arm(branch, snapshot))
            return tuple(persisted)

    def update_evidence(
        self,
        evidence: ContinuationEvidence,
    ) -> tuple[ProtectedRunnerRecord, ...]:
        if not self.enabled:
            return ()
        with self._lock:
            persisted: list[ProtectedRunnerRecord] = []
            for branch_id in tuple(
                sorted(self._branches_by_symbol.get(evidence.symbol, ()))
            ):
                branch = self._branches.get(branch_id)
                if branch is None:
                    continue
                branch.evidence = evidence
            return tuple(persisted)

    def _mirror_control_exit(
        self,
        source: MicroProfitRecord,
    ) -> ProtectedRunnerRecord | None:
        branch_id = self._branches_by_source.get(_source_key(source))
        branch = self._branches.get(branch_id) if branch_id is not None else None
        if branch is None:
            return None
        branch.current_price = source.current_price
        branch.last_event_at = source.recorded_at
        branch.completed_bars = source.completed_bars
        branch.maximum_gross_r = max(
            branch.maximum_gross_r,
            source.costs.gross_r,
        )
        branch.maximum_net_r = max(branch.maximum_net_r, source.costs.net_r)
        record = self._record(
            branch,
            record_type=ProtectedRunnerRecordType.PROTECTED_RUNNER_EXITED,
            at=source.recorded_at,
            price=source.current_price,
            stage=ProtectedRunnerStage.CLOSED,
            outcome=ProtectedRunnerOutcome.COMPLETE,
            exit_reason=source.runner_exit_reason,
            actual_exit_price=source.current_price,
            actual_gross_r=source.costs.gross_r,
            actual_total_cost_r=source.costs.total_cost_r,
            actual_net_r=source.costs.net_r,
            estimated_net_r_before_exit=source.costs.net_r,
            completed_bars=source.completed_bars,
            wall_clock_seconds=source.wall_clock_seconds,
        )
        persisted = self._journal.append(record)
        self._remove_branch(branch)
        self._branches_completed += 1
        return record if persisted else None

    def stop(self, *, at: datetime) -> tuple[ProtectedRunnerRecord, ...]:
        normalized = _utc(at)
        with self._lock:
            persisted: list[ProtectedRunnerRecord] = []
            for branch in tuple(self._branches.values()):
                record = self._record(
                    branch,
                    record_type=ProtectedRunnerRecordType.INTERRUPTED,
                    at=normalized,
                    price=branch.current_price,
                    stage=ProtectedRunnerStage.INTERRUPTED,
                    outcome=ProtectedRunnerOutcome.INCOMPLETE,
                    exit_reason=ProtectedRunnerExitReason.INTERRUPTED.value,
                )
                if self._journal.append(record):
                    persisted.append(record)
                self._remove_branch(branch)
            self._interrupted_branches += len(persisted)
            self._journal.flush()
            self._started = False
            return tuple(persisted)

    def protected_symbols(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._branches_by_symbol))

    def metrics(self) -> ProtectedRunnerRuntimeMetrics:
        with self._lock:
            return ProtectedRunnerRuntimeMetrics(
                active_branches=len(self._branches),
                protected_symbols=len(self._branches_by_symbol),
                branches_created=self._branches_created,
                branches_armed=self._branches_armed,
                branches_completed=self._branches_completed,
                capacity_rejections=self._capacity_rejections,
                duplicate_targets=self._duplicate_targets,
                interrupted_branches=self._interrupted_branches,
                retained_branch_ids=len(self._seen_branches),
                last_warning=self._last_warning,
            )

    def _new_branch(
        self,
        source: MicroProfitRecord,
        *,
        evidence: ContinuationEvidence,
        event: PublicTradeEvent,
    ) -> _ProtectedBranch:
        entry_at = source.entry_at
        if entry_at is None:
            raise ValueError("Protected runner source requires an observed entry.")
        elapsed = max(0.0, (event.exchange_at - entry_at).total_seconds())
        gross_r = _gross_r(source, event.price)
        costs = calculate_cost_breakdown(
            self._config.cost_model,
            entry_price=source.entry_price,
            exit_price=event.price,
            risk_per_unit=source.risk_per_unit,
            gross_r=gross_r,
            duration_seconds=elapsed,
            entry_spread_bps=evidence.spread_bps,
            exit_spread_bps=evidence.spread_bps,
            accrual=CostAccrual.ROUND_TRIP,
        )
        return _ProtectedBranch(
            source=source,
            evidence=evidence,
            branch_id=_branch_id(source),
            target_reached_price=event.price,
            current_price=event.price,
            last_event_at=event.exchange_at,
            last_event_id=event.trade_id,
            last_bucket=int(elapsed),
            completed_bars=source.completed_bars,
            maximum_net_r=costs.net_r,
            maximum_gross_r=max(gross_r, 0.0),
        )

    def _observe_event(
        self,
        branch: _ProtectedBranch,
        event: PublicTradeEvent,
    ) -> None:
        entry_at = branch.source.entry_at
        if entry_at is None:
            raise ValueError("Protected runner source requires an observed entry.")
        elapsed = max(
            0.0,
            (event.exchange_at - entry_at).total_seconds(),
        )
        bucket = int(elapsed)
        if bucket > branch.last_bucket:
            branch.completed_bars += 1
            branch.last_bucket = bucket
        branch.current_price = event.price
        branch.last_event_at = event.exchange_at
        branch.last_event_id = event.trade_id

    def _costs(
        self,
        branch: _ProtectedBranch,
        *,
        at: datetime,
        price: float,
    ) -> CostBreakdown:
        entry_at = branch.source.entry_at
        if entry_at is None:
            raise ValueError("Protected runner source requires an observed entry.")
        duration = max(0.0, (at - entry_at).total_seconds())
        return calculate_cost_breakdown(
            self._config.cost_model,
            entry_price=branch.source.entry_price,
            exit_price=price,
            risk_per_unit=branch.source.risk_per_unit,
            gross_r=_gross_r(branch.source, price),
            duration_seconds=duration,
            entry_spread_bps=branch.evidence.spread_bps,
            exit_spread_bps=branch.evidence.spread_bps,
            accrual=CostAccrual.ROUND_TRIP,
        )

    def _arm(
        self,
        branch: _ProtectedBranch,
        snapshot: ProtectedRunnerRecord,
    ) -> list[ProtectedRunnerRecord]:
        branch.armed_at = snapshot.recorded_at
        branch.net_r_at_floor_arm = snapshot.estimated_net_r_before_exit
        self._branches_armed += 1
        record = self._record(
            branch,
            record_type=ProtectedRunnerRecordType.NET_FLOOR_ARMED,
            at=snapshot.recorded_at,
            price=snapshot.current_price,
        )
        return [record] if self._journal.append(record) else []

    def _exit(
        self,
        branch: _ProtectedBranch,
        *,
        at: datetime,
        price: float,
        reason: ProtectedRunnerExitReason,
    ) -> ProtectedRunnerRecord:
        costs = self._costs(branch, at=at, price=price)
        breach = (
            max(0.0, self._config.protected_min_net_r - costs.net_r)
            if reason is ProtectedRunnerExitReason.NET_PROFIT_FLOOR
            else None
        )
        return self._record(
            branch,
            record_type=ProtectedRunnerRecordType.PROTECTED_RUNNER_EXITED,
            at=at,
            price=price,
            stage=ProtectedRunnerStage.CLOSED,
            outcome=ProtectedRunnerOutcome.COMPLETE,
            exit_reason=reason.value,
            actual_exit_price=price,
            actual_gross_r=costs.gross_r,
            actual_total_cost_r=costs.total_cost_r,
            actual_net_r=costs.net_r,
            floor_breach_amount_r=breach,
        )

    def _record(
        self,
        branch: _ProtectedBranch,
        *,
        record_type: ProtectedRunnerRecordType,
        at: datetime,
        price: float,
        stage: ProtectedRunnerStage = ProtectedRunnerStage.ACTIVE,
        outcome: ProtectedRunnerOutcome = ProtectedRunnerOutcome.PENDING,
        exit_reason: str | None = None,
        actual_exit_price: float | None = None,
        actual_gross_r: float | None = None,
        actual_total_cost_r: float | None = None,
        actual_net_r: float | None = None,
        floor_breach_amount_r: float | None = None,
        estimated_net_r_before_exit: float | None = None,
        completed_bars: int | None = None,
        wall_clock_seconds: float | None = None,
    ) -> ProtectedRunnerRecord:
        source = branch.source
        entry_at = source.entry_at
        if entry_at is None:
            raise ValueError("Protected runner source requires an observed entry.")
        costs = self._costs(branch, at=at, price=price)
        return ProtectedRunnerRecord(
            recorded_at=at,
            record_type=record_type,
            branch_id=branch.branch_id,
            experiment_group_id=source.experiment_group_id,
            baseline_trade_id=source.baseline_trade_id,
            source_variant_id=source.variant_id,
            symbol=source.symbol,
            direction=source.direction,
            target=source.target,
            score=source.score,
            setup_state=source.setup_state,
            market_state=source.market_state,
            entry_at=entry_at,
            entry_price=source.entry_price,
            initial_stop=source.initial_stop,
            risk_per_unit=source.risk_per_unit,
            target_reached_at=source.first_reached_at or source.recorded_at,
            target_reached_price=branch.target_reached_price,
            stage=stage,
            outcome=outcome,
            requested_net_floor_r=self._config.protected_min_net_r,
            estimated_net_r_before_exit=(
                costs.net_r
                if estimated_net_r_before_exit is None
                else estimated_net_r_before_exit
            ),
            current_price=price,
            actual_exit_price=actual_exit_price,
            actual_gross_r=actual_gross_r,
            actual_total_cost_r=actual_total_cost_r,
            actual_net_r=actual_net_r,
            floor_breach_amount_r=floor_breach_amount_r,
            floor_armed_at=branch.armed_at,
            net_r_at_floor_arm=branch.net_r_at_floor_arm,
            maximum_net_r_observed=max(branch.maximum_net_r, costs.net_r),
            maximum_excursion_after_target_r=branch.maximum_gross_r,
            wall_clock_seconds=(
                max(0.0, (at - entry_at).total_seconds())
                if wall_clock_seconds is None
                else wall_clock_seconds
            ),
            completed_bars=(
                branch.completed_bars if completed_bars is None else completed_bars
            ),
            exit_reason=exit_reason,
        )

    def _remove_branch(self, branch: _ProtectedBranch) -> None:
        self._branches.pop(branch.branch_id, None)
        self._branches_by_source.pop(_source_key(branch.source), None)
        symbol_branches = self._branches_by_symbol.get(branch.source.symbol)
        if symbol_branches is None:
            return
        symbol_branches.discard(branch.branch_id)
        if not symbol_branches:
            self._branches_by_symbol.pop(branch.source.symbol, None)

    def _remember_branch(self, branch_id: str) -> None:
        self._seen_branches[branch_id] = None
        if len(self._seen_branches) > _RECENT_ID_CAPACITY:
            del self._seen_branches[next(iter(self._seen_branches))]


def iter_protected_runner_records(path: Path) -> Iterator[ProtectedRunnerRecord]:
    if not path.exists():
        return
    with path.open("rb") as stream:
        for raw_line in stream:
            if not raw_line.strip() or not raw_line.endswith(b"\n"):
                continue
            try:
                yield deserialize_protected_runner_record(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                continue


def serialize_protected_runner_record(record: ProtectedRunnerRecord) -> str:
    return json.dumps(
        _record_payload(record, include_record_id=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def deserialize_protected_runner_record(line: str) -> ProtectedRunnerRecord:
    try:
        loaded = cast(object, json.loads(line))
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Protected runner line is not valid JSON.") from exc
    if not isinstance(loaded, dict):
        raise ValueError("Protected runner line must contain an object.")
    payload = cast(dict[str, object], loaded)
    try:
        expected_id = _text(payload, "record_id")
        record = ProtectedRunnerRecord(
            recorded_at=_datetime(payload, "recorded_at"),
            record_type=ProtectedRunnerRecordType(_text(payload, "record_type")),
            branch_id=_text(payload, "branch_id"),
            experiment_group_id=_text(payload, "experiment_group_id"),
            baseline_trade_id=_text(payload, "baseline_trade_id"),
            source_variant_id=_text(payload, "source_variant_id"),
            symbol=_text(payload, "symbol"),
            direction=ShadowDirection(_text(payload, "direction")),
            target=MicroTarget(_text(payload, "target")),
            score=_number(payload, "score"),
            setup_state=_text(payload, "setup_state"),
            market_state=_text(payload, "market_state"),
            entry_at=_datetime(payload, "entry_at"),
            entry_price=_number(payload, "entry_price"),
            initial_stop=_number(payload, "initial_stop"),
            risk_per_unit=_number(payload, "risk_per_unit"),
            target_reached_at=_datetime(payload, "target_reached_at"),
            target_reached_price=_number(payload, "target_reached_price"),
            stage=ProtectedRunnerStage(_text(payload, "stage")),
            outcome=ProtectedRunnerOutcome(_text(payload, "outcome")),
            requested_net_floor_r=_number(payload, "requested_net_floor_r"),
            estimated_net_r_before_exit=_number(
                payload, "estimated_net_r_before_exit"
            ),
            current_price=_number(payload, "current_price"),
            actual_exit_price=_optional_number(payload, "actual_exit_price"),
            actual_gross_r=_optional_number(payload, "actual_gross_r"),
            actual_total_cost_r=_optional_number(payload, "actual_total_cost_r"),
            actual_net_r=_optional_number(payload, "actual_net_r"),
            floor_breach_amount_r=_optional_number(
                payload, "floor_breach_amount_r"
            ),
            floor_armed_at=_optional_datetime(payload, "floor_armed_at"),
            net_r_at_floor_arm=_optional_number(payload, "net_r_at_floor_arm"),
            maximum_net_r_observed=_number(payload, "maximum_net_r_observed"),
            maximum_excursion_after_target_r=_number(
                payload, "maximum_excursion_after_target_r"
            ),
            wall_clock_seconds=_number(payload, "wall_clock_seconds"),
            completed_bars=_integer(payload, "completed_bars"),
            exit_reason=_optional_text(payload, "exit_reason"),
            schema_version=_integer(payload, "schema_version"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Protected runner line has an invalid schema.") from exc
    if record.record_id != expected_id:
        raise ValueError("Protected runner record id does not match payload.")
    return record


def _record_id(record: ProtectedRunnerRecord) -> str:
    canonical = json.dumps(
        _record_payload(record, include_record_id=False),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _record_payload(
    record: ProtectedRunnerRecord,
    *,
    include_record_id: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "recorded_at": record.recorded_at.isoformat(),
        "record_type": record.record_type.value,
        "branch_id": record.branch_id,
        "experiment_group_id": record.experiment_group_id,
        "baseline_trade_id": record.baseline_trade_id,
        "source_variant_id": record.source_variant_id,
        "symbol": record.symbol,
        "direction": record.direction.value,
        "target": record.target.value,
        "score": record.score,
        "setup_state": record.setup_state,
        "market_state": record.market_state,
        "entry_at": record.entry_at.isoformat(),
        "entry_price": record.entry_price,
        "initial_stop": record.initial_stop,
        "risk_per_unit": record.risk_per_unit,
        "target_reached_at": record.target_reached_at.isoformat(),
        "target_reached_price": record.target_reached_price,
        "stage": record.stage.value,
        "outcome": record.outcome.value,
        "requested_net_floor_r": record.requested_net_floor_r,
        "estimated_net_r_before_exit": record.estimated_net_r_before_exit,
        "current_price": record.current_price,
        "actual_exit_price": record.actual_exit_price,
        "actual_gross_r": record.actual_gross_r,
        "actual_total_cost_r": record.actual_total_cost_r,
        "actual_net_r": record.actual_net_r,
        "floor_breach_amount_r": record.floor_breach_amount_r,
        "floor_armed_at": (
            record.floor_armed_at.isoformat() if record.floor_armed_at else None
        ),
        "net_r_at_floor_arm": record.net_r_at_floor_arm,
        "maximum_net_r_observed": record.maximum_net_r_observed,
        "maximum_excursion_after_target_r": (
            record.maximum_excursion_after_target_r
        ),
        "wall_clock_seconds": record.wall_clock_seconds,
        "completed_bars": record.completed_bars,
        "exit_reason": record.exit_reason,
        "schema_version": record.schema_version,
    }
    if include_record_id:
        payload["record_id"] = record.record_id
    return payload


def _branch_id(source: MicroProfitRecord) -> str:
    digest = hashlib.sha256(
        (
            f"protected-runner|{source.baseline_trade_id}|{source.target.value}|"
            f"{source.variant_id}|{source.recorded_at.isoformat()}"
        ).encode()
    ).hexdigest()[:24]
    return f"protected-{digest}"


def _source_key(source: MicroProfitRecord) -> tuple[str, MicroTarget, str]:
    return source.baseline_trade_id, source.target, source.variant_id


def _gross_r(source: MicroProfitRecord, price: float) -> float:
    direction = 1.0 if source.direction is ShadowDirection.LONG else -1.0
    return (price - source.entry_price) / source.risk_per_unit * direction


def _newer_event(branch: _ProtectedBranch, event: PublicTradeEvent) -> bool:
    return event.exchange_at > branch.last_event_at or (
        event.exchange_at == branch.last_event_at
        and event.trade_id != branch.last_event_id
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Protected runner timestamps must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return math.isfinite(value)


def _positive(value: float) -> bool:
    return _finite(value) and value > 0


def _environment_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


def _environment_int(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer.") from exc


def _environment_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric.") from exc


def _text(payload: dict[str, object], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value:
        raise ValueError(f"Protected runner {key} must be text.")
    return value


def _optional_text(payload: dict[str, object], key: str) -> str | None:
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"Protected runner {key} must be text or null.")
    return value


def _number(payload: dict[str, object], key: str) -> float:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Protected runner {key} must be numeric.")
    return float(value)


def _optional_number(payload: dict[str, object], key: str) -> float | None:
    value = payload[key]
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Protected runner {key} must be numeric or null.")
    return float(value)


def _integer(payload: dict[str, object], key: str) -> int:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Protected runner {key} must be an integer.")
    return value


def _datetime(payload: dict[str, object], key: str) -> datetime:
    return _utc(datetime.fromisoformat(_text(payload, key)))


def _optional_datetime(
    payload: dict[str, object],
    key: str,
) -> datetime | None:
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Protected runner {key} must be datetime text or null.")
    return _utc(datetime.fromisoformat(value))
