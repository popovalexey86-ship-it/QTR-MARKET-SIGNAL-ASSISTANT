from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import cast

from market_signal_assistant.qtr_micro_scalper.data.liquidity import PressureDirection
from market_signal_assistant.qtr_micro_scalper.data.market_state import MarketBias
from market_signal_assistant.qtr_micro_scalper.data.models import PublicTradeEvent
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowAnalysisInput
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    ShadowDirection,
    ShadowOpportunityDecision,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowTrade,
    ShadowTradeStage,
)

DEFAULT_MICRO_PROFIT_EXPERIMENT_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_micro_scalper_micro_profit_experiment.jsonl"
)
_SCHEMA_VERSION = 1
_RECENT_ID_CAPACITY = 100_000
_RECOVERED_ACTIVE_CAPACITY = 5_000
_MAX_RECOVERY_WARNINGS = 1_000
_SECONDS_PER_FUNDING_INTERVAL = 8 * 60 * 60


class MicroTarget(StrEnum):
    M05 = "M05"
    M10 = "M10"
    M15 = "M15"
    M20 = "M20"
    M25 = "M25"

    @property
    def target_r(self) -> float:
        return {
            MicroTarget.M05: 0.05,
            MicroTarget.M10: 0.10,
            MicroTarget.M15: 0.15,
            MicroTarget.M20: 0.20,
            MicroTarget.M25: 0.25,
        }[self]


class CostScenario(StrEnum):
    TAKER_TAKER = "taker_taker"
    MAKER_TAKER = "maker_taker"
    MAKER_MAKER = "maker_maker"
    CUSTOM = "custom"


class MicroExperimentStage(StrEnum):
    WAITING_ENTRY = "WAITING_ENTRY"
    OPEN = "OPEN"
    RUNNER_ACTIVE = "RUNNER_ACTIVE"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    INTERRUPTED = "INTERRUPTED"


class MicroExperimentOutcome(StrEnum):
    PENDING = "PENDING"
    TARGET_REACHED = "TARGET_REACHED"
    TARGET_MISSED = "TARGET_MISSED"
    NOT_TRIGGERED = "NOT_TRIGGERED"
    INCOMPLETE = "INCOMPLETE"


class MicroExperimentRecordType(StrEnum):
    CREATED = "CREATED"
    ENTRY_OPENED = "ENTRY_OPENED"
    TARGET_REACHED = "TARGET_REACHED"
    TARGET_CLOSED = "TARGET_CLOSED"
    RUNNER_EXITED = "RUNNER_EXITED"
    BASELINE_CLOSED = "BASELINE_CLOSED"
    EXPIRED = "EXPIRED"
    INTERRUPTED = "INTERRUPTED"


class ContinuationExitReason(StrEnum):
    DIRECTIONAL_EVIDENCE_LOST = "DIRECTIONAL_EVIDENCE_LOST"
    OPPOSITE_MARKET_STATE = "OPPOSITE_MARKET_STATE"
    STRUCTURAL_INVALIDATION = "STRUCTURAL_INVALIDATION"
    TRAILING_EXCURSION = "TRAILING_EXCURSION"
    MAXIMUM_SAFETY_HORIZON = "MAXIMUM_SAFETY_HORIZON"
    STOP = "STOP"
    TIME_EXIT = "TIME_EXIT"
    EXPIRED = "EXPIRED"
    INTERRUPTED = "INTERRUPTED"


@dataclass(frozen=True, slots=True)
class MicroCostModelConfig:
    """Configurable one-unit round-trip model; rates are decimal fractions."""

    enabled: bool = True
    scenario: CostScenario = CostScenario.TAKER_TAKER
    taker_fee_rate: float = 0.00055
    maker_fee_rate: float = 0.00020
    custom_entry_fee_rate: float | None = None
    custom_exit_fee_rate: float | None = None
    slippage_bps: float = 0.0
    funding_rate_8h: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("Cost model enabled flag must be boolean.")
        for name, value in (
            ("taker fee rate", self.taker_fee_rate),
            ("maker fee rate", self.maker_fee_rate),
            ("slippage bps", self.slippage_bps),
            ("funding rate", self.funding_rate_8h),
        ):
            if not _finite(value) or value < 0:
                raise ValueError(f"Cost model {name} cannot be negative.")
        for name, optional_value in (
            ("custom entry fee rate", self.custom_entry_fee_rate),
            ("custom exit fee rate", self.custom_exit_fee_rate),
        ):
            if optional_value is not None and (
                not _finite(optional_value) or optional_value < 0
            ):
                raise ValueError(f"Cost model {name} cannot be negative.")
        if self.scenario is CostScenario.CUSTOM and (
            self.custom_entry_fee_rate is None
            or self.custom_exit_fee_rate is None
        ):
            raise ValueError("Custom cost scenario requires both fee rates.")

    @property
    def entry_is_taker(self) -> bool:
        return self.scenario in {
            CostScenario.TAKER_TAKER,
            CostScenario.CUSTOM,
        }

    @property
    def exit_is_taker(self) -> bool:
        return self.scenario in {
            CostScenario.TAKER_TAKER,
            CostScenario.MAKER_TAKER,
            CostScenario.CUSTOM,
        }

    @property
    def entry_fee_rate(self) -> float:
        if self.scenario is CostScenario.CUSTOM:
            assert self.custom_entry_fee_rate is not None
            return self.custom_entry_fee_rate
        return self.taker_fee_rate if self.entry_is_taker else self.maker_fee_rate

    @property
    def exit_fee_rate(self) -> float:
        if self.scenario is CostScenario.CUSTOM:
            assert self.custom_exit_fee_rate is not None
            return self.custom_exit_fee_rate
        return self.taker_fee_rate if self.exit_is_taker else self.maker_fee_rate

    @classmethod
    def from_environment(cls) -> MicroCostModelConfig:
        scenario = CostScenario(
            os.getenv("QTR_SCALPER_V2_COST_SCENARIO", "taker_taker")
            .strip()
            .lower()
        )
        custom_entry = _optional_environment_float(
            "QTR_SCALPER_V2_COST_ENTRY_FEE_RATE"
        )
        custom_exit = _optional_environment_float(
            "QTR_SCALPER_V2_COST_EXIT_FEE_RATE"
        )
        if scenario is not CostScenario.CUSTOM:
            default_entry = (
                0.00055
                if scenario is CostScenario.TAKER_TAKER
                else 0.00020
            )
            default_exit = (
                0.00020
                if scenario is CostScenario.MAKER_MAKER
                else 0.00055
            )
            if custom_entry is not None or custom_exit is not None:
                scenario = CostScenario.CUSTOM
                custom_entry = default_entry if custom_entry is None else custom_entry
                custom_exit = default_exit if custom_exit is None else custom_exit
        return cls(
            enabled=_environment_bool(
                "QTR_SCALPER_V2_COST_MODEL_ENABLED",
                default=True,
            ),
            scenario=scenario,
            taker_fee_rate=_environment_float(
                "QTR_SCALPER_V2_COST_TAKER_FEE_RATE",
                default=0.00055,
            ),
            maker_fee_rate=_environment_float(
                "QTR_SCALPER_V2_COST_MAKER_FEE_RATE",
                default=0.00020,
            ),
            custom_entry_fee_rate=custom_entry,
            custom_exit_fee_rate=custom_exit,
            slippage_bps=_environment_float(
                "QTR_SCALPER_V2_COST_SLIPPAGE_BPS",
                default=0.0,
            ),
            funding_rate_8h=_environment_float(
                "QTR_SCALPER_V2_COST_FUNDING_RATE_8H",
                default=0.0,
            ),
        )


@dataclass(frozen=True, slots=True)
class MicroProfitExperimentConfig:
    enabled: bool = False
    maximum_active_groups: int = 1_000
    runner_trailing_r: float = 0.10
    maximum_safety_bars: int = 300
    cost_model: MicroCostModelConfig = field(default_factory=MicroCostModelConfig)

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("Micro profit experiment enabled flag must be boolean.")
        if (
            isinstance(self.maximum_active_groups, bool)
            or not 1 <= self.maximum_active_groups <= 10_000
        ):
            raise ValueError("Micro experiment group capacity must be within 1..10000.")
        if not _positive(self.runner_trailing_r):
            raise ValueError("Runner trailing excursion must be positive.")
        if (
            isinstance(self.maximum_safety_bars, bool)
            or self.maximum_safety_bars < 1
        ):
            raise ValueError("Runner maximum safety bars must be positive.")

    @classmethod
    def from_environment(cls) -> MicroProfitExperimentConfig:
        return cls(
            enabled=_environment_bool(
                "QTR_SCALPER_V2_MICRO_PROFIT_EXPERIMENT_ENABLED",
                default=False,
            ),
            maximum_active_groups=_environment_int(
                "QTR_SCALPER_V2_MICRO_PROFIT_MAX_ACTIVE_GROUPS",
                default=1_000,
            ),
            runner_trailing_r=_environment_float(
                "QTR_SCALPER_V2_RUNNER_TRAILING_R",
                default=0.10,
            ),
            maximum_safety_bars=_environment_int(
                "QTR_SCALPER_V2_RUNNER_MAXIMUM_SAFETY_BARS",
                default=300,
            ),
            cost_model=MicroCostModelConfig.from_environment(),
        )


@dataclass(frozen=True, slots=True)
class CostBreakdown:
    gross_r: float
    entry_fee: float
    exit_fee: float
    spread_cost: float
    slippage_cost: float
    funding_cost: float
    total_cost: float
    total_cost_r: float
    cost_floor_r: float
    net_r: float

    def __post_init__(self) -> None:
        for name, value in (
            ("gross_r", self.gross_r),
            ("entry_fee", self.entry_fee),
            ("exit_fee", self.exit_fee),
            ("spread_cost", self.spread_cost),
            ("slippage_cost", self.slippage_cost),
            ("funding_cost", self.funding_cost),
            ("total_cost", self.total_cost),
            ("total_cost_r", self.total_cost_r),
            ("cost_floor_r", self.cost_floor_r),
            ("net_r", self.net_r),
        ):
            if not _finite(value):
                raise ValueError(f"Cost breakdown {name} must be finite.")
        if min(
            self.entry_fee,
            self.exit_fee,
            self.spread_cost,
            self.slippage_cost,
            self.funding_cost,
            self.total_cost,
            self.total_cost_r,
            self.cost_floor_r,
        ) < 0:
            raise ValueError("Cost breakdown costs cannot be negative.")
        for name in (
            "gross_r",
            "entry_fee",
            "exit_fee",
            "spread_cost",
            "slippage_cost",
            "funding_cost",
            "total_cost",
            "total_cost_r",
            "cost_floor_r",
            "net_r",
        ):
            object.__setattr__(self, name, float(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class ContinuationEvidence:
    symbol: str
    observed_at: datetime
    direction: ShadowDirection
    market_price: float
    invalidation_price: float
    setup_state: str
    setup_confidence: float
    market_state: str
    market_bias: MarketBias
    delta_5s: float
    orderbook_imbalance: float | None
    liquidity_pressure: PressureDirection
    spread_bps: float

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Continuation evidence symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "observed_at", _utc(self.observed_at))
        for name, value in (
            ("market price", self.market_price),
            ("invalidation price", self.invalidation_price),
        ):
            if not _positive(value):
                raise ValueError(f"Continuation evidence {name} must be positive.")
        if not 0.0 <= self.setup_confidence <= 100.0:
            raise ValueError("Continuation confidence must be within 0..100.")
        if not _finite(self.delta_5s):
            raise ValueError("Continuation delta must be finite.")
        if self.orderbook_imbalance is not None and not (
            _finite(self.orderbook_imbalance)
            and -1.0 <= self.orderbook_imbalance <= 1.0
        ):
            raise ValueError("Continuation orderbook imbalance is invalid.")
        if not _finite(self.spread_bps) or self.spread_bps < 0:
            raise ValueError("Continuation spread cannot be negative.")

    @classmethod
    def from_analysis(cls, analysis: ShadowAnalysisInput) -> ContinuationEvidence:
        context = analysis.setup_context
        imbalance = analysis.orderbook.imbalance_l5
        spread = analysis.orderbook.spread_bps
        if spread is None:
            raise ValueError("Continuation evidence requires observed spread.")
        return cls(
            symbol=analysis.symbol,
            observed_at=analysis.generated_at,
            direction=context.direction,
            market_price=context.price_context.market_price,
            invalidation_price=context.price_context.invalidation_price,
            setup_state=context.decision.value,
            setup_confidence=context.confidence,
            market_state=analysis.market_state.state.value,
            market_bias=analysis.market_state.bias,
            delta_5s=analysis.trade_flow.delta_5s,
            orderbook_imbalance=imbalance,
            liquidity_pressure=analysis.liquidity.pressure.direction,
            spread_bps=spread,
        )


@dataclass(frozen=True, slots=True)
class MicroProfitRecord:
    record_id: str = field(init=False)
    recorded_at: datetime
    record_type: MicroExperimentRecordType
    experiment_group_id: str
    baseline_trade_id: str
    variant_id: str
    target: MicroTarget
    symbol: str
    direction: ShadowDirection
    score: float
    setup_state: str
    setup_confidence: float
    market_state: str
    planned_at: datetime
    entry_at: datetime | None
    stage: MicroExperimentStage
    outcome: MicroExperimentOutcome
    entry_price: float
    initial_stop: float
    risk_per_unit: float
    target_price: float
    current_price: float
    first_reached_at: datetime | None
    completed_bars: int
    wall_clock_seconds: float | None
    maximum_excursion_before_r: float
    maximum_excursion_after_r: float
    gross_price_move_pct: float
    costs: CostBreakdown
    runner_exit_reason: str | None = None
    baseline_gross_r: float | None = None
    baseline_net_r: float | None = None
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in (
            "experiment_group_id",
            "baseline_trade_id",
            "variant_id",
            "setup_state",
            "market_state",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"Micro profit {name} cannot be empty.")
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Micro profit symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "recorded_at", _utc(self.recorded_at))
        object.__setattr__(self, "planned_at", _utc(self.planned_at))
        for name in ("entry_at", "first_reached_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value))
        for name, value in (
            ("entry price", self.entry_price),
            ("initial stop", self.initial_stop),
            ("risk per unit", self.risk_per_unit),
            ("target price", self.target_price),
            ("current price", self.current_price),
        ):
            if not _positive(value):
                raise ValueError(f"Micro profit {name} must be positive.")
        for name, value in (
            ("score", self.score),
            ("setup confidence", self.setup_confidence),
        ):
            if not _finite(value) or not 0.0 <= value <= 100.0:
                raise ValueError(f"Micro profit {name} must be within 0..100.")
        for name in (
            "score",
            "setup_confidence",
            "entry_price",
            "initial_stop",
            "risk_per_unit",
            "target_price",
            "current_price",
            "maximum_excursion_before_r",
            "maximum_excursion_after_r",
            "gross_price_move_pct",
        ):
            object.__setattr__(self, name, float(getattr(self, name)))
        if isinstance(self.completed_bars, bool) or self.completed_bars < 0:
            raise ValueError("Micro profit completed bars cannot be negative.")
        if self.wall_clock_seconds is not None and (
            not _finite(self.wall_clock_seconds) or self.wall_clock_seconds < 0
        ):
            raise ValueError("Micro profit wall clock is invalid.")
        for name, value in (
            ("maximum excursion before", self.maximum_excursion_before_r),
            ("maximum excursion after", self.maximum_excursion_after_r),
        ):
            if not _finite(value) or value < 0:
                raise ValueError(f"Micro profit {name} cannot be negative.")
        if not _finite(self.gross_price_move_pct):
            raise ValueError("Micro profit gross price move must be finite.")
        for name, value in (
            ("baseline gross R", self.baseline_gross_r),
            ("baseline net R", self.baseline_net_r),
        ):
            if value is not None and not _finite(value):
                raise ValueError(f"Micro profit {name} must be finite.")
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("Micro profit schema version is unsupported.")
        object.__setattr__(self, "record_id", _record_id(self))

    @property
    def terminal(self) -> bool:
        return self.stage in {
            MicroExperimentStage.CLOSED,
            MicroExperimentStage.EXPIRED,
            MicroExperimentStage.INTERRUPTED,
        }


@dataclass(frozen=True, slots=True)
class MicroProfitRecovery:
    active_records: tuple[MicroProfitRecord, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MicroProfitJournalMetrics:
    bootstrap_scans: int
    bytes_read: int
    records_processed: int
    recent_record_ids: int
    malformed_lines: int
    active_recovered_variants: int


class MicroProfitJournal:
    """Separate append-only journal with streaming and bounded recovery."""

    def __init__(
        self,
        path: Path = DEFAULT_MICRO_PROFIT_EXPERIMENT_JOURNAL_PATH,
        *,
        maximum_recent_record_ids: int = _RECENT_ID_CAPACITY,
        maximum_recovered_active_variants: int = _RECOVERED_ACTIVE_CAPACITY,
    ) -> None:
        if (
            isinstance(maximum_recent_record_ids, bool)
            or maximum_recent_record_ids < 1
        ):
            raise ValueError("Micro journal recent-ID capacity must be positive.")
        if (
            isinstance(maximum_recovered_active_variants, bool)
            or maximum_recovered_active_variants < 1
        ):
            raise ValueError("Micro journal active recovery capacity must be positive.")
        self._path = path.resolve()
        self._maximum_recent_record_ids = maximum_recent_record_ids
        self._maximum_recovered_active_variants = (
            maximum_recovered_active_variants
        )
        self._recent_record_ids: dict[str, None] = {}
        self._active: dict[str, MicroProfitRecord] = {}
        self._warnings: list[str] = []
        self._lock = Lock()
        self._bootstrap_scans = 0
        self._bytes_read = 0
        self._records_processed = 0
        self._malformed_lines = 0
        self._recover_streaming()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> MicroProfitRecovery:
        with self._lock:
            return MicroProfitRecovery(
                active_records=tuple(
                    sorted(self._active.values(), key=lambda item: item.variant_id)
                ),
                warnings=tuple(self._warnings),
            )

    @property
    def metrics(self) -> MicroProfitJournalMetrics:
        with self._lock:
            return MicroProfitJournalMetrics(
                bootstrap_scans=self._bootstrap_scans,
                bytes_read=self._bytes_read,
                records_processed=self._records_processed,
                recent_record_ids=len(self._recent_record_ids),
                malformed_lines=self._malformed_lines,
                active_recovered_variants=len(self._active),
            )

    def append(self, record: MicroProfitRecord) -> bool:
        encoded = serialize_micro_profit_record(record)
        with self._lock:
            if record.record_id in self._recent_record_ids:
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            needs_separator = self._needs_separator()
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                if needs_separator:
                    stream.write("\n")
                stream.write(encoded)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._remember(record.record_id)
            self._records_processed += 1
            self._track(record)
            return True

    def clear_recovered_active(self) -> None:
        with self._lock:
            self._active.clear()

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
                    record = deserialize_micro_profit_record(
                        raw_line.decode("utf-8")
                    )
                except (UnicodeDecodeError, ValueError) as exc:
                    self._warn(line_number, str(exc))
                    continue
                self._records_processed += 1
                self._remember(record.record_id)
                self._track(record)

    def _track(self, record: MicroProfitRecord) -> None:
        if record.terminal:
            self._active.pop(record.variant_id, None)
        elif record.record_type is not MicroExperimentRecordType.BASELINE_CLOSED:
            self._active[record.variant_id] = record
            if len(self._active) > self._maximum_recovered_active_variants:
                self._active.pop(next(iter(self._active)))

    def _remember(self, record_id: str) -> None:
        self._recent_record_ids[record_id] = None
        if len(self._recent_record_ids) > self._maximum_recent_record_ids:
            del self._recent_record_ids[next(iter(self._recent_record_ids))]

    def _warn(self, line_number: int, reason: str) -> None:
        self._malformed_lines += 1
        if len(self._warnings) < _MAX_RECOVERY_WARNINGS:
            self._warnings.append(
                f"Micro profit line {line_number} ignored: {reason}"
            )

    def _needs_separator(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with self._path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            return stream.read(1) != b"\n"


@dataclass(frozen=True, slots=True)
class MicroProfitRuntimeMetrics:
    active_groups: int
    active_variants: int
    protected_symbols: int
    groups_created: int
    variants_completed: int
    capacity_rejections: int
    interrupted_variants: int
    retained_group_ids: int
    last_warning: str | None


@dataclass(frozen=True, slots=True)
class MicroProfitActivation:
    accepted: bool
    experiment_group_id: str | None
    records: tuple[MicroProfitRecord, ...]
    reason: str


@dataclass(slots=True)
class _MicroVariant:
    target: MicroTarget
    stage: MicroExperimentStage = MicroExperimentStage.WAITING_ENTRY
    entry_at: datetime | None = None
    first_reached_at: datetime | None = None
    completed_bars: int = 0
    last_observed_bucket: int | None = None
    maximum_excursion_before_r: float = 0.0
    maximum_excursion_after_r: float = 0.0
    runner_peak_r: float = 0.0
    outcome: MicroExperimentOutcome = MicroExperimentOutcome.PENDING


@dataclass(slots=True)
class _MicroGroup:
    group_id: str
    baseline_trade: ShadowTrade
    score: float
    evidence: ContinuationEvidence
    variants: dict[MicroTarget, _MicroVariant]
    baseline_closed_recorded: bool = False
    last_price: float | None = None


class MicroProfitExperimentRuntime:
    """Shadow-only micro target and continuation evaluation."""

    def __init__(
        self,
        journal: MicroProfitJournal,
        config: MicroProfitExperimentConfig | None = None,
    ) -> None:
        self._journal = journal
        self._config = config or MicroProfitExperimentConfig()
        self._groups: dict[str, _MicroGroup] = {}
        self._groups_by_symbol: dict[str, set[str]] = {}
        self._groups_by_baseline: dict[str, str] = {}
        self._seen_groups: dict[str, None] = {}
        self._groups_created = 0
        self._variants_completed = 0
        self._capacity_rejections = 0
        self._interrupted_variants = 0
        self._last_warning: str | None = None
        self._started = False
        self._lock = Lock()

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    @property
    def journal(self) -> MicroProfitJournal:
        return self._journal

    def start(self, *, at: datetime) -> tuple[MicroProfitRecord, ...]:
        normalized = _utc(at)
        with self._lock:
            if self._started or not self.enabled:
                return ()
            interrupted = tuple(
                _interrupted_record(record, interrupted_at=normalized)
                for record in self._journal.recovery.active_records
            )
            persisted = tuple(
                record for record in interrupted if self._journal.append(record)
            )
            self._journal.clear_recovered_active()
            self._interrupted_variants += len(persisted)
            self._started = True
            return persisted

    def activate(
        self,
        baseline_trade: ShadowTrade,
        evidence: ContinuationEvidence,
        *,
        score: float,
    ) -> MicroProfitActivation:
        if not self.enabled:
            return MicroProfitActivation(False, None, (), "Micro experiment disabled.")
        if baseline_trade.symbol != evidence.symbol:
            raise ValueError("Micro baseline and evidence symbols differ.")
        if baseline_trade.direction is not evidence.direction:
            raise ValueError("Micro baseline and evidence directions differ.")
        group_id = _group_id(baseline_trade.trade_id)
        with self._lock:
            if group_id in self._groups or group_id in self._seen_groups:
                return MicroProfitActivation(
                    False,
                    group_id,
                    (),
                    "Duplicate micro experiment group suppressed.",
                )
            if len(self._groups) >= self._config.maximum_active_groups:
                self._capacity_rejections += 1
                self._last_warning = (
                    "Micro experiment capacity reached; baseline remains unaffected."
                )
                return MicroProfitActivation(
                    False,
                    group_id,
                    (),
                    self._last_warning,
                )
            group = _MicroGroup(
                group_id=group_id,
                baseline_trade=baseline_trade,
                score=score,
                evidence=evidence,
                variants={target: _MicroVariant(target) for target in MicroTarget},
            )
            self._groups[group_id] = group
            self._groups_by_baseline[baseline_trade.trade_id] = group_id
            self._groups_by_symbol.setdefault(baseline_trade.symbol, set()).add(
                group_id
            )
            self._remember_group(group_id)
            self._groups_created += 1
            records = tuple(
                self._record(
                    group,
                    variant,
                    MicroExperimentRecordType.CREATED,
                    at=baseline_trade.planned_at,
                    price=baseline_trade.entry_price,
                    gross_r=0.0,
                )
                for variant in group.variants.values()
            )
            for record in records:
                self._journal.append(record)
            return MicroProfitActivation(
                True,
                group_id,
                records,
                "M05/M10/M15/M20/M25 shadow experiment created.",
            )

    def sync_baseline(
        self,
        baseline_trade: ShadowTrade,
    ) -> tuple[MicroProfitRecord, ...]:
        if not self.enabled:
            return ()
        with self._lock:
            group_id = self._groups_by_baseline.get(baseline_trade.trade_id)
            group = self._groups.get(group_id) if group_id is not None else None
            if group is None:
                return ()
            group.baseline_trade = baseline_trade
            persisted: list[MicroProfitRecord] = []
            if baseline_trade.entry_at is not None:
                for variant in group.variants.values():
                    if variant.entry_at is None:
                        variant.entry_at = baseline_trade.entry_at
                        variant.stage = MicroExperimentStage.OPEN
                        variant.last_observed_bucket = 0
                        record = self._record(
                            group,
                            variant,
                            MicroExperimentRecordType.ENTRY_OPENED,
                            at=baseline_trade.entry_at,
                            price=baseline_trade.entry_price,
                            gross_r=0.0,
                        )
                        if self._journal.append(record):
                            persisted.append(record)
            if baseline_trade.stage is ShadowTradeStage.EXPIRED:
                persisted.extend(self._expire_group(group, baseline_trade.closed_at))
            elif baseline_trade.terminal and not group.baseline_closed_recorded:
                record = self._baseline_closed_record(group)
                if self._journal.append(record):
                    persisted.append(record)
                group.baseline_closed_recorded = True
            if not group.variants:
                self._remove_group(group)
            return tuple(persisted)

    def process_event(
        self,
        event: PublicTradeEvent,
    ) -> tuple[MicroProfitRecord, ...]:
        if not self.enabled:
            return ()
        with self._lock:
            persisted: list[MicroProfitRecord] = []
            for group_id in tuple(
                sorted(self._groups_by_symbol.get(event.symbol, ()))
            ):
                group = self._groups.get(group_id)
                if group is None:
                    continue
                if event.exchange_at <= group.baseline_trade.planned_at:
                    continue
                group.last_price = event.price
                persisted.extend(self._process_price(group, event))
                if not group.variants:
                    self._remove_group(group)
            return tuple(persisted)

    def update_evidence(
        self,
        evidence: ContinuationEvidence,
    ) -> tuple[MicroProfitRecord, ...]:
        if not self.enabled:
            return ()
        with self._lock:
            persisted: list[MicroProfitRecord] = []
            for group_id in tuple(
                sorted(self._groups_by_symbol.get(evidence.symbol, ()))
            ):
                group = self._groups.get(group_id)
                if group is None:
                    continue
                group.evidence = evidence
                reason = _continuation_exit(group.baseline_trade.direction, evidence)
                if reason is None:
                    continue
                for target, variant in tuple(group.variants.items()):
                    if variant.stage is not MicroExperimentStage.RUNNER_ACTIVE:
                        continue
                    persisted.extend(
                        self._close_runner(
                            group,
                            variant,
                            at=evidence.observed_at,
                            price=evidence.market_price,
                            reason=reason,
                        )
                    )
                    group.variants.pop(target, None)
                    self._variants_completed += 1
                if not group.variants:
                    self._remove_group(group)
            return tuple(persisted)

    def stop(self, *, at: datetime) -> tuple[MicroProfitRecord, ...]:
        normalized = _utc(at)
        with self._lock:
            persisted: list[MicroProfitRecord] = []
            for group in tuple(self._groups.values()):
                for variant in group.variants.values():
                    record = self._record(
                        group,
                        variant,
                        MicroExperimentRecordType.INTERRUPTED,
                        at=normalized,
                        price=group.last_price or group.baseline_trade.entry_price,
                        gross_r=_current_r(
                            group.baseline_trade,
                            group.last_price or group.baseline_trade.entry_price,
                        ),
                        stage=MicroExperimentStage.INTERRUPTED,
                        outcome=MicroExperimentOutcome.INCOMPLETE,
                        runner_exit_reason=ContinuationExitReason.INTERRUPTED.value,
                    )
                    if self._journal.append(record):
                        persisted.append(record)
                self._remove_group(group)
            self._interrupted_variants += len(persisted)
            self._journal.flush()
            self._started = False
            return tuple(persisted)

    def protected_symbols(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._groups_by_symbol))

    def metrics(self) -> MicroProfitRuntimeMetrics:
        with self._lock:
            return MicroProfitRuntimeMetrics(
                active_groups=len(self._groups),
                active_variants=sum(
                    len(group.variants) for group in self._groups.values()
                ),
                protected_symbols=len(self._groups_by_symbol),
                groups_created=self._groups_created,
                variants_completed=self._variants_completed,
                capacity_rejections=self._capacity_rejections,
                interrupted_variants=self._interrupted_variants,
                retained_group_ids=len(self._seen_groups),
                last_warning=self._last_warning,
            )

    def _process_price(
        self,
        group: _MicroGroup,
        event: PublicTradeEvent,
    ) -> list[MicroProfitRecord]:
        persisted: list[MicroProfitRecord] = []
        trade = group.baseline_trade
        for target, variant in tuple(group.variants.items()):
            if variant.entry_at is None:
                continue
            elapsed = max(0.0, (event.exchange_at - variant.entry_at).total_seconds())
            bucket = int(elapsed)
            if variant.last_observed_bucket is None:
                variant.last_observed_bucket = bucket
            elif bucket > variant.last_observed_bucket:
                variant.completed_bars += 1
                variant.last_observed_bucket = bucket
            current_r = _current_r(trade, event.price)
            if variant.first_reached_at is None:
                variant.maximum_excursion_before_r = max(
                    variant.maximum_excursion_before_r,
                    current_r,
                    0.0,
                )
            else:
                variant.maximum_excursion_after_r = max(
                    variant.maximum_excursion_after_r,
                    current_r,
                    0.0,
                )
                variant.runner_peak_r = max(variant.runner_peak_r, current_r)

            if current_r <= -1.0:
                record_type = (
                    MicroExperimentRecordType.RUNNER_EXITED
                    if variant.first_reached_at is not None
                    else MicroExperimentRecordType.TARGET_CLOSED
                )
                record = self._record(
                    group,
                    variant,
                    record_type,
                    at=event.exchange_at,
                    price=trade.initial_stop,
                    gross_r=-1.0,
                    stage=MicroExperimentStage.CLOSED,
                    outcome=(
                        MicroExperimentOutcome.TARGET_REACHED
                        if variant.first_reached_at is not None
                        else MicroExperimentOutcome.TARGET_MISSED
                    ),
                    runner_exit_reason=ContinuationExitReason.STOP.value,
                )
                if self._journal.append(record):
                    persisted.append(record)
                group.variants.pop(target, None)
                self._variants_completed += 1
                continue

            if variant.first_reached_at is None and current_r >= target.target_r:
                variant.first_reached_at = event.exchange_at
                variant.stage = MicroExperimentStage.RUNNER_ACTIVE
                variant.outcome = MicroExperimentOutcome.TARGET_REACHED
                variant.runner_peak_r = current_r
                target_price = _target_price(trade, target)
                record = self._record(
                    group,
                    variant,
                    MicroExperimentRecordType.TARGET_REACHED,
                    at=event.exchange_at,
                    price=target_price,
                    gross_r=target.target_r,
                )
                if self._journal.append(record):
                    persisted.append(record)

            if variant.stage is MicroExperimentStage.RUNNER_ACTIVE:
                trailing = variant.runner_peak_r - current_r
                if trailing >= self._config.runner_trailing_r:
                    persisted.extend(
                        self._close_runner(
                            group,
                            variant,
                            at=event.exchange_at,
                            price=event.price,
                            reason=ContinuationExitReason.TRAILING_EXCURSION,
                        )
                    )
                    group.variants.pop(target, None)
                    self._variants_completed += 1
                    continue

            if variant.completed_bars >= self._config.maximum_safety_bars:
                if variant.stage is MicroExperimentStage.RUNNER_ACTIVE:
                    persisted.extend(
                        self._close_runner(
                            group,
                            variant,
                            at=event.exchange_at,
                            price=event.price,
                            reason=ContinuationExitReason.MAXIMUM_SAFETY_HORIZON,
                        )
                    )
                else:
                    record = self._record(
                        group,
                        variant,
                        MicroExperimentRecordType.TARGET_CLOSED,
                        at=event.exchange_at,
                        price=event.price,
                        gross_r=current_r,
                        stage=MicroExperimentStage.CLOSED,
                        outcome=MicroExperimentOutcome.TARGET_MISSED,
                        runner_exit_reason=(
                            ContinuationExitReason.MAXIMUM_SAFETY_HORIZON.value
                        ),
                    )
                    if self._journal.append(record):
                        persisted.append(record)
                group.variants.pop(target, None)
                self._variants_completed += 1
        return persisted

    def _close_runner(
        self,
        group: _MicroGroup,
        variant: _MicroVariant,
        *,
        at: datetime,
        price: float,
        reason: ContinuationExitReason,
    ) -> list[MicroProfitRecord]:
        record = self._record(
            group,
            variant,
            MicroExperimentRecordType.RUNNER_EXITED,
            at=at,
            price=price,
            gross_r=_current_r(group.baseline_trade, price),
            stage=MicroExperimentStage.CLOSED,
            outcome=MicroExperimentOutcome.TARGET_REACHED,
            runner_exit_reason=reason.value,
        )
        return [record] if self._journal.append(record) else []

    def _expire_group(
        self,
        group: _MicroGroup,
        closed_at: datetime | None,
    ) -> list[MicroProfitRecord]:
        at = closed_at or group.baseline_trade.entry_expires_at
        persisted: list[MicroProfitRecord] = []
        for variant in group.variants.values():
            record = self._record(
                group,
                variant,
                MicroExperimentRecordType.EXPIRED,
                at=at,
                price=group.baseline_trade.entry_price,
                gross_r=0.0,
                stage=MicroExperimentStage.EXPIRED,
                outcome=MicroExperimentOutcome.NOT_TRIGGERED,
                runner_exit_reason=ContinuationExitReason.EXPIRED.value,
            )
            if self._journal.append(record):
                persisted.append(record)
            self._variants_completed += 1
        group.variants.clear()
        return persisted

    def _baseline_closed_record(self, group: _MicroGroup) -> MicroProfitRecord:
        trade = group.baseline_trade
        last_event = trade.events[-1] if trade.events else None
        exit_price = (
            last_event.price
            if last_event is not None and last_event.price is not None
            else group.last_price or trade.entry_price
        )
        gross_r = trade.realized_r
        variant = group.variants.get(MicroTarget.M05) or _MicroVariant(
            MicroTarget.M05,
            entry_at=trade.entry_at,
        )
        record = self._record(
            group,
            variant,
            MicroExperimentRecordType.BASELINE_CLOSED,
            at=trade.closed_at or trade.last_processed_at or trade.planned_at,
            price=exit_price,
            gross_r=gross_r,
            baseline_gross_r=gross_r,
        )
        return replace(
            record,
            baseline_net_r=record.costs.net_r,
        )

    def _record(
        self,
        group: _MicroGroup,
        variant: _MicroVariant,
        record_type: MicroExperimentRecordType,
        *,
        at: datetime,
        price: float,
        gross_r: float,
        stage: MicroExperimentStage | None = None,
        outcome: MicroExperimentOutcome | None = None,
        runner_exit_reason: str | None = None,
        baseline_gross_r: float | None = None,
    ) -> MicroProfitRecord:
        trade = group.baseline_trade
        duration = (
            max(0.0, (_utc(at) - variant.entry_at).total_seconds())
            if variant.entry_at is not None
            else 0.0
        )
        costs = calculate_cost_breakdown(
            self._config.cost_model,
            entry_price=trade.entry_price,
            exit_price=price,
            risk_per_unit=trade.risk_per_unit,
            gross_r=gross_r,
            duration_seconds=duration,
            entry_spread_bps=group.evidence.spread_bps,
            exit_spread_bps=group.evidence.spread_bps,
        )
        return MicroProfitRecord(
            recorded_at=at,
            record_type=record_type,
            experiment_group_id=group.group_id,
            baseline_trade_id=trade.trade_id,
            variant_id=f"{trade.trade_id}-micro-{variant.target.value.lower()}",
            target=variant.target,
            symbol=trade.symbol,
            direction=trade.direction,
            score=group.score,
            setup_state=group.evidence.setup_state,
            setup_confidence=group.evidence.setup_confidence,
            market_state=group.evidence.market_state,
            planned_at=trade.planned_at,
            entry_at=variant.entry_at,
            stage=stage or variant.stage,
            outcome=outcome or variant.outcome,
            entry_price=trade.entry_price,
            initial_stop=trade.initial_stop,
            risk_per_unit=trade.risk_per_unit,
            target_price=_target_price(trade, variant.target),
            current_price=price,
            first_reached_at=variant.first_reached_at,
            completed_bars=variant.completed_bars,
            wall_clock_seconds=(duration if variant.entry_at is not None else None),
            maximum_excursion_before_r=variant.maximum_excursion_before_r,
            maximum_excursion_after_r=variant.maximum_excursion_after_r,
            gross_price_move_pct=_gross_price_move_pct(trade, price),
            costs=costs,
            runner_exit_reason=runner_exit_reason,
            baseline_gross_r=baseline_gross_r,
            baseline_net_r=None,
        )

    def _remove_group(self, group: _MicroGroup) -> None:
        self._groups.pop(group.group_id, None)
        self._groups_by_baseline.pop(group.baseline_trade.trade_id, None)
        symbol_groups = self._groups_by_symbol.get(group.baseline_trade.symbol)
        if symbol_groups is not None:
            symbol_groups.discard(group.group_id)
            if not symbol_groups:
                self._groups_by_symbol.pop(group.baseline_trade.symbol, None)

    def _remember_group(self, group_id: str) -> None:
        self._seen_groups[group_id] = None
        if len(self._seen_groups) > _RECENT_ID_CAPACITY:
            del self._seen_groups[next(iter(self._seen_groups))]


def calculate_cost_breakdown(
    config: MicroCostModelConfig,
    *,
    entry_price: float,
    exit_price: float,
    risk_per_unit: float,
    gross_r: float,
    duration_seconds: float,
    entry_spread_bps: float,
    exit_spread_bps: float,
) -> CostBreakdown:
    """Convert observed/configured round-trip costs to each trade's actual R."""

    for name, value in (
        ("entry price", entry_price),
        ("exit price", exit_price),
        ("risk per unit", risk_per_unit),
    ):
        if not _positive(value):
            raise ValueError(f"Cost {name} must be positive.")
    for name, value in (
        ("duration", duration_seconds),
        ("entry spread", entry_spread_bps),
        ("exit spread", exit_spread_bps),
    ):
        if not _finite(value) or value < 0:
            raise ValueError(f"Cost {name} cannot be negative.")
    if not _finite(gross_r):
        raise ValueError("Cost gross R must be finite.")
    if not config.enabled:
        return CostBreakdown(
            gross_r=gross_r,
            entry_fee=0.0,
            exit_fee=0.0,
            spread_cost=0.0,
            slippage_cost=0.0,
            funding_cost=0.0,
            total_cost=0.0,
            total_cost_r=0.0,
            cost_floor_r=0.0,
            net_r=gross_r,
        )
    entry_fee = entry_price * config.entry_fee_rate
    exit_fee = exit_price * config.exit_fee_rate
    entry_spread = (
        entry_price * entry_spread_bps / 20_000
        if config.entry_is_taker
        else 0.0
    )
    exit_spread = (
        exit_price * exit_spread_bps / 20_000
        if config.exit_is_taker
        else 0.0
    )
    spread_cost = entry_spread + exit_spread
    slippage_cost = (
        (entry_price + exit_price) * config.slippage_bps / 10_000
    )
    funding_cost = (
        entry_price
        * config.funding_rate_8h
        * duration_seconds
        / _SECONDS_PER_FUNDING_INTERVAL
    )
    total = entry_fee + exit_fee + spread_cost + slippage_cost + funding_cost
    total_r = total / risk_per_unit
    floor_total = (
        entry_price * (config.entry_fee_rate + config.exit_fee_rate)
        + (
            entry_price * entry_spread_bps / 20_000
            if config.entry_is_taker
            else 0.0
        )
        + (
            entry_price * exit_spread_bps / 20_000
            if config.exit_is_taker
            else 0.0
        )
        + entry_price * 2 * config.slippage_bps / 10_000
    )
    return CostBreakdown(
        gross_r=gross_r,
        entry_fee=entry_fee,
        exit_fee=exit_fee,
        spread_cost=spread_cost,
        slippage_cost=slippage_cost,
        funding_cost=funding_cost,
        total_cost=total,
        total_cost_r=total_r,
        cost_floor_r=floor_total / risk_per_unit,
        net_r=gross_r - total_r,
    )


def iter_micro_profit_records(path: Path) -> Iterator[MicroProfitRecord]:
    if not path.exists():
        return
    with path.open("rb") as stream:
        for raw_line in stream:
            if not raw_line.strip() or not raw_line.endswith(b"\n"):
                continue
            try:
                yield deserialize_micro_profit_record(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                continue


def serialize_micro_profit_record(record: MicroProfitRecord) -> str:
    return json.dumps(
        _record_payload(record, include_record_id=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def deserialize_micro_profit_record(line: str) -> MicroProfitRecord:
    try:
        loaded = cast(object, json.loads(line))
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Micro profit line is not valid JSON.") from exc
    if not isinstance(loaded, dict):
        raise ValueError("Micro profit line must contain an object.")
    payload = cast(dict[str, object], loaded)
    try:
        expected_id = _text(payload, "record_id")
        cost_payload = _mapping(payload, "costs")
        record = MicroProfitRecord(
            recorded_at=_datetime(payload, "recorded_at"),
            record_type=MicroExperimentRecordType(_text(payload, "record_type")),
            experiment_group_id=_text(payload, "experiment_group_id"),
            baseline_trade_id=_text(payload, "baseline_trade_id"),
            variant_id=_text(payload, "variant_id"),
            target=MicroTarget(_text(payload, "target")),
            symbol=_text(payload, "symbol"),
            direction=ShadowDirection(_text(payload, "direction")),
            score=_number(payload, "score"),
            setup_state=_text(payload, "setup_state"),
            setup_confidence=_number(payload, "setup_confidence"),
            market_state=_text(payload, "market_state"),
            planned_at=_datetime(payload, "planned_at"),
            entry_at=_optional_datetime(payload, "entry_at"),
            stage=MicroExperimentStage(_text(payload, "stage")),
            outcome=MicroExperimentOutcome(_text(payload, "outcome")),
            entry_price=_number(payload, "entry_price"),
            initial_stop=_number(payload, "initial_stop"),
            risk_per_unit=_number(payload, "risk_per_unit"),
            target_price=_number(payload, "target_price"),
            current_price=_number(payload, "current_price"),
            first_reached_at=_optional_datetime(payload, "first_reached_at"),
            completed_bars=_integer(payload, "completed_bars"),
            wall_clock_seconds=_optional_number(payload, "wall_clock_seconds"),
            maximum_excursion_before_r=_number(
                payload,
                "maximum_excursion_before_r",
            ),
            maximum_excursion_after_r=_number(
                payload,
                "maximum_excursion_after_r",
            ),
            gross_price_move_pct=_number(payload, "gross_price_move_pct"),
            costs=CostBreakdown(
                gross_r=_number(cost_payload, "gross_r"),
                entry_fee=_number(cost_payload, "entry_fee"),
                exit_fee=_number(cost_payload, "exit_fee"),
                spread_cost=_number(cost_payload, "spread_cost"),
                slippage_cost=_number(cost_payload, "slippage_cost"),
                funding_cost=_number(cost_payload, "funding_cost"),
                total_cost=_number(cost_payload, "total_cost"),
                total_cost_r=_number(cost_payload, "total_cost_r"),
                cost_floor_r=_number(cost_payload, "cost_floor_r"),
                net_r=_number(cost_payload, "net_r"),
            ),
            runner_exit_reason=_optional_text(payload, "runner_exit_reason"),
            baseline_gross_r=_optional_number(payload, "baseline_gross_r"),
            baseline_net_r=_optional_number(payload, "baseline_net_r"),
            schema_version=_integer(payload, "schema_version"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Micro profit line has an invalid schema.") from exc
    if record.record_id != expected_id:
        raise ValueError("Micro profit record id does not match payload.")
    return record


def _continuation_exit(
    direction: ShadowDirection,
    evidence: ContinuationEvidence,
) -> ContinuationExitReason | None:
    if direction is ShadowDirection.LONG:
        if evidence.market_price <= evidence.invalidation_price:
            return ContinuationExitReason.STRUCTURAL_INVALIDATION
        if evidence.market_bias is MarketBias.BEARISH:
            return ContinuationExitReason.OPPOSITE_MARKET_STATE
        opposing_flow = evidence.delta_5s < 0
        opposing_book = (
            evidence.orderbook_imbalance is not None
            and evidence.orderbook_imbalance < -0.15
        )
        opposing_liquidity = evidence.liquidity_pressure is PressureDirection.SELL
    else:
        if evidence.market_price >= evidence.invalidation_price:
            return ContinuationExitReason.STRUCTURAL_INVALIDATION
        if evidence.market_bias is MarketBias.BULLISH:
            return ContinuationExitReason.OPPOSITE_MARKET_STATE
        opposing_flow = evidence.delta_5s > 0
        opposing_book = (
            evidence.orderbook_imbalance is not None
            and evidence.orderbook_imbalance > 0.15
        )
        opposing_liquidity = evidence.liquidity_pressure is PressureDirection.BUY
    if evidence.direction is not direction or evidence.setup_state in {
        ShadowOpportunityDecision.CONFLICTED.value,
        ShadowOpportunityDecision.BLOCKED.value,
    }:
        return ContinuationExitReason.DIRECTIONAL_EVIDENCE_LOST
    if sum((opposing_flow, opposing_book, opposing_liquidity)) >= 2:
        return ContinuationExitReason.DIRECTIONAL_EVIDENCE_LOST
    return None


def _target_price(trade: ShadowTrade, target: MicroTarget) -> float:
    direction = 1.0 if trade.direction is ShadowDirection.LONG else -1.0
    price = trade.entry_price + direction * trade.risk_per_unit * target.target_r
    if not _positive(price):
        raise ValueError("Micro target price must remain positive.")
    return price


def _current_r(trade: ShadowTrade, price: float) -> float:
    direction = 1.0 if trade.direction is ShadowDirection.LONG else -1.0
    return (price - trade.entry_price) / trade.risk_per_unit * direction


def _gross_price_move_pct(trade: ShadowTrade, price: float) -> float:
    direction = 1.0 if trade.direction is ShadowDirection.LONG else -1.0
    return (price - trade.entry_price) / trade.entry_price * direction * 100.0


def _group_id(baseline_trade_id: str) -> str:
    digest = hashlib.sha256(
        f"micro-profit|{baseline_trade_id}".encode()
    ).hexdigest()[:24]
    return f"micro-{digest}"


def _interrupted_record(
    record: MicroProfitRecord,
    *,
    interrupted_at: datetime,
) -> MicroProfitRecord:
    normalized = _utc(interrupted_at)
    duration = (
        max(0.0, (normalized - record.entry_at).total_seconds())
        if record.entry_at is not None
        else None
    )
    return replace(
        record,
        recorded_at=normalized,
        record_type=MicroExperimentRecordType.INTERRUPTED,
        stage=MicroExperimentStage.INTERRUPTED,
        outcome=MicroExperimentOutcome.INCOMPLETE,
        wall_clock_seconds=duration,
        runner_exit_reason=ContinuationExitReason.INTERRUPTED.value,
    )


def _record_id(record: MicroProfitRecord) -> str:
    canonical = json.dumps(
        _record_payload(record, include_record_id=False),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _record_payload(
    record: MicroProfitRecord,
    *,
    include_record_id: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "recorded_at": record.recorded_at.isoformat(),
        "record_type": record.record_type.value,
        "experiment_group_id": record.experiment_group_id,
        "baseline_trade_id": record.baseline_trade_id,
        "variant_id": record.variant_id,
        "target": record.target.value,
        "symbol": record.symbol,
        "direction": record.direction.value,
        "score": record.score,
        "setup_state": record.setup_state,
        "setup_confidence": record.setup_confidence,
        "market_state": record.market_state,
        "planned_at": record.planned_at.isoformat(),
        "entry_at": record.entry_at.isoformat() if record.entry_at else None,
        "stage": record.stage.value,
        "outcome": record.outcome.value,
        "entry_price": record.entry_price,
        "initial_stop": record.initial_stop,
        "risk_per_unit": record.risk_per_unit,
        "target_price": record.target_price,
        "current_price": record.current_price,
        "first_reached_at": (
            record.first_reached_at.isoformat() if record.first_reached_at else None
        ),
        "completed_bars": record.completed_bars,
        "wall_clock_seconds": record.wall_clock_seconds,
        "maximum_excursion_before_r": record.maximum_excursion_before_r,
        "maximum_excursion_after_r": record.maximum_excursion_after_r,
        "gross_price_move_pct": record.gross_price_move_pct,
        "costs": {
            "gross_r": record.costs.gross_r,
            "entry_fee": record.costs.entry_fee,
            "exit_fee": record.costs.exit_fee,
            "spread_cost": record.costs.spread_cost,
            "slippage_cost": record.costs.slippage_cost,
            "funding_cost": record.costs.funding_cost,
            "total_cost": record.costs.total_cost,
            "total_cost_r": record.costs.total_cost_r,
            "cost_floor_r": record.costs.cost_floor_r,
            "net_r": record.costs.net_r,
        },
        "runner_exit_reason": record.runner_exit_reason,
        "baseline_gross_r": record.baseline_gross_r,
        "baseline_net_r": record.baseline_net_r,
        "schema_version": record.schema_version,
    }
    if include_record_id:
        payload["record_id"] = record.record_id
    return payload


def _mapping(payload: dict[str, object], key: str) -> dict[str, object]:
    value = payload[key]
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return cast(dict[str, object], value)


def _text(payload: dict[str, object], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be non-empty text")
    return value


def _optional_text(payload: dict[str, object], key: str) -> str | None:
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be text or null")
    return value


def _number(payload: dict[str, object], key: str) -> float:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key} must be numeric")
    result = float(value)
    if not _finite(result):
        raise ValueError(f"{key} must be finite")
    return result


def _optional_number(payload: dict[str, object], key: str) -> float | None:
    value = payload[key]
    if value is None:
        return None
    return _number(payload, key)


def _integer(payload: dict[str, object], key: str) -> int:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _datetime(payload: dict[str, object], key: str) -> datetime:
    return _parse_datetime(_text(payload, key))


def _optional_datetime(payload: dict[str, object], key: str) -> datetime | None:
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a timestamp or null")
    return _parse_datetime(value)


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Micro profit timestamps must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return math.isfinite(value)


def _positive(value: float) -> bool:
    return _finite(value) and value > 0


def _environment_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


def _environment_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric.") from exc
    if not _finite(value) or value < 0:
        raise ValueError(f"{name} must be a non-negative finite number.")
    return value


def _optional_environment_float(name: str) -> float | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    return _environment_float(name, default=0.0)


def _environment_int(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer.") from exc
    if value < 1:
        raise ValueError(f"{name} must be positive.")
    return value
