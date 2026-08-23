from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import cast

from market_signal_assistant.qtr_micro_scalper.data.models import (
    LiquidationEvent,
    OrderBookEvent,
    PublicTradeEvent,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowDecisionConfig,
    ShadowDecisionEngine,
    ShadowPriceBar,
    ShadowTrade,
    ShadowTradeEventType,
    shadow_outcome,
)

HoldingMarketEvent = PublicTradeEvent | OrderBookEvent | LiquidationEvent

DEFAULT_HOLDING_EXPERIMENT_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_micro_scalper_holding_experiment.jsonl"
)
_SCHEMA_VERSION = 1
_DEFAULT_RECENT_RECORD_CAPACITY = 100_000
_MAXIMUM_RECOVERY_WARNINGS = 1_000


class HoldingVariant(StrEnum):
    A30 = "A30"
    B60 = "B60"
    C120 = "C120"
    D300 = "D300"

    @property
    def maximum_holding_bars(self) -> int:
        return {
            HoldingVariant.A30: 30,
            HoldingVariant.B60: 60,
            HoldingVariant.C120: 120,
            HoldingVariant.D300: 300,
        }[self]


class HoldingExperimentStage(StrEnum):
    WAITING_ENTRY = "WAITING_ENTRY"
    OPEN = "OPEN"
    TP1_HIT = "TP1_HIT"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    INTERRUPTED = "INTERRUPTED"


class HoldingExperimentOutcome(StrEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    WIN = "WIN"
    LOSS = "LOSS"
    BREAKEVEN = "BREAKEVEN"
    NOT_TRIGGERED = "NOT_TRIGGERED"
    INCOMPLETE = "INCOMPLETE"


class HoldingExperimentRecordType(StrEnum):
    CREATED = "CREATED"
    ENTRY_OPENED = "ENTRY_OPENED"
    TP1_REACHED = "TP1_REACHED"
    TERMINAL = "TERMINAL"
    INTERRUPTED = "INTERRUPTED"


@dataclass(frozen=True, slots=True)
class HoldingExperimentConfig:
    enabled: bool = False
    maximum_active_groups: int = 1_000

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError("Holding experiment enabled flag must be boolean.")
        if (
            isinstance(self.maximum_active_groups, bool)
            or not 1 <= self.maximum_active_groups <= 10_000
        ):
            raise ValueError(
                "Holding experiment active-group capacity must be within 1..10000."
            )

    @classmethod
    def from_environment(cls) -> HoldingExperimentConfig:
        return cls(
            enabled=_environment_bool(
                "QTR_SCALPER_V2_HOLDING_EXPERIMENT_ENABLED",
                default=False,
            ),
            maximum_active_groups=_environment_int(
                "QTR_SCALPER_V2_HOLDING_EXPERIMENT_MAX_ACTIVE_GROUPS",
                default=1_000,
            ),
        )


@dataclass(frozen=True, slots=True)
class HoldingExperimentRecord:
    record_id: str = field(init=False)
    recorded_at: datetime
    experiment_group_id: str
    baseline_trade_id: str
    variant_trade_id: str
    variant: HoldingVariant
    maximum_holding_bars: int
    record_type: HoldingExperimentRecordType
    symbol: str
    direction: ShadowDirection
    score: float
    market_state: str
    setup_context: str
    planned_at: datetime
    stage: HoldingExperimentStage
    entry: float
    entry_time: datetime | None
    stop: float
    tp1: float
    tp2: float
    exit_reason: str | None
    exit_time: datetime | None
    holding_completed_bars: int
    holding_wall_clock_seconds: float | None
    result_r: float
    mfe: float
    mae: float
    tp1_hit: bool
    tp2_hit: bool
    outcome: HoldingExperimentOutcome
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in (
            "experiment_group_id",
            "baseline_trade_id",
            "variant_trade_id",
            "market_state",
            "setup_context",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"Holding experiment {name} cannot be empty.")
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Holding experiment symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "recorded_at", _utc(self.recorded_at))
        object.__setattr__(self, "planned_at", _utc(self.planned_at))
        for name in ("entry_time", "exit_time"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value))
        if self.maximum_holding_bars != self.variant.maximum_holding_bars:
            raise ValueError("Holding experiment variant horizon is inconsistent.")
        for name, value in (
            ("entry", self.entry),
            ("stop", self.stop),
            ("tp1", self.tp1),
            ("tp2", self.tp2),
        ):
            if not _positive(value):
                raise ValueError(f"Holding experiment {name} must be positive.")
        if not _finite(self.score) or not 0.0 <= self.score <= 100.0:
            raise ValueError("Holding experiment score must be within 0..100.")
        if (
            isinstance(self.holding_completed_bars, bool)
            or self.holding_completed_bars < 0
        ):
            raise ValueError("Holding experiment completed bars cannot be negative.")
        if self.holding_wall_clock_seconds is not None and (
            not _finite(self.holding_wall_clock_seconds)
            or self.holding_wall_clock_seconds < 0
        ):
            raise ValueError("Holding experiment wall-clock duration is invalid.")
        for name, value in (
            ("result_r", self.result_r),
            ("mfe", self.mfe),
            ("mae", self.mae),
        ):
            if not _finite(value):
                raise ValueError(f"Holding experiment {name} must be finite.")
        if self.mfe < 0 or self.mae < 0:
            raise ValueError("Holding experiment excursions cannot be negative.")
        if not isinstance(self.tp1_hit, bool) or not isinstance(self.tp2_hit, bool):
            raise ValueError("Holding experiment TP flags must be boolean.")
        if self.tp2_hit and not self.tp1_hit:
            raise ValueError("Holding experiment TP2 cannot precede TP1.")
        if self.terminal and self.exit_time is None:
            raise ValueError("Terminal holding experiment record requires exit_time.")
        if self.stage is HoldingExperimentStage.INTERRUPTED:
            if self.outcome is not HoldingExperimentOutcome.INCOMPLETE:
                raise ValueError("Interrupted experiment must be INCOMPLETE.")
        elif self.outcome is HoldingExperimentOutcome.INCOMPLETE:
            raise ValueError("Only interrupted experiments can be INCOMPLETE.")
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("Holding experiment schema version is unsupported.")
        object.__setattr__(self, "record_id", _record_id(self))

    @property
    def terminal(self) -> bool:
        return self.stage in {
            HoldingExperimentStage.CLOSED,
            HoldingExperimentStage.EXPIRED,
            HoldingExperimentStage.INTERRUPTED,
        }


@dataclass(frozen=True, slots=True)
class HoldingExperimentRecovery:
    active_records: tuple[HoldingExperimentRecord, ...]
    corrupted_line_numbers: tuple[int, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HoldingExperimentJournalMetrics:
    bootstrap_scans: int
    bytes_read: int
    records_processed: int
    recent_record_ids: int
    malformed_lines: int
    active_recovered_variants: int


class HoldingExperimentJournal:
    """Append-only lifecycle journal with streaming, bounded recovery state."""

    def __init__(
        self,
        path: Path = DEFAULT_HOLDING_EXPERIMENT_JOURNAL_PATH,
        *,
        maximum_recent_record_ids: int = _DEFAULT_RECENT_RECORD_CAPACITY,
    ) -> None:
        if (
            isinstance(maximum_recent_record_ids, bool)
            or maximum_recent_record_ids < 1
        ):
            raise ValueError("Holding journal recent-ID capacity must be positive.")
        self._path = path.resolve()
        self._maximum_recent_record_ids = maximum_recent_record_ids
        self._recent_record_ids: dict[str, None] = {}
        self._lock = Lock()
        self._bootstrap_scans = 0
        self._bytes_read = 0
        self._records_processed = 0
        self._malformed_lines = 0
        self._active_recovered: dict[str, HoldingExperimentRecord] = {}
        self._corrupted_line_numbers: list[int] = []
        self._warnings: list[str] = []
        self._recover_streaming()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> HoldingExperimentRecovery:
        with self._lock:
            return HoldingExperimentRecovery(
                active_records=tuple(
                    sorted(
                        self._active_recovered.values(),
                        key=lambda item: item.variant_trade_id,
                    )
                ),
                corrupted_line_numbers=tuple(self._corrupted_line_numbers),
                warnings=tuple(self._warnings),
            )

    @property
    def metrics(self) -> HoldingExperimentJournalMetrics:
        with self._lock:
            return HoldingExperimentJournalMetrics(
                bootstrap_scans=self._bootstrap_scans,
                bytes_read=self._bytes_read,
                records_processed=self._records_processed,
                recent_record_ids=len(self._recent_record_ids),
                malformed_lines=self._malformed_lines,
                active_recovered_variants=len(self._active_recovered),
            )

    def append(self, record: HoldingExperimentRecord) -> bool:
        line = serialize_holding_experiment_record(record)
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
            self._remember_record_id(record.record_id)
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
                    record = deserialize_holding_experiment_record(
                        raw_line.decode("utf-8")
                    )
                except (UnicodeDecodeError, ValueError) as exc:
                    self._warn(line_number, str(exc))
                    continue
                self._records_processed += 1
                self._remember_record_id(record.record_id)
                self._track_active(record)

    def _track_active(self, record: HoldingExperimentRecord) -> None:
        if record.terminal:
            self._active_recovered.pop(record.variant_trade_id, None)
        else:
            self._active_recovered[record.variant_trade_id] = record

    def _remember_record_id(self, record_id: str) -> None:
        self._recent_record_ids[record_id] = None
        if len(self._recent_record_ids) > self._maximum_recent_record_ids:
            del self._recent_record_ids[next(iter(self._recent_record_ids))]

    def _warn(self, line_number: int, reason: str) -> None:
        self._malformed_lines += 1
        if len(self._warnings) >= _MAXIMUM_RECOVERY_WARNINGS:
            return
        self._corrupted_line_numbers.append(line_number)
        self._warnings.append(
            f"Holding experiment line {line_number} ignored: {reason}"
        )

    def _needs_separator(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with self._path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            return stream.read(1) != b"\n"


@dataclass(frozen=True, slots=True)
class HoldingExperimentMetrics:
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
class HoldingExperimentActivation:
    accepted: bool
    experiment_group_id: str | None
    records: tuple[HoldingExperimentRecord, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class HoldingExperimentVariantState:
    experiment_group_id: str
    variant: HoldingVariant
    maximum_holding_bars: int
    trade: ShadowTrade


@dataclass(slots=True)
class _ObservedBucket:
    index: int
    opened_at: datetime
    closed_at: datetime
    open: float
    high: float
    low: float
    close: float
    open_key: tuple[datetime, str]
    close_key: tuple[datetime, str]

    @classmethod
    def from_trade(
        cls,
        event: PublicTradeEvent,
        *,
        index: int,
        opened_at: datetime,
        closed_at: datetime,
    ) -> _ObservedBucket:
        key = (event.exchange_at, event.trade_id)
        return cls(
            index=index,
            opened_at=opened_at,
            closed_at=closed_at,
            open=event.price,
            high=event.price,
            low=event.price,
            close=event.price,
            open_key=key,
            close_key=key,
        )

    def observe(self, event: PublicTradeEvent) -> None:
        key = (event.exchange_at, event.trade_id)
        self.high = max(self.high, event.price)
        self.low = min(self.low, event.price)
        if key < self.open_key:
            self.open_key = key
            self.open = event.price
        if key > self.close_key:
            self.close_key = key
            self.close = event.price

    def freeze(self, symbol: str) -> ShadowPriceBar:
        return ShadowPriceBar(
            symbol=symbol,
            opened_at=self.opened_at,
            closed_at=self.closed_at,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
        )


@dataclass(slots=True)
class _ObservedBarStream:
    symbol: str
    starts_at: datetime
    bucket: _ObservedBucket | None = None
    last_emitted_at: datetime | None = None

    def process(self, event: HoldingMarketEvent) -> tuple[ShadowPriceBar, ...]:
        completed: list[ShadowPriceBar] = []
        if isinstance(event, PublicTradeEvent):
            completed.extend(self._observe_trade(event))
        current = self.bucket
        market_at = event.exchange_at.astimezone(UTC)
        if current is not None and current.closed_at <= market_at:
            completed.append(current.freeze(self.symbol))
            self.last_emitted_at = current.closed_at
            self.bucket = None
        return tuple(completed)

    def _observe_trade(self, event: PublicTradeEvent) -> tuple[ShadowPriceBar, ...]:
        observed_at = event.exchange_at.astimezone(UTC)
        if observed_at < self.starts_at:
            return ()
        elapsed = (observed_at - self.starts_at).total_seconds()
        index = int(elapsed)
        opened_at = self.starts_at + timedelta(seconds=index)
        closed_at = opened_at + timedelta(seconds=1)
        if self.last_emitted_at is not None and closed_at <= self.last_emitted_at:
            return ()
        current = self.bucket
        if current is None:
            self.bucket = _ObservedBucket.from_trade(
                event,
                index=index,
                opened_at=opened_at,
                closed_at=closed_at,
            )
            return ()
        if index < current.index:
            return ()
        if index == current.index:
            current.observe(event)
            return ()
        completed = current.freeze(self.symbol)
        self.last_emitted_at = current.closed_at
        self.bucket = _ObservedBucket.from_trade(
            event,
            index=index,
            opened_at=opened_at,
            closed_at=closed_at,
        )
        return (completed,)


@dataclass(slots=True)
class _ActiveVariant:
    variant: HoldingVariant
    engine: ShadowDecisionEngine
    trade: ShadowTrade


@dataclass(slots=True)
class _ActiveGroup:
    group_id: str
    baseline_trade_id: str
    symbol: str
    score: float
    market_state: str
    setup_context: str
    stream: _ObservedBarStream
    variants: dict[HoldingVariant, _ActiveVariant]


class HoldingExperimentRuntime:
    """Parallel shadow-only holding horizons fed by observed public trades."""

    def __init__(
        self,
        journal: HoldingExperimentJournal,
        config: HoldingExperimentConfig | None = None,
        *,
        baseline_decision_config: ShadowDecisionConfig | None = None,
    ) -> None:
        self._journal = journal
        self._config = config or HoldingExperimentConfig()
        self._baseline = baseline_decision_config or ShadowDecisionConfig()
        self._groups: dict[str, _ActiveGroup] = {}
        self._groups_by_symbol: dict[str, set[str]] = {}
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

    def start(self, *, at: datetime) -> tuple[HoldingExperimentRecord, ...]:
        normalized_at = _utc(at)
        with self._lock:
            if self._started or not self.enabled:
                return ()
            interrupted = tuple(
                _interrupt_record(record, interrupted_at=normalized_at)
                for record in self._journal.recovery.active_records
            )
            persisted = tuple(
                record for record in interrupted if self._journal.append(record)
            )
            self._interrupted_variants += len(persisted)
            self._journal.clear_recovered_active()
            self._started = True
            return persisted

    def activate(
        self,
        baseline_trade: ShadowTrade,
        *,
        score: float,
        market_state: str,
        setup_context: str,
    ) -> HoldingExperimentActivation:
        if not self.enabled:
            return HoldingExperimentActivation(
                False,
                None,
                (),
                "Holding experiment is disabled.",
            )
        group_id = _group_id(baseline_trade.trade_id)
        with self._lock:
            if group_id in self._groups or group_id in self._seen_groups:
                return HoldingExperimentActivation(
                    False,
                    group_id,
                    (),
                    "Duplicate holding experiment group suppressed.",
                )
            if len(self._groups) >= self._config.maximum_active_groups:
                self._capacity_rejections += 1
                self._last_warning = (
                    "Holding experiment active-group capacity reached; "
                    "baseline shadow trade remains unaffected."
                )
                return HoldingExperimentActivation(
                    False,
                    group_id,
                    (),
                    self._last_warning,
                )
            variants = {
                variant: _variant_from_baseline(
                    baseline_trade,
                    variant,
                    baseline_config=self._baseline,
                )
                for variant in HoldingVariant
            }
            group = _ActiveGroup(
                group_id=group_id,
                baseline_trade_id=baseline_trade.trade_id,
                symbol=baseline_trade.symbol,
                score=score,
                market_state=market_state,
                setup_context=setup_context,
                stream=_ObservedBarStream(
                    symbol=baseline_trade.symbol,
                    starts_at=baseline_trade.planned_at,
                ),
                variants=variants,
            )
            self._groups[group_id] = group
            self._groups_by_symbol.setdefault(baseline_trade.symbol, set()).add(
                group_id
            )
            self._remember_group(group_id)
            self._groups_created += 1
            records = tuple(
                _record_from_trade(
                    group,
                    item,
                    record_type=HoldingExperimentRecordType.CREATED,
                    recorded_at=baseline_trade.planned_at,
                )
                for item in variants.values()
            )
            for record in records:
                self._journal.append(record)
            return HoldingExperimentActivation(
                True,
                group_id,
                records,
                "A30/B60/C120/D300 holding experiment created.",
            )

    def process_event(
        self,
        event: HoldingMarketEvent,
    ) -> tuple[HoldingExperimentRecord, ...]:
        if not self.enabled:
            return ()
        with self._lock:
            group_ids = tuple(sorted(self._groups_by_symbol.get(event.symbol, ())))
            persisted: list[HoldingExperimentRecord] = []
            for group_id in group_ids:
                group = self._groups.get(group_id)
                if group is None:
                    continue
                for bar in group.stream.process(event):
                    persisted.extend(self._process_bar(group, bar))
                if not group.variants:
                    self._remove_group(group)
            return tuple(persisted)

    def stop(self, *, at: datetime) -> tuple[HoldingExperimentRecord, ...]:
        normalized_at = _utc(at)
        with self._lock:
            interrupted: list[HoldingExperimentRecord] = []
            for group in tuple(self._groups.values()):
                for active in group.variants.values():
                    record = _interrupted_from_trade(
                        group,
                        active,
                        interrupted_at=normalized_at,
                    )
                    if self._journal.append(record):
                        interrupted.append(record)
                self._remove_group(group)
            self._interrupted_variants += len(interrupted)
            self._journal.flush()
            self._started = False
            return tuple(interrupted)

    def protected_symbols(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._groups_by_symbol))

    def metrics(self) -> HoldingExperimentMetrics:
        with self._lock:
            return HoldingExperimentMetrics(
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

    def active_variant_states(self) -> tuple[HoldingExperimentVariantState, ...]:
        with self._lock:
            return tuple(
                HoldingExperimentVariantState(
                    experiment_group_id=group.group_id,
                    variant=active.variant,
                    maximum_holding_bars=active.variant.maximum_holding_bars,
                    trade=active.trade,
                )
                for group in sorted(
                    self._groups.values(),
                    key=lambda item: item.group_id,
                )
                for active in (
                    group.variants[variant]
                    for variant in HoldingVariant
                    if variant in group.variants
                )
            )

    def _process_bar(
        self,
        group: _ActiveGroup,
        bar: ShadowPriceBar,
    ) -> tuple[HoldingExperimentRecord, ...]:
        persisted: list[HoldingExperimentRecord] = []
        for variant in tuple(HoldingVariant):
            active = group.variants.get(variant)
            if active is None:
                continue
            previous = active.trade
            updated = active.engine.process_bar(previous, bar)
            active.trade = updated
            record_type = _record_type(previous, updated)
            if record_type is not None:
                record = _record_from_trade(
                    group,
                    active,
                    record_type=record_type,
                    recorded_at=bar.closed_at,
                )
                if self._journal.append(record):
                    persisted.append(record)
            if updated.terminal:
                group.variants.pop(variant, None)
                self._variants_completed += 1
        return tuple(persisted)

    def _remove_group(self, group: _ActiveGroup) -> None:
        self._groups.pop(group.group_id, None)
        symbol_groups = self._groups_by_symbol.get(group.symbol)
        if symbol_groups is None:
            return
        symbol_groups.discard(group.group_id)
        if not symbol_groups:
            self._groups_by_symbol.pop(group.symbol, None)

    def _remember_group(self, group_id: str) -> None:
        self._seen_groups[group_id] = None
        if len(self._seen_groups) > _DEFAULT_RECENT_RECORD_CAPACITY:
            del self._seen_groups[next(iter(self._seen_groups))]


def iter_holding_experiment_records(
    path: Path,
) -> Iterator[HoldingExperimentRecord]:
    """Stream valid experiment records without materializing the JSONL."""

    if not path.exists():
        return
    with path.open("rb") as stream:
        for raw_line in stream:
            if not raw_line.strip() or not raw_line.endswith(b"\n"):
                continue
            try:
                yield deserialize_holding_experiment_record(
                    raw_line.decode("utf-8")
                )
            except (UnicodeDecodeError, ValueError):
                continue


def serialize_holding_experiment_record(record: HoldingExperimentRecord) -> str:
    return json.dumps(
        _record_payload(record, include_record_id=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def deserialize_holding_experiment_record(line: str) -> HoldingExperimentRecord:
    try:
        loaded = cast(object, json.loads(line))
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Holding experiment line is not valid JSON.") from exc
    if not isinstance(loaded, dict):
        raise ValueError("Holding experiment line must contain an object.")
    payload = cast(dict[str, object], loaded)
    try:
        expected_id = _string(payload, "record_id")
        record = HoldingExperimentRecord(
            recorded_at=_datetime(payload, "recorded_at"),
            experiment_group_id=_string(payload, "experiment_group_id"),
            baseline_trade_id=_string(payload, "baseline_trade_id"),
            variant_trade_id=_string(payload, "variant_trade_id"),
            variant=HoldingVariant(_string(payload, "variant")),
            maximum_holding_bars=_integer(payload, "maximum_holding_bars"),
            record_type=HoldingExperimentRecordType(
                _string(payload, "record_type")
            ),
            symbol=_string(payload, "symbol"),
            direction=ShadowDirection(_string(payload, "direction")),
            score=_number(payload, "score"),
            market_state=_string(payload, "market_state"),
            setup_context=_string(payload, "setup_context"),
            planned_at=_datetime(payload, "planned_at"),
            stage=HoldingExperimentStage(_string(payload, "stage")),
            entry=_number(payload, "entry"),
            entry_time=_optional_datetime(payload, "entry_time"),
            stop=_number(payload, "stop"),
            tp1=_number(payload, "tp1"),
            tp2=_number(payload, "tp2"),
            exit_reason=_optional_string(payload, "exit_reason"),
            exit_time=_optional_datetime(payload, "exit_time"),
            holding_completed_bars=_integer(payload, "holding_completed_bars"),
            holding_wall_clock_seconds=_optional_number(
                payload,
                "holding_wall_clock_seconds",
            ),
            result_r=_number(payload, "result_r"),
            mfe=_number(payload, "mfe"),
            mae=_number(payload, "mae"),
            tp1_hit=_boolean(payload, "tp1_hit"),
            tp2_hit=_boolean(payload, "tp2_hit"),
            outcome=HoldingExperimentOutcome(_string(payload, "outcome")),
            schema_version=_integer(payload, "schema_version"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Holding experiment line has an invalid schema.") from exc
    if record.record_id != expected_id:
        raise ValueError("Holding experiment record id does not match payload.")
    return record


def _variant_from_baseline(
    baseline: ShadowTrade,
    variant: HoldingVariant,
    *,
    baseline_config: ShadowDecisionConfig,
) -> _ActiveVariant:
    config = replace(
        baseline_config,
        maximum_holding_bars=variant.maximum_holding_bars,
    )
    trade = replace(
        baseline,
        trade_id=f"{baseline.trade_id}-holding-{variant.value.lower()}",
    )
    return _ActiveVariant(
        variant=variant,
        engine=ShadowDecisionEngine(config),
        trade=trade,
    )


def _record_type(
    previous: ShadowTrade,
    updated: ShadowTrade,
) -> HoldingExperimentRecordType | None:
    if updated.terminal:
        return HoldingExperimentRecordType.TERMINAL
    new_events = updated.events[len(previous.events) :]
    if any(event.event_type is ShadowTradeEventType.TP1 for event in new_events):
        return HoldingExperimentRecordType.TP1_REACHED
    if any(event.event_type is ShadowTradeEventType.ENTRY for event in new_events):
        return HoldingExperimentRecordType.ENTRY_OPENED
    return None


def _record_from_trade(
    group: _ActiveGroup,
    active: _ActiveVariant,
    *,
    record_type: HoldingExperimentRecordType,
    recorded_at: datetime,
) -> HoldingExperimentRecord:
    trade = active.trade
    outcome = shadow_outcome(trade)
    exit_reason = trade.events[-1].event_type.value if trade.terminal else None
    wall_clock = (
        (trade.closed_at - trade.entry_at).total_seconds()
        if trade.closed_at is not None and trade.entry_at is not None
        else None
    )
    return HoldingExperimentRecord(
        recorded_at=recorded_at,
        experiment_group_id=group.group_id,
        baseline_trade_id=group.baseline_trade_id,
        variant_trade_id=trade.trade_id,
        variant=active.variant,
        maximum_holding_bars=active.variant.maximum_holding_bars,
        record_type=record_type,
        symbol=trade.symbol,
        direction=trade.direction,
        score=group.score,
        market_state=group.market_state,
        setup_context=group.setup_context,
        planned_at=trade.planned_at,
        stage=HoldingExperimentStage(trade.stage.value),
        entry=trade.entry_price,
        entry_time=trade.entry_at,
        stop=trade.initial_stop,
        tp1=trade.tp1_price,
        tp2=trade.tp2_price,
        exit_reason=exit_reason,
        exit_time=trade.closed_at,
        holding_completed_bars=trade.bars_held,
        holding_wall_clock_seconds=wall_clock,
        result_r=trade.realized_r,
        mfe=trade.max_favorable_excursion_r,
        mae=trade.max_adverse_excursion_r,
        tp1_hit=trade.tp1_hit,
        tp2_hit=trade.tp2_hit,
        outcome=HoldingExperimentOutcome(outcome.status.value),
    )


def _interrupted_from_trade(
    group: _ActiveGroup,
    active: _ActiveVariant,
    *,
    interrupted_at: datetime,
) -> HoldingExperimentRecord:
    record = _record_from_trade(
        group,
        active,
        record_type=HoldingExperimentRecordType.CREATED,
        recorded_at=interrupted_at,
    )
    return _interrupt_record(record, interrupted_at=interrupted_at)


def _interrupt_record(
    record: HoldingExperimentRecord,
    *,
    interrupted_at: datetime,
) -> HoldingExperimentRecord:
    normalized = _utc(interrupted_at)
    wall_clock = (
        max(0.0, (normalized - record.entry_time).total_seconds())
        if record.entry_time is not None
        else None
    )
    return replace(
        record,
        recorded_at=normalized,
        record_type=HoldingExperimentRecordType.INTERRUPTED,
        stage=HoldingExperimentStage.INTERRUPTED,
        exit_reason="INTERRUPTED",
        exit_time=normalized,
        holding_wall_clock_seconds=wall_clock,
        outcome=HoldingExperimentOutcome.INCOMPLETE,
    )


def _group_id(baseline_trade_id: str) -> str:
    digest = hashlib.sha256(baseline_trade_id.encode("utf-8")).hexdigest()[:20]
    return f"holding-{digest}"


def _record_id(record: HoldingExperimentRecord) -> str:
    canonical = json.dumps(
        _record_payload(record, include_record_id=False),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _record_payload(
    record: HoldingExperimentRecord,
    *,
    include_record_id: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "baseline_trade_id": record.baseline_trade_id,
        "direction": record.direction.value,
        "entry": record.entry,
        "entry_time": _iso(record.entry_time),
        "exit_reason": record.exit_reason,
        "exit_time": _iso(record.exit_time),
        "experiment_group_id": record.experiment_group_id,
        "holding_completed_bars": record.holding_completed_bars,
        "holding_wall_clock_seconds": record.holding_wall_clock_seconds,
        "mae": record.mae,
        "market_state": record.market_state,
        "maximum_holding_bars": record.maximum_holding_bars,
        "mfe": record.mfe,
        "outcome": record.outcome.value,
        "planned_at": record.planned_at.isoformat(),
        "record_type": record.record_type.value,
        "recorded_at": record.recorded_at.isoformat(),
        "result_r": record.result_r,
        "schema_version": record.schema_version,
        "score": record.score,
        "setup_context": record.setup_context,
        "stage": record.stage.value,
        "stop": record.stop,
        "symbol": record.symbol,
        "tp1": record.tp1,
        "tp1_hit": record.tp1_hit,
        "tp2": record.tp2,
        "tp2_hit": record.tp2_hit,
        "variant": record.variant.value,
        "variant_trade_id": record.variant_trade_id,
    }
    if include_record_id:
        payload["record_id"] = record.record_id
    return payload


def _string(payload: dict[str, object], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Holding experiment {key} must be a string.")
    return value


def _optional_string(payload: dict[str, object], key: str) -> str | None:
    value = payload[key]
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Holding experiment {key} must be a string or null.")
    return value


def _number(payload: dict[str, object], key: str) -> float:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Holding experiment {key} must be numeric.")
    return float(value)


def _optional_number(payload: dict[str, object], key: str) -> float | None:
    return None if payload[key] is None else _number(payload, key)


def _integer(payload: dict[str, object], key: str) -> int:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Holding experiment {key} must be an integer.")
    return value


def _boolean(payload: dict[str, object], key: str) -> bool:
    value = payload[key]
    if not isinstance(value, bool):
        raise ValueError(f"Holding experiment {key} must be boolean.")
    return value


def _datetime(payload: dict[str, object], key: str) -> datetime:
    return _utc(datetime.fromisoformat(_string(payload, key)))


def _optional_datetime(
    payload: dict[str, object],
    key: str,
) -> datetime | None:
    return None if payload[key] is None else _datetime(payload, key)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


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


def _environment_int(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer.") from exc


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Holding experiment timestamp must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError("Holding experiment timestamp must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _positive(value: float) -> bool:
    return _finite(value) and value > 0
