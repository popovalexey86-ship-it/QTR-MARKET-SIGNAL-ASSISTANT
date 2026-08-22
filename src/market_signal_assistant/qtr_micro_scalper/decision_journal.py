from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import cast

from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperComponentScores,
)

DEFAULT_DECISION_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_micro_scalper_decisions.jsonl"
)
_SCHEMA_VERSION = 1


class ShadowDecisionEventType(StrEnum):
    TARGET_FOUND = "TARGET_FOUND"
    ANALYSIS_STARTED = "ANALYSIS_STARTED"
    SNAPSHOT_READY = "SNAPSHOT_READY"
    SCORE_CREATED = "SCORE_CREATED"
    DECISION_BLOCKED = "DECISION_BLOCKED"
    SHADOW_ENTRY_CREATED = "SHADOW_ENTRY_CREATED"
    TRADE_UPDATED = "TRADE_UPDATED"
    TRADE_FINISHED = "TRADE_FINISHED"


_DEFAULT_EVENT_ID_CAPACITY = 100_000
_DEFAULT_DIAGNOSTIC_INTERVAL = timedelta(minutes=1)
_MAXIMUM_RECOVERY_WARNINGS = 1_000
_DIAGNOSTIC_EVENTS = frozenset(
    {
        ShadowDecisionEventType.SNAPSHOT_READY,
        ShadowDecisionEventType.SCORE_CREATED,
        ShadowDecisionEventType.DECISION_BLOCKED,
    }
)


@dataclass(frozen=True, slots=True)
class ShadowDecisionRecord:
    event_id: str = field(init=False)
    timestamp: datetime
    symbol: str
    event_type: ShadowDecisionEventType
    score: float | None
    score_components: ScalperComponentScores | None
    market_state: str | None
    setup_context: str | None
    reasons: tuple[str, ...]
    warnings: tuple[str, ...] = ()
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp))
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Decision journal symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        if not isinstance(self.event_type, ShadowDecisionEventType):
            raise ValueError("Decision journal event type is invalid.")
        if self.score is not None and (
            not _finite(self.score) or not 0.0 <= self.score <= 100.0
        ):
            raise ValueError("Decision journal score must be between 0 and 100.")
        if self.score_components is not None and not isinstance(
            self.score_components,
            ScalperComponentScores,
        ):
            raise ValueError("Decision journal score components are invalid.")
        for name in ("market_state", "setup_context"):
            value = getattr(self, name)
            if value is not None:
                normalized = value.strip().upper()
                if not normalized:
                    raise ValueError(f"Decision journal {name} cannot be empty.")
                object.__setattr__(self, name, normalized)
        reasons = _texts("reason", self.reasons)
        warnings = _texts("warning", self.warnings)
        if not reasons:
            raise ValueError("Decision journal record requires reasons.")
        object.__setattr__(self, "reasons", reasons)
        object.__setattr__(self, "warnings", warnings)
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("Decision journal schema version is unsupported.")
        object.__setattr__(self, "event_id", _event_id(self))


@dataclass(frozen=True, slots=True)
class DecisionJournalRecovery:
    records: tuple[ShadowDecisionRecord, ...]
    corrupted_line_numbers: tuple[int, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DecisionJournalIndexMetrics:
    bootstrap_scans: int
    incremental_reads: int
    bytes_read: int
    records_processed: int
    cached_event_ids: int
    malformed_lines: int
    resets_rotations: int
    file_offset: int


class DecisionJournalIndex:
    """Streaming JSONL index with bounded recent-ID duplicate protection."""

    def __init__(
        self,
        path: Path = DEFAULT_DECISION_JOURNAL_PATH,
        *,
        on_record: Callable[[ShadowDecisionRecord], None] | None = None,
        on_reset: Callable[[], None] | None = None,
        maximum_cached_event_ids: int = _DEFAULT_EVENT_ID_CAPACITY,
    ) -> None:
        if isinstance(maximum_cached_event_ids, bool) or maximum_cached_event_ids < 1:
            raise ValueError("Decision journal event ID capacity must be positive.")
        self._path = path.resolve()
        self._on_record = on_record
        self._on_reset = on_reset
        self._maximum_cached_event_ids = maximum_cached_event_ids
        self._event_ids: dict[bytes, None] = {}
        self._record_count = 0
        self._offset = 0
        self._line_number = 0
        self._observed_size = 0
        self._identity: tuple[int, int] | None = None
        self._bootstrapped = False
        self._bootstrap_scans = 0
        self._incremental_reads = 0
        self._bytes_read = 0
        self._records_processed = 0
        self._malformed_lines = 0
        self._resets_rotations = 0
        self._corrupted_line_numbers: list[int] = []
        self._warnings: list[str] = []
        self._lock = Lock()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def record_count(self) -> int:
        with self._lock:
            return self._record_count

    @property
    def recovery(self) -> DecisionJournalRecovery:
        with self._lock:
            return DecisionJournalRecovery(
                records=(),
                corrupted_line_numbers=tuple(self._corrupted_line_numbers),
                warnings=tuple(self._warnings),
            )

    def contains(self, event_id: str) -> bool:
        with self._lock:
            return _event_key(event_id) in self._event_ids

    def metrics(self) -> DecisionJournalIndexMetrics:
        with self._lock:
            return DecisionJournalIndexMetrics(
                bootstrap_scans=self._bootstrap_scans,
                incremental_reads=self._incremental_reads,
                bytes_read=self._bytes_read,
                records_processed=self._records_processed,
                cached_event_ids=len(self._event_ids),
                malformed_lines=self._malformed_lines,
                resets_rotations=self._resets_rotations,
                file_offset=self._offset,
            )

    def refresh(self) -> int:
        """Consume complete lines appended after the current byte offset."""

        with self._lock:
            if not self._path.exists():
                if self._identity is not None:
                    self._reset_locked()
                return 0
            stat = self._path.stat()
            identity = (stat.st_dev, stat.st_ino)
            if (
                self._identity is not None
                and (identity != self._identity or stat.st_size < self._offset)
            ):
                self._reset_locked()
            if stat.st_size == self._observed_size:
                return 0
            self._observed_size = stat.st_size
            self._identity = identity
            if stat.st_size <= self._offset:
                return 0
            if not self._bootstrapped:
                self._bootstrap_scans += 1
                self._bootstrapped = True
            else:
                self._incremental_reads += 1
            accepted = 0
            with self._path.open("rb") as stream:
                stream.seek(self._offset)
                while True:
                    line_start = stream.tell()
                    raw_line = stream.readline()
                    if not raw_line:
                        break
                    self._bytes_read += len(raw_line)
                    if not raw_line.endswith(b"\n"):
                        stream.seek(line_start)
                        break
                    self._offset = stream.tell()
                    self._line_number += 1
                    if not raw_line.strip():
                        continue
                    try:
                        record = deserialize_decision_record(
                            raw_line.decode("utf-8")
                        )
                    except (UnicodeDecodeError, ValueError) as exc:
                        self._malformed_lines += 1
                        if len(self._warnings) < _MAXIMUM_RECOVERY_WARNINGS:
                            self._corrupted_line_numbers.append(self._line_number)
                            self._warnings.append(
                                "Decision journal line "
                                f"{self._line_number} ignored: {exc}"
                            )
                        continue
                    event_key = _event_key(record.event_id)
                    if event_key in self._event_ids:
                        continue
                    self._remember_event_key_locked(event_key)
                    self._record_count += 1
                    self._records_processed += 1
                    accepted += 1
                    if self._on_record is not None:
                        self._on_record(record)
            return accepted

    def _remember_event_key_locked(self, event_key: bytes) -> None:
        self._event_ids[event_key] = None
        if len(self._event_ids) > self._maximum_cached_event_ids:
            del self._event_ids[next(iter(self._event_ids))]

    def _reset_locked(self) -> None:
        self._observed_size = 0
        self._event_ids.clear()
        self._record_count = 0
        self._offset = 0
        self._line_number = 0
        self._identity = None
        self._bootstrapped = False
        self._corrupted_line_numbers.clear()
        self._warnings.clear()
        self._resets_rotations += 1
        if self._on_reset is not None:
            self._on_reset()



class ShadowDecisionJournal:
    """Thread-safe append journal with bounded diagnostic write amplification."""

    def __init__(
        self,
        path: Path = DEFAULT_DECISION_JOURNAL_PATH,
        *,
        index: DecisionJournalIndex | None = None,
        retain_records: bool = True,
        diagnostic_interval: timedelta = _DEFAULT_DIAGNOSTIC_INTERVAL,
    ) -> None:
        if diagnostic_interval.total_seconds() <= 0:
            raise ValueError("Decision journal diagnostic interval must be positive.")
        self._path = path.resolve()
        self._lock = Lock()
        self._retain_records = retain_records
        self._records: list[ShadowDecisionRecord] = []
        self._diagnostic_interval = diagnostic_interval
        self._diagnostic_state: dict[
            tuple[str, ShadowDecisionEventType], datetime
        ] = {}
        if index is not None and index.path != self._path:
            raise ValueError("Decision journal index path does not match journal path.")
        if index is not None and retain_records:
            raise ValueError("Shared decision journal index requires lean retention.")
        self._index = index or DecisionJournalIndex(
            self._path,
            on_record=self._records.append if retain_records else None,
            on_reset=self._records.clear if retain_records else None,
        )
        self._index.refresh()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> DecisionJournalRecovery:
        state = self._index.recovery
        return DecisionJournalRecovery(
            records=self.records(),
            corrupted_line_numbers=state.corrupted_line_numbers,
            warnings=state.warnings,
        )

    @property
    def record_count(self) -> int:
        return self._index.record_count

    @property
    def index_metrics(self) -> DecisionJournalIndexMetrics:
        return self._index.metrics()

    @property
    def diagnostic_state_size(self) -> int:
        with self._lock:
            return len(self._diagnostic_state)

    def append(self, record: ShadowDecisionRecord) -> bool:
        with self._lock:
            self._index.refresh()
            if self._index.contains(record.event_id):
                return False
            if not self._should_persist_diagnostic(record):
                return False
            line = serialize_decision_record(record)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            needs_separator = self._needs_separator()
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                if needs_separator:
                    stream.write("\n")
                stream.write(line)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            return self._index.refresh() > 0

    def _should_persist_diagnostic(self, record: ShadowDecisionRecord) -> bool:
        if record.event_type not in _DIAGNOSTIC_EVENTS:
            return True
        key = (record.symbol, record.event_type)
        previous_at = self._diagnostic_state.get(key)
        if (
            previous_at is not None
            and record.timestamp - previous_at < self._diagnostic_interval
        ):
            return False
        self._diagnostic_state[key] = record.timestamp
        self._prune_diagnostic_state(record.timestamp, preserve=key)
        return True

    def _prune_diagnostic_state(
        self,
        current_at: datetime,
        *,
        preserve: tuple[str, ShadowDecisionEventType],
    ) -> None:
        expired = tuple(
            key
            for key, persisted_at in self._diagnostic_state.items()
            if key != preserve
            and current_at - persisted_at >= self._diagnostic_interval
        )
        for key in expired:
            del self._diagnostic_state[key]

    def records(self) -> tuple[ShadowDecisionRecord, ...]:
        with self._lock:
            if not self._retain_records:
                return ()
            return tuple(self._records)

    def flush(self) -> None:
        with self._lock:
            if not self._path.exists():
                return
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.flush()
                os.fsync(stream.fileno())

    def refresh(self) -> int:
        return self._index.refresh()

    def _needs_separator(self) -> bool:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        with self._path.open("rb") as stream:
            stream.seek(-1, os.SEEK_END)
            return stream.read(1) != b"\n"


def serialize_decision_record(record: ShadowDecisionRecord) -> str:
    return json.dumps(
        _payload(record, include_event_id=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def deserialize_decision_record(line: str) -> ShadowDecisionRecord:
    try:
        loaded = cast(object, json.loads(line))
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Decision journal line is not valid JSON.") from exc
    if not isinstance(loaded, dict):
        raise ValueError("Decision journal line must contain an object.")
    payload = cast(dict[str, object], loaded)
    expected_id = _string(payload, "event_id")
    components_value = payload.get("score_components")
    components = (
        None
        if components_value is None
        else _components(components_value)
    )
    record = ShadowDecisionRecord(
        timestamp=_datetime(payload, "timestamp"),
        symbol=_string(payload, "symbol"),
        event_type=ShadowDecisionEventType(_string(payload, "event_type")),
        score=_optional_number(payload, "score"),
        score_components=components,
        market_state=_optional_string(payload, "market_state"),
        setup_context=_optional_string(payload, "setup_context"),
        reasons=_strings(payload, "reasons"),
        warnings=_strings(payload, "warnings"),
        schema_version=_integer(payload, "schema_version"),
    )
    if record.event_id != expected_id:
        raise ValueError("Decision journal event ID does not match its payload.")
    return record


def recover_decision_journal(path: Path) -> DecisionJournalRecovery:
    resolved = path.resolve()
    if not resolved.exists():
        return DecisionJournalRecovery((), (), ())
    records: list[ShadowDecisionRecord] = []
    index = DecisionJournalIndex(
        resolved,
        on_record=records.append,
        on_reset=records.clear,
    )
    index.refresh()
    state = index.recovery
    return DecisionJournalRecovery(
        records=tuple(records),
        corrupted_line_numbers=state.corrupted_line_numbers,
        warnings=state.warnings,
    )


def _event_id(record: ShadowDecisionRecord) -> str:
    canonical = json.dumps(
        _payload(record, include_event_id=False),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"shadow-decision-{digest}"

def _event_key(event_id: str) -> bytes:
    prefix = "shadow-decision-"
    if event_id.startswith(prefix):
        try:
            return bytes.fromhex(event_id[len(prefix) :])
        except ValueError:
            pass
    return hashlib.sha256(event_id.encode("utf-8")).digest()


def _payload(
    record: ShadowDecisionRecord,
    *,
    include_event_id: bool,
) -> dict[str, object]:
    components: dict[str, float] | None = None
    if record.score_components is not None:
        components = {
            "liquidity_score": record.score_components.liquidity_score,
            "trade_flow_score": record.score_components.trade_flow_score,
            "orderbook_score": record.score_components.orderbook_score,
            "market_state_score": record.score_components.market_state_score,
            "setup_score": record.score_components.setup_score,
            "risk_score": record.score_components.risk_score,
        }
    payload: dict[str, object] = {
        "timestamp": record.timestamp.isoformat(),
        "symbol": record.symbol,
        "event_type": record.event_type.value,
        "score": record.score,
        "score_components": components,
        "market_state": record.market_state,
        "setup_context": record.setup_context,
        "reasons": list(record.reasons),
        "warnings": list(record.warnings),
        "schema_version": record.schema_version,
    }
    if include_event_id:
        payload["event_id"] = record.event_id
    return payload


def _components(value: object) -> ScalperComponentScores:
    if not isinstance(value, dict):
        raise ValueError("Decision journal score components must be an object.")
    payload = cast(dict[str, object], value)
    return ScalperComponentScores(
        liquidity_score=_number(payload, "liquidity_score"),
        trade_flow_score=_number(payload, "trade_flow_score"),
        orderbook_score=_number(payload, "orderbook_score"),
        market_state_score=_number(payload, "market_state_score"),
        setup_score=_number(payload, "setup_score"),
        risk_score=_number(payload, "risk_score"),
    )


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise ValueError(f"Decision journal {name} must be a string.")
    return value


def _optional_string(payload: dict[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Decision journal {name} must be a string or null.")
    return value


def _number(payload: dict[str, object], name: str) -> float:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Decision journal {name} must be numeric.")
    return float(value)


def _optional_number(payload: dict[str, object], name: str) -> float | None:
    return None if payload.get(name) is None else _number(payload, name)


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Decision journal {name} must be an integer.")
    return value


def _datetime(payload: dict[str, object], name: str) -> datetime:
    try:
        return datetime.fromisoformat(_string(payload, name))
    except ValueError as exc:
        raise ValueError(f"Decision journal {name} is invalid.") from exc


def _strings(payload: dict[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"Decision journal {name} must be a string list.")
    return tuple(cast(list[str], value))


def _texts(name: str, values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(value.strip() for value in values)
    if any(not value for value in normalized):
        raise ValueError(f"Decision journal {name} cannot be empty.")
    return tuple(dict.fromkeys(normalized))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Decision journal timestamp must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )
