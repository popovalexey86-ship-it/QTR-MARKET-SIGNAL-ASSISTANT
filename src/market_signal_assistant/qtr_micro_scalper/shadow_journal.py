from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import cast

from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperDirection,
    ScalperScore,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
    ShadowTrade,
    ShadowTradeStage,
    shadow_outcome,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEvent,
    ShadowRuntimeEventType,
)

DEFAULT_SHADOW_JOURNAL_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_micro_scalper_shadow_journal.jsonl"
)
_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class ShadowTradeRecord:
    record_id: str = field(init=False)
    recorded_at: datetime
    trade_id: str
    symbol: str
    direction: ShadowDirection
    stage: ShadowTradeStage
    entry: float
    stop: float
    tp1: float
    tp2: float
    score: float
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    entry_time: datetime | None
    exit_time: datetime | None
    outcome: ShadowOutcomeStatus
    result_r: float
    mfe: float
    mae: float
    events: tuple[ShadowRuntimeEvent, ...]
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not self.trade_id.strip():
            raise ValueError("Shadow journal record identity cannot be empty.")
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Shadow journal record symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "recorded_at", _utc("recorded_at", self.recorded_at))
        for name in ("entry_time", "exit_time"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(name, value))
        for name, value in (
            ("entry", self.entry),
            ("stop", self.stop),
            ("tp1", self.tp1),
            ("tp2", self.tp2),
        ):
            if not _finite(value) or value <= 0:
                raise ValueError(f"Shadow journal {name} must be positive.")
        if not _finite(self.score) or not 0.0 <= self.score <= 100.0:
            raise ValueError("Shadow journal score must be between 0 and 100.")
        for name, value in (
            ("result_r", self.result_r),
            ("mfe", self.mfe),
            ("mae", self.mae),
        ):
            if not _finite(value):
                raise ValueError(f"Shadow journal {name} must be finite.")
        if self.mfe < 0 or self.mae < 0:
            raise ValueError("Shadow journal excursions cannot be negative.")
        reasons = _texts("reason", self.reasons)
        warnings = _texts("warning", self.warnings)
        if not reasons:
            raise ValueError("Shadow journal record requires reasons.")
        object.__setattr__(self, "reasons", reasons)
        object.__setattr__(self, "warnings", warnings)
        events = tuple(self.events)
        if any(
            event.trade_id != self.trade_id or event.symbol != symbol
            for event in events
        ):
            raise ValueError("Shadow journal event belongs to another trade.")
        object.__setattr__(self, "events", events)
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("Shadow journal schema version is unsupported.")
        object.__setattr__(self, "record_id", _record_id(self))


@dataclass(frozen=True, slots=True)
class ShadowJournalRecovery:
    records: tuple[ShadowTradeRecord, ...]
    corrupted_line_numbers: tuple[int, ...]
    warnings: tuple[str, ...]


class ShadowTradeJournal:
    """Thread-safe append-only JSONL journal with restart recovery."""

    def __init__(self, path: Path = DEFAULT_SHADOW_JOURNAL_PATH) -> None:
        self._path = path.resolve()
        self._lock = Lock()
        recovery = recover_shadow_journal(self._path)
        self._records = list(recovery.records)
        self._record_ids = {record.record_id for record in recovery.records}
        self._recovery = recovery

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> ShadowJournalRecovery:
        return self._recovery

    def append(self, record: ShadowTradeRecord) -> bool:
        line = serialize_shadow_trade_record(record)
        with self._lock:
            if record.record_id in self._record_ids:
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(line)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._record_ids.add(record.record_id)
            self._records.append(record)
            return True

    def records(self) -> tuple[ShadowTradeRecord, ...]:
        with self._lock:
            return tuple(self._records)

    def flush(self) -> None:
        """Durably flush the append-only file; append already fsyncs each record."""

        with self._lock:
            if not self._path.exists():
                return
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.flush()
                os.fsync(stream.fileno())

    def latest(self, trade_id: str) -> ShadowTradeRecord | None:
        normalized = trade_id.strip()
        if not normalized:
            raise ValueError("Shadow trade id cannot be empty.")
        with self._lock:
            return next(
                (
                    record
                    for record in reversed(self._records)
                    if record.trade_id == normalized
                ),
                None,
            )


def build_shadow_trade_record(
    trade: ShadowTrade,
    score: ScalperScore,
    *,
    recorded_at: datetime,
    events: tuple[ShadowRuntimeEvent, ...] = (),
    reasons: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
) -> ShadowTradeRecord:
    normalized_at = _utc("recorded_at", recorded_at)
    latest_state_at = trade.last_processed_at or trade.planned_at
    if normalized_at < latest_state_at:
        raise ValueError("Shadow journal record cannot precede its trade state.")
    expected_direction = (
        ScalperDirection.LONG
        if trade.direction is ShadowDirection.LONG
        else ScalperDirection.SHORT
    )
    if score.direction is not expected_direction:
        raise ValueError("Shadow trade and scalper score directions do not match.")
    outcome = shadow_outcome(trade)
    draft = ShadowTradeRecord(
        recorded_at=normalized_at,
        trade_id=trade.trade_id,
        symbol=trade.symbol,
        direction=trade.direction,
        stage=trade.stage,
        entry=trade.entry_price,
        stop=trade.initial_stop,
        tp1=trade.tp1_price,
        tp2=trade.tp2_price,
        score=score.total_score,
        reasons=_unique((*score.reasons, *reasons)),
        warnings=_unique((*score.warnings, *warnings)),
        entry_time=trade.entry_at,
        exit_time=trade.closed_at,
        outcome=outcome.status,
        result_r=trade.realized_r,
        mfe=trade.max_favorable_excursion_r,
        mae=trade.max_adverse_excursion_r,
        events=events,
    )
    return draft


def serialize_shadow_trade_record(record: ShadowTradeRecord) -> str:
    return json.dumps(
        _record_payload(record, include_recorded_at=True, include_record_id=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def deserialize_shadow_trade_record(line: str) -> ShadowTradeRecord:
    try:
        loaded = cast(object, json.loads(line))
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("Shadow journal line is not valid JSON.") from exc
    if not isinstance(loaded, dict):
        raise ValueError("Shadow journal line must contain an object.")
    payload = cast(dict[str, object], loaded)
    try:
        expected_record_id = _string(payload, "record_id")
        events = tuple(_event_from_payload(item) for item in _list(payload, "events"))
        record = ShadowTradeRecord(
            recorded_at=_datetime(payload, "recorded_at"),
            trade_id=_string(payload, "trade_id"),
            symbol=_string(payload, "symbol"),
            direction=ShadowDirection(_string(payload, "direction")),
            stage=ShadowTradeStage(_string(payload, "stage")),
            entry=_number(payload, "entry"),
            stop=_number(payload, "stop"),
            tp1=_number(payload, "tp1"),
            tp2=_number(payload, "tp2"),
            score=_number(payload, "score"),
            reasons=_strings(payload, "reasons"),
            warnings=_strings(payload, "warnings"),
            entry_time=_optional_datetime(payload, "entry_time"),
            exit_time=_optional_datetime(payload, "exit_time"),
            outcome=ShadowOutcomeStatus(_string(payload, "outcome")),
            result_r=_number(payload, "result_r"),
            mfe=_number(payload, "mfe"),
            mae=_number(payload, "mae"),
            events=events,
            schema_version=_integer(payload, "schema_version"),
        )
        if record.record_id != expected_record_id:
            raise ValueError("Shadow journal record id does not match its content.")
        return record
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Shadow journal line has an invalid schema.") from exc


def recover_shadow_journal(path: Path) -> ShadowJournalRecovery:
    resolved = path.resolve()
    if not resolved.exists():
        return ShadowJournalRecovery(records=(), corrupted_line_numbers=(), warnings=())
    records: list[ShadowTradeRecord] = []
    seen: set[str] = set()
    corrupted: list[int] = []
    warnings: list[str] = []
    with resolved.open("rb") as stream:
        for line_number, raw_line in enumerate(stream, start=1):
            if not raw_line.strip():
                continue
            try:
                line = raw_line.decode("utf-8")
                record = deserialize_shadow_trade_record(line)
            except (UnicodeError, ValueError) as exc:
                corrupted.append(line_number)
                warnings.append(
                    f"Skipped corrupted shadow journal line {line_number}: {exc}"
                )
                continue
            if record.record_id in seen:
                continue
            seen.add(record.record_id)
            records.append(record)
    return ShadowJournalRecovery(
        records=tuple(records),
        corrupted_line_numbers=tuple(corrupted),
        warnings=tuple(warnings),
    )


def _record_payload(
    record: ShadowTradeRecord,
    *,
    include_recorded_at: bool,
    include_record_id: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "direction": record.direction.value,
        "entry": record.entry,
        "entry_time": _iso(record.entry_time),
        "events": [_event_payload(event) for event in record.events],
        "exit_time": _iso(record.exit_time),
        "mae": record.mae,
        "mfe": record.mfe,
        "outcome": record.outcome.value,
        "reasons": list(record.reasons),
        "result_r": record.result_r,
        "schema_version": record.schema_version,
        "score": record.score,
        "stage": record.stage.value,
        "stop": record.stop,
        "symbol": record.symbol,
        "tp1": record.tp1,
        "tp2": record.tp2,
        "trade_id": record.trade_id,
        "warnings": list(record.warnings),
    }
    if include_recorded_at:
        payload["recorded_at"] = record.recorded_at.isoformat()
    if include_record_id:
        payload["record_id"] = record.record_id
    return payload


def _event_payload(event: ShadowRuntimeEvent) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type.value,
        "message": event.message,
        "occurred_at": event.occurred_at.isoformat(),
        "price": event.price,
        "realized_r": event.realized_r,
        "stage": event.stage.value,
        "symbol": event.symbol,
        "trade_id": event.trade_id,
    }


def _event_from_payload(value: object) -> ShadowRuntimeEvent:
    if not isinstance(value, dict):
        raise ValueError("Shadow journal event must be an object.")
    payload = cast(dict[str, object], value)
    return ShadowRuntimeEvent(
        event_id=_string(payload, "event_id"),
        event_type=ShadowRuntimeEventType(_string(payload, "event_type")),
        occurred_at=_datetime(payload, "occurred_at"),
        trade_id=_string(payload, "trade_id"),
        symbol=_string(payload, "symbol"),
        stage=ShadowTradeStage(_string(payload, "stage")),
        price=_optional_number(payload, "price"),
        realized_r=_number(payload, "realized_r"),
        message=_string(payload, "message"),
    )


def _record_id(record: ShadowTradeRecord) -> str:
    canonical = json.dumps(
        _record_payload(record, include_recorded_at=False, include_record_id=False),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _string(payload: dict[str, object], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Shadow journal {key} must be a string.")
    return value


def _number(payload: dict[str, object], key: str) -> float:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Shadow journal {key} must be numeric.")
    return float(value)


def _optional_number(payload: dict[str, object], key: str) -> float | None:
    return None if payload[key] is None else _number(payload, key)


def _integer(payload: dict[str, object], key: str) -> int:
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Shadow journal {key} must be an integer.")
    return value


def _list(payload: dict[str, object], key: str) -> list[object]:
    value = payload[key]
    if not isinstance(value, list):
        raise ValueError(f"Shadow journal {key} must be a list.")
    return cast(list[object], value)


def _strings(payload: dict[str, object], key: str) -> tuple[str, ...]:
    values = _list(payload, key)
    if any(not isinstance(value, str) for value in values):
        raise ValueError(f"Shadow journal {key} must contain strings.")
    return tuple(cast(str, value) for value in values)


def _datetime(payload: dict[str, object], key: str) -> datetime:
    value = _string(payload, key)
    return _utc(key, datetime.fromisoformat(value))


def _optional_datetime(
    payload: dict[str, object],
    key: str,
) -> datetime | None:
    return None if payload[key] is None else _datetime(payload, key)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _texts(name: str, values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(value.strip() for value in values)
    if any(not value for value in normalized):
        raise ValueError(f"Shadow journal {name} cannot be empty.")
    return tuple(dict.fromkeys(normalized))


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value.strip() for value in values if value.strip()))


def _utc(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"Shadow journal {name} must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError(f"Shadow journal {name} must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )
