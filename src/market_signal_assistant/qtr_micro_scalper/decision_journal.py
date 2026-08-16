from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
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


class ShadowDecisionJournal:
    """Thread-safe, restart-safe append-only decision event journal."""

    def __init__(self, path: Path = DEFAULT_DECISION_JOURNAL_PATH) -> None:
        self._path = path.resolve()
        self._lock = Lock()
        recovery = recover_decision_journal(self._path)
        self._records = list(recovery.records)
        self._event_ids = {record.event_id for record in recovery.records}
        self._recovery = recovery

    @property
    def path(self) -> Path:
        return self._path

    @property
    def recovery(self) -> DecisionJournalRecovery:
        return self._recovery

    def append(self, record: ShadowDecisionRecord) -> bool:
        line = serialize_decision_record(record)
        with self._lock:
            if record.event_id in self._event_ids:
                return False
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(line)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._event_ids.add(record.event_id)
            self._records.append(record)
            return True

    def records(self) -> tuple[ShadowDecisionRecord, ...]:
        with self._lock:
            return tuple(self._records)

    def flush(self) -> None:
        with self._lock:
            if not self._path.exists():
                return
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.flush()
                os.fsync(stream.fileno())


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
    corrupted: list[int] = []
    warnings: list[str] = []
    for line_number, raw_line in enumerate(resolved.read_bytes().splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            records.append(deserialize_decision_record(raw_line.decode("utf-8")))
        except (UnicodeDecodeError, ValueError) as exc:
            corrupted.append(line_number)
            warnings.append(f"Decision journal line {line_number} ignored: {exc}")
    unique: dict[str, ShadowDecisionRecord] = {}
    for record in records:
        unique.setdefault(record.event_id, record)
    return DecisionJournalRecovery(
        records=tuple(unique.values()),
        corrupted_line_numbers=tuple(corrupted),
        warnings=tuple(warnings),
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
