from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import cast

from market_signal_assistant.watchdog.aggregation import SequenceContext, SequenceStep
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.journal import (
    ImmutableJsonlJournal,
    JournalRecovery,
)
from market_signal_assistant.watchdog.models import (
    AnomalyContribution,
    AnomalyType,
    FeatureObservation,
    WatchdogState,
)


class WatchdogEventJournal:
    def __init__(self, path: Path) -> None:
        self._journal = ImmutableJsonlJournal(path, id_field="event_id")
        self._daily_counts: dict[date, int] | None = None

    @property
    def path(self) -> Path:
        return self._journal.path

    @property
    def recovery(self) -> JournalRecovery:
        return self._journal.recovery

    def append(self, event: WatchdogEventEvidence) -> bool:
        created = self._journal.append(event.event_id, _event_payload(event))
        if created and self._daily_counts is not None:
            day = event.detected_at.date()
            self._daily_counts[day] = self._daily_counts.get(day, 0) + 1
        return created

    def records(self) -> tuple[WatchdogEventEvidence, ...]:
        return tuple(_event_from_payload(item) for item in self._journal.records())

    def get(self, event_id: str) -> WatchdogEventEvidence | None:
        payload = self._journal.get(event_id)
        return None if payload is None else _event_from_payload(payload)

    def count_detected_on(self, day: date) -> int:
        if self._daily_counts is None:
            counts: dict[date, int] = {}
            for event in self.records():
                detected_day = event.detected_at.date()
                counts[detected_day] = counts.get(detected_day, 0) + 1
            self._daily_counts = counts
        return self._daily_counts.get(day, 0)


def _event_payload(event: WatchdogEventEvidence) -> dict[str, object]:
    return {
        "schema_version": event.schema_version,
        "event_id": event.event_id,
        "symbol": event.symbol,
        "event_time": event.event_time.isoformat(),
        "detected_at": event.detected_at.isoformat(),
        "available_at": event.available_at.isoformat(),
        "state_before": event.state_before.value,
        "state_after": event.state_after.value,
        "anomaly_score": event.anomaly_score,
        "anomaly_types": [item.value for item in event.anomaly_types],
        "contributors": [
            {
                "anomaly_type": item.anomaly_type.value,
                "base_points": item.base_points,
                "adjusted_points": item.adjusted_points,
                "correlation_group": item.correlation_group,
                "reason": item.reason,
            }
            for item in event.contributors
        ],
        "reasons": list(event.reasons),
        "features": [
            {
                "name": item.name,
                "value": item.value,
                "observed_at": item.observed_at.isoformat(),
                "available_at": item.available_at.isoformat(),
                "unit": item.unit,
                "baseline_value": item.baseline_value,
                "normalized_value": item.normalized_value,
            }
            for item in event.features
        ],
        "baseline_sample_counts": [
            {"feature": feature, "count": count}
            for feature, count in event.baseline_sample_counts
        ],
        "missing_data": list(event.missing_data),
        "stale_data": list(event.stale_data),
        "sequence_context": [
            {
                "anomaly_type": item.anomaly_type.value,
                "detected_at": item.detected_at.isoformat(),
            }
            for item in event.sequence_context.steps
        ],
        "price_at_detection": event.price_at_detection,
        "universe_tier": event.universe_tier,
    }


def _event_from_payload(value: Mapping[str, object]) -> WatchdogEventEvidence:
    return WatchdogEventEvidence(
        event_id=_string(value, "event_id"),
        symbol=_string(value, "symbol"),
        event_time=_datetime(value, "event_time"),
        detected_at=_datetime(value, "detected_at"),
        available_at=_datetime(value, "available_at"),
        state_before=WatchdogState(_string(value, "state_before")),
        state_after=WatchdogState(_string(value, "state_after")),
        anomaly_score=_number(value, "anomaly_score"),
        anomaly_types=tuple(
            AnomalyType(_plain_string(item))
            for item in _sequence(value, "anomaly_types")
        ),
        contributors=tuple(
            _contribution(_mapping(item))
            for item in _sequence(value, "contributors")
        ),
        reasons=tuple(
            _plain_string(item) for item in _sequence(value, "reasons")
        ),
        features=tuple(
            _feature(_mapping(item)) for item in _sequence(value, "features")
        ),
        baseline_sample_counts=tuple(
            (
                _string(_mapping(item), "feature"),
                _integer(_mapping(item), "count"),
            )
            for item in _sequence(value, "baseline_sample_counts")
        ),
        missing_data=tuple(
            _plain_string(item) for item in _sequence(value, "missing_data")
        ),
        stale_data=tuple(
            _plain_string(item) for item in _sequence(value, "stale_data")
        ),
        sequence_context=SequenceContext(
            tuple(
                _step(_mapping(item))
                for item in _sequence(value, "sequence_context")
            )
        ),
        price_at_detection=_number(value, "price_at_detection"),
        universe_tier=_string(value, "universe_tier"),
        schema_version=_integer(value, "schema_version"),
    )


def _contribution(value: Mapping[str, object]) -> AnomalyContribution:
    return AnomalyContribution(
        AnomalyType(_string(value, "anomaly_type")),
        _number(value, "base_points"),
        _number(value, "adjusted_points"),
        _string(value, "correlation_group"),
        _string(value, "reason"),
    )


def _feature(value: Mapping[str, object]) -> FeatureObservation:
    baseline = value.get("baseline_value")
    normalized = value.get("normalized_value")
    return FeatureObservation(
        name=_string(value, "name"),
        value=_number(value, "value"),
        observed_at=_datetime(value, "observed_at"),
        available_at=_datetime(value, "available_at"),
        unit=_string(value, "unit"),
        baseline_value=_nullable_number(baseline),
        normalized_value=_nullable_number(normalized),
    )


def _step(value: Mapping[str, object]) -> SequenceStep:
    return SequenceStep(
        AnomalyType(_string(value, "anomaly_type")),
        _datetime(value, "detected_at"),
    )


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError("Journal field must be an object.")
    return cast(Mapping[str, object], value)


def _sequence(value: Mapping[str, object], field: str) -> list[object]:
    result = value.get(field)
    if not isinstance(result, list):
        raise ValueError(f"Journal field {field} must be a list.")
    return cast(list[object], result)


def _string(value: Mapping[str, object], field: str) -> str:
    return _plain_string(value.get(field))


def _plain_string(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Journal field must be a non-empty string.")
    return value


def _number(value: Mapping[str, object], field: str) -> float:
    raw = value.get(field)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"Journal field {field} must be numeric.")
    return float(raw)


def _nullable_number(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Journal field must be numeric or null.")
    return float(value)


def _integer(value: Mapping[str, object], field: str) -> int:
    raw = value.get(field)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"Journal field {field} must be an integer.")
    return raw


def _datetime(value: Mapping[str, object], field: str) -> datetime:
    return datetime.fromisoformat(_string(value, field))
