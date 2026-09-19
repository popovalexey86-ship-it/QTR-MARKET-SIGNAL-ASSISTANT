from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime

from market_signal_assistant.watchdog.aggregation import SequenceContext
from market_signal_assistant.watchdog.engine import WatchdogDetectionResult
from market_signal_assistant.watchdog.models import (
    AnomalyContribution,
    AnomalyType,
    FeatureObservation,
    WatchdogState,
    watchdog_event_id,
)


@dataclass(frozen=True, slots=True)
class WatchdogEventEvidence:
    event_id: str
    symbol: str
    event_time: datetime
    detected_at: datetime
    available_at: datetime
    state_before: WatchdogState
    state_after: WatchdogState
    anomaly_score: float
    anomaly_types: tuple[AnomalyType, ...]
    contributors: tuple[AnomalyContribution, ...]
    reasons: tuple[str, ...]
    features: tuple[FeatureObservation, ...]
    baseline_sample_counts: tuple[tuple[str, int], ...]
    missing_data: tuple[str, ...]
    stale_data: tuple[str, ...]
    sequence_context: SequenceContext
    price_at_detection: float
    universe_tier: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.symbol.strip():
            raise ValueError("Event evidence identity is required.")
        if self.event_id != watchdog_event_id(self.symbol, self.detected_at):
            raise ValueError("Event evidence ID is not deterministic.")
        if not math.isfinite(self.price_at_detection) or self.price_at_detection <= 0:
            raise ValueError("Detection price must be finite and positive.")
        if not math.isfinite(self.anomaly_score) or not 0 <= self.anomaly_score <= 100:
            raise ValueError("Evidence anomaly score must be between 0 and 100.")
        event_time = _utc(self.event_time)
        available_at = _utc(self.available_at)
        detected_at = _utc(self.detected_at)
        if event_time > available_at or available_at > detected_at:
            raise ValueError("Event evidence violates PIT chronology.")
        if not self.anomaly_types or not self.reasons:
            raise ValueError("Event evidence requires anomalies and reasons.")
        if any(item.available_at > detected_at for item in self.features):
            raise ValueError("Event evidence contains future features.")
        if any(count < 0 for _, count in self.baseline_sample_counts):
            raise ValueError("Baseline sample count cannot be negative.")
        if not self.universe_tier.strip() or self.schema_version != 1:
            raise ValueError("Event evidence metadata is invalid.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "detected_at", detected_at)

    @classmethod
    def from_detection(
        cls,
        result: WatchdogDetectionResult,
        *,
        price_at_detection: float,
        universe_tier: str,
    ) -> WatchdogEventEvidence:
        event = result.event
        if event is None:
            raise ValueError("Detection result does not contain a Watchdog event.")
        return cls(
            event_id=event.event_id,
            symbol=event.symbol,
            event_time=event.event_time,
            detected_at=event.detected_at,
            available_at=event.available_at,
            state_before=event.previous_state,
            state_after=event.state,
            anomaly_score=event.anomaly_score,
            anomaly_types=tuple(
                dict.fromkeys(item.anomaly_type for item in event.anomalies)
            ),
            contributors=event.contributors,
            reasons=event.reasons,
            features=result.snapshot.features,
            baseline_sample_counts=result.snapshot.baseline_sample_counts,
            missing_data=event.missing_data,
            stale_data=result.snapshot.stale_data,
            sequence_context=result.aggregation.sequence_context,
            price_at_detection=price_at_detection,
            universe_tier=universe_tier,
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Evidence time must be timezone-aware.")
    return value.astimezone(UTC)
