from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class AnomalyType(StrEnum):
    COMPRESSION = "COMPRESSION"
    RANGE_BUILDUP = "RANGE_BUILDUP"
    VOLUME_SHOCK = "VOLUME_SHOCK"
    VOLUME_ACCELERATION = "VOLUME_ACCELERATION"
    PRICE_ACCELERATION = "PRICE_ACCELERATION"
    VOLATILITY_EXPANSION = "VOLATILITY_EXPANSION"
    OI_SHOCK = "OI_SHOCK"
    FUNDING_ANOMALY = "FUNDING_ANOMALY"
    LIQUIDATION_EVENT = "LIQUIDATION_EVENT"
    LIQUIDITY_SPREAD_EVENT = "LIQUIDITY_SPREAD_EVENT"
    NEW_MARKET = "NEW_MARKET"
    MULTI_FACTOR_ANOMALY = "MULTI_FACTOR_ANOMALY"


class WatchdogState(StrEnum):
    NORMAL = "NORMAL"
    WATCH = "WATCH"
    IN_PLAY = "IN_PLAY"
    HIGH_ATTENTION = "HIGH_ATTENTION"
    COOLDOWN = "COOLDOWN"


@dataclass(frozen=True, slots=True)
class FeatureObservation:
    name: str
    value: float
    observed_at: datetime
    available_at: datetime
    unit: str = "ratio"
    baseline_value: float | None = None
    normalized_value: float | None = None

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.unit.strip():
            raise ValueError("Feature name and unit are required.")
        _finite("feature value", self.value)
        for name in ("baseline_value", "normalized_value"):
            value = getattr(self, name)
            if value is not None:
                _finite(name, value)
        observed_at = _utc("observed_at", self.observed_at)
        available_at = _utc("available_at", self.available_at)
        if observed_at > available_at:
            raise ValueError("Feature cannot be available before it was observed.")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "available_at", available_at)


@dataclass(frozen=True, slots=True)
class AnomalyContribution:
    anomaly_type: AnomalyType
    base_points: float
    adjusted_points: float
    correlation_group: str
    reason: str

    def __post_init__(self) -> None:
        for name in ("base_points", "adjusted_points"):
            value = getattr(self, name)
            _finite(name, value)
            if not 0.0 <= value <= 100.0:
                raise ValueError(f"{name} must be between 0 and 100.")
        if self.adjusted_points > self.base_points:
            raise ValueError("Adjusted contribution cannot exceed base points.")
        if not self.correlation_group.strip() or not self.reason.strip():
            raise ValueError("Contribution group and reason are required.")


@dataclass(frozen=True, slots=True)
class AnomalyObservation:
    anomaly_type: AnomalyType
    severity: float
    window: str
    detected_at: datetime
    available_at: datetime
    features: tuple[FeatureObservation, ...]
    reasons: tuple[str, ...]
    missing_data: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _score("severity", self.severity)
        if not self.window.strip() or not self.reasons:
            raise ValueError("Anomaly window and reasons are required.")
        detected_at = _utc("detected_at", self.detected_at)
        available_at = _utc("available_at", self.available_at)
        if available_at > detected_at:
            raise ValueError("Anomaly data must be available by detection time.")
        if any(item.available_at > detected_at for item in self.features):
            raise ValueError("Anomaly contains a future feature.")
        object.__setattr__(self, "detected_at", detected_at)
        object.__setattr__(self, "available_at", available_at)


@dataclass(frozen=True, slots=True)
class WatchdogEvent:
    event_id: str
    symbol: str
    event_time: datetime
    available_at: datetime
    detected_at: datetime
    previous_state: WatchdogState
    state: WatchdogState
    anomaly_score: float
    anomalies: tuple[AnomalyObservation, ...]
    features: tuple[FeatureObservation, ...]
    reasons: tuple[str, ...]
    missing_data: tuple[str, ...]
    contributors: tuple[AnomalyContribution, ...] = ()
    source: str = "qtr-market-watchdog"
    version: str = "1"
    config_version: str = "phase1"

    def __post_init__(self) -> None:
        _identity(self.event_id, self.symbol, self.source, self.version)
        _score("anomaly_score", self.anomaly_score)
        if not self.anomalies or not self.reasons:
            raise ValueError("Watchdog event requires anomalies and reasons.")
        event_time = _utc("event_time", self.event_time)
        available_at = _utc("available_at", self.available_at)
        detected_at = _utc("detected_at", self.detected_at)
        if event_time > available_at or available_at > detected_at:
            raise ValueError("Watchdog event violates PIT chronology.")
        if any(
            item.available_at > detected_at or item.detected_at > detected_at
            for item in self.anomalies
        ):
            raise ValueError("Watchdog event contains a future anomaly.")
        if any(item.available_at > detected_at for item in self.features):
            raise ValueError("Watchdog event contains a future feature.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "detected_at", detected_at)


@dataclass(frozen=True, slots=True)
class WatchdogCandidate:
    candidate_id: str
    event_id: str
    symbol: str
    detected_at: datetime
    available_at: datetime
    state: WatchdogState
    anomaly_types: tuple[AnomalyType, ...]
    anomaly_score: float
    features: tuple[FeatureObservation, ...]
    liquidity_quality: float | None
    execution_quality: float | None
    reasons: tuple[str, ...]
    missing_data: tuple[str, ...]
    contributors: tuple[AnomalyContribution, ...] = ()
    source: str = "qtr-market-watchdog"
    version: str = "1"

    def __post_init__(self) -> None:
        _identity(self.candidate_id, self.event_id, self.symbol, self.source)
        _score("anomaly_score", self.anomaly_score)
        for name in ("liquidity_quality", "execution_quality"):
            value = getattr(self, name)
            if value is not None:
                _score(name, value)
        if not self.anomaly_types or not self.reasons:
            raise ValueError("Watchdog candidate requires anomalies and reasons.")
        detected_at = _utc("detected_at", self.detected_at)
        available_at = _utc("available_at", self.available_at)
        if available_at > detected_at:
            raise ValueError("Candidate data must be available by detection time.")
        if any(item.available_at > detected_at for item in self.features):
            raise ValueError("Watchdog candidate contains a future feature.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "detected_at", detected_at)
        object.__setattr__(self, "available_at", available_at)

    @classmethod
    def from_event(cls, event: WatchdogEvent) -> WatchdogCandidate:
        anomaly_types = tuple(
            dict.fromkeys(item.anomaly_type for item in event.anomalies)
        )
        return cls(
            candidate_id=f"watchdog:{event.event_id}",
            event_id=event.event_id,
            symbol=event.symbol,
            detected_at=event.detected_at,
            available_at=event.available_at,
            state=event.state,
            anomaly_types=anomaly_types,
            anomaly_score=event.anomaly_score,
            features=event.features,
            liquidity_quality=None,
            execution_quality=None,
            reasons=event.reasons,
            missing_data=event.missing_data,
            contributors=event.contributors,
            source=event.source,
            version=event.version,
        )


def watchdog_event_id(symbol: str, detected_at: datetime) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("Watchdog event symbol cannot be empty.")
    timestamp = _utc("detected_at", detected_at).strftime("%Y%m%dT%H%M%S.%fZ")
    return f"watchdog:{normalized}:{timestamp}"


def _identity(*values: str) -> None:
    if any(not value.strip() for value in values):
        raise ValueError("Watchdog identifiers cannot be empty.")


def _score(name: str, value: float) -> None:
    _finite(name, value)
    if not 0.0 <= value <= 100.0:
        raise ValueError(f"{name} must be between 0 and 100.")


def _finite(name: str, value: float) -> None:
    if isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"{name} must be finite.")


def _utc(name: str, value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return value.astimezone(UTC)
