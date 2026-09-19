from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

OUTCOME_HORIZONS_MINUTES = (1, 5, 15, 30, 60)


class OutcomeDataQuality(StrEnum):
    ON_TIME = "ON_TIME"
    LATE = "LATE"
    MISSING = "MISSING"


class BreakoutSide(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    BOTH = "BOTH"
    NONE = "NONE"


@dataclass(frozen=True, slots=True)
class PriceObservation:
    symbol: str
    observed_at: datetime
    available_at: datetime
    price: float
    high: float | None = None
    low: float | None = None
    source: str = "injected-market-data"

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.source.strip():
            raise ValueError("Price observation identity is required.")
        observed_at = _utc(self.observed_at)
        available_at = _utc(self.available_at)
        if observed_at > available_at:
            raise ValueError("Price cannot be available before observation.")
        high = self.price if self.high is None else self.high
        low = self.price if self.low is None else self.low
        if any(
            isinstance(value, bool) or not math.isfinite(value) or value <= 0
            for value in (self.price, high, low)
        ):
            raise ValueError("Price observation values must be positive and finite.")
        if high < max(self.price, low) or low > min(self.price, high):
            raise ValueError("Price observation high/low are inconsistent.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "high", high)
        object.__setattr__(self, "low", low)

    @property
    def observation_id(self) -> str:
        identity = "|".join(
            (
                self.symbol,
                self.observed_at.isoformat(),
                self.available_at.isoformat(),
                format(self.price, ".17g"),
                format(self.high or self.price, ".17g"),
                format(self.low or self.price, ".17g"),
                self.source,
            )
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ForwardOutcome:
    outcome_id: str
    event_id: str
    symbol: str
    horizon_minutes: int
    target_time: datetime
    observed_at: datetime
    available_at: datetime
    reference_price: float
    horizon_price: float | None
    signed_return: float | None
    abs_return: float | None
    mfe_up: float | None
    mfe_down: float | None
    max_abs_excursion: float | None
    realized_range_after_event: float | None
    time_to_mfe_up_seconds: float | None
    time_to_mfe_down_seconds: float | None
    lateness_seconds: float
    data_quality: OutcomeDataQuality
    did_expansion_occur: bool | None = None
    time_to_expansion_seconds: float | None = None
    expansion_magnitude: float | None = None
    breakout_side: BreakoutSide | None = None
    false_breakout: bool | None = None
    subsequent_abs_move: float | None = None
    persistence: float | None = None
    reversal: bool | None = None
    volatility_persistence: float | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        if (
            not self.outcome_id.strip()
            or not self.event_id.strip()
            or not self.symbol.strip()
        ):
            raise ValueError("Outcome identity is required.")
        if self.horizon_minutes not in OUTCOME_HORIZONS_MINUTES:
            raise ValueError("Unsupported Watchdog outcome horizon.")
        target = _utc(self.target_time)
        observed = _utc(self.observed_at)
        available = _utc(self.available_at)
        if target > observed or observed > available:
            raise ValueError("Outcome chronology is invalid.")
        if not math.isfinite(self.reference_price) or self.reference_price <= 0:
            raise ValueError("Outcome reference price must be positive and finite.")
        if not math.isfinite(self.lateness_seconds) or self.lateness_seconds < 0:
            raise ValueError("Outcome lateness cannot be negative.")
        metrics = (
            self.horizon_price,
            self.signed_return,
            self.abs_return,
            self.mfe_up,
            self.mfe_down,
            self.max_abs_excursion,
            self.realized_range_after_event,
        )
        if self.data_quality is OutcomeDataQuality.MISSING:
            if any(item is not None for item in metrics):
                raise ValueError("Missing outcome cannot contain movement metrics.")
        elif any(item is None for item in metrics):
            raise ValueError("Observed outcome requires movement metrics.")
        timed_metrics = (
            *metrics,
            self.time_to_mfe_up_seconds,
            self.time_to_mfe_down_seconds,
        )
        for value in timed_metrics:
            if value is not None and (
                isinstance(value, bool) or not math.isfinite(value)
            ):
                raise ValueError("Outcome metrics must be finite.")
        if self.schema_version != 1:
            raise ValueError("Unsupported outcome schema version.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "target_time", target)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)

    @property
    def mfe(self) -> float | None:
        return self.mfe_up

    @property
    def mae(self) -> float | None:
        return self.mfe_down

    @property
    def max_abs_move(self) -> float | None:
        return self.max_abs_excursion


def outcome_id(event_id: str, horizon_minutes: int) -> str:
    identity = f"{event_id}|{horizon_minutes}m"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def target_time(detected_at: datetime, horizon_minutes: int) -> datetime:
    if horizon_minutes not in OUTCOME_HORIZONS_MINUTES:
        raise ValueError("Unsupported Watchdog outcome horizon.")
    return _utc(detected_at) + timedelta(minutes=horizon_minutes)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Outcome time must be timezone-aware.")
    return value.astimezone(UTC)
