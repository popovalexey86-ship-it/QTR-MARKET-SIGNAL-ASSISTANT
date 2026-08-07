from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum

from market_signal_assistant.derivatives.models import MarketPositioningSignal
from market_signal_assistant.models import Instrument, MarketSignal
from market_signal_assistant.signals.fusion import EnrichedMarketSignal

SUPPORTED_INTERVALS = frozenset({"5m", "15m", "1h", "4h", "1d"})


class ScreeningDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


class ScreeningWarningCode(Enum):
    TECHNICAL_CONFLICT = "technical_conflict"
    DERIVATIVES_UNAVAILABLE = "derivatives_unavailable"
    DERIVATIVES_WEAKENED = "derivatives_weakened"
    OI_UNCONFIRMED = "oi_unconfirmed"
    OVERHEATED_LONG = "overheated_long"
    LIVE_LIQUIDATIONS_INACTIVE = "live_liquidations_inactive"


@dataclass(frozen=True, slots=True)
class ScreeningWarning:
    code: ScreeningWarningCode
    message: str

    def __post_init__(self) -> None:
        if not self.message.strip():
            raise ValueError("Screening warning message cannot be empty.")


@dataclass(frozen=True, slots=True)
class ScreeningRequest:
    instruments: tuple[Instrument, ...]
    interval: str = "1h"
    minimum_score: float = 45.0
    minimum_confidence: float = 0.0
    include_derivatives: bool = False
    maximum_results: int = 10

    def __post_init__(self) -> None:
        if not self.instruments:
            raise ValueError("At least one instrument is required.")
        normalized = tuple(
            Instrument(item.symbol.strip().upper(), item.asset_class)
            for item in self.instruments
        )
        identities = tuple(
            (item.symbol, item.asset_class) for item in normalized
        )
        if len(set(identities)) != len(identities):
            raise ValueError("Duplicate instruments are not allowed.")
        if self.interval not in SUPPORTED_INTERVALS:
            raise ValueError("Unsupported screening interval.")
        _validate_percentage(self.minimum_score, "minimum_score")
        _validate_percentage(self.minimum_confidence, "minimum_confidence")
        if not isinstance(self.include_derivatives, bool):
            raise ValueError("include_derivatives must be boolean.")
        if (
            isinstance(self.maximum_results, bool)
            or not isinstance(self.maximum_results, int)
            or self.maximum_results <= 0
        ):
            raise ValueError("maximum_results must be a positive integer.")
        object.__setattr__(self, "instruments", normalized)
        object.__setattr__(self, "minimum_score", float(self.minimum_score))
        object.__setattr__(
            self, "minimum_confidence", float(self.minimum_confidence)
        )


@dataclass(frozen=True, slots=True)
class InstrumentFailure:
    instrument: Instrument
    stage: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class ScreeningSignalResult:
    instrument: Instrument
    direction: ScreeningDirection
    technical_signal: MarketSignal | None
    derivatives_signal: MarketPositioningSignal | None = None
    fused_signal: EnrichedMarketSignal | None = None
    warnings: tuple[ScreeningWarning, ...] = ()


@dataclass(frozen=True, slots=True)
class MarketSummary:
    total_instruments: int
    successful: int
    failed: int
    long: int
    short: int
    neutral: int


@dataclass(frozen=True, slots=True)
class ScreeningReport:
    generated_at: datetime
    successful_results: tuple[ScreeningSignalResult, ...]
    failed_instruments: tuple[InstrumentFailure, ...]
    ranked_signals: tuple[ScreeningSignalResult, ...]
    market_summary: MarketSummary

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("Report timestamp must be timezone-aware.")
        object.__setattr__(self, "generated_at", self.generated_at.astimezone(UTC))


def _validate_percentage(value: float, field: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0.0 <= value <= 100.0
    ):
        raise ValueError(f"{field} must be between 0 and 100.")
