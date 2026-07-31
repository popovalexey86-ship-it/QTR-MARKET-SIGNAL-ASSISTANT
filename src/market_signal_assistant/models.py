from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum


class AssetClass(Enum):
    CRYPTO = "crypto"
    STOCK = "stock"
    FUND = "fund"
    FOREX = "forex"


class SignalDirection(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


@dataclass(frozen=True, slots=True)
class Instrument:
    symbol: str
    asset_class: AssetClass

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("Instrument symbol cannot be empty.")


@dataclass(frozen=True, slots=True)
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("Candle timestamp must be timezone-aware.")
        object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))
        values = (self.open, self.high, self.low, self.close, self.volume)
        if any(not math.isfinite(value) for value in values):
            raise ValueError("Candle values must be finite.")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("OHLC prices must be positive.")
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("Candle high is inconsistent with OHLC values.")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("Candle low is inconsistent with OHLC values.")
        if self.volume < 0:
            raise ValueError("Candle volume cannot be negative.")


@dataclass(frozen=True, slots=True)
class MarketSeries:
    instrument: Instrument
    interval: str
    candles: tuple[Candle, ...]

    def __post_init__(self) -> None:
        if not self.interval.strip():
            raise ValueError("Market interval cannot be empty.")
        if not self.candles:
            raise ValueError("Market series cannot be empty.")
        for previous, current in zip(self.candles, self.candles[1:], strict=False):
            if current.timestamp <= previous.timestamp:
                raise ValueError(
                    "Candles must have unique ascending timestamps."
                )


@dataclass(frozen=True, slots=True)
class SignalEvidence:
    name: str
    direction: SignalDirection
    weight: float
    detail: str

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.detail.strip():
            raise ValueError("Signal evidence must be explained.")
        if not math.isfinite(self.weight) or self.weight <= 0:
            raise ValueError("Signal evidence weight must be positive.")


@dataclass(frozen=True, slots=True)
class MarketSignal:
    instrument: Instrument
    interval: str
    timestamp: datetime
    direction: SignalDirection
    score: float
    confidence: float
    confirmations: int
    conflicts: int
    price: float
    evidence: tuple[SignalEvidence, ...]

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("Signal timestamp must be timezone-aware.")
        object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))
        if not 0 <= self.score <= 100:
            raise ValueError("Signal score must be between 0 and 100.")
        if not 0 <= self.confidence <= 100:
            raise ValueError("Signal confidence must be between 0 and 100.")
        if self.confirmations <= 0 or self.conflicts < 0:
            raise ValueError("Signal evidence counts are invalid.")


@dataclass(frozen=True, slots=True)
class ScreeningFailure:
    instrument: Instrument
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class ScreeningResult:
    generated_at: datetime
    signals: tuple[MarketSignal, ...]
    no_signal: tuple[Instrument, ...]
    failures: tuple[ScreeningFailure, ...]

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError("Screening timestamp must be timezone-aware.")
