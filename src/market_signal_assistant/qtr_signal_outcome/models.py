from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class OutcomeStatus(StrEnum):
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"
    FAILED_MARKET_DATA = "FAILED_MARKET_DATA"


class BarrierOrder(StrEnum):
    FAVORABLE_FIRST = "FAVORABLE_FIRST"
    ADVERSE_FIRST = "ADVERSE_FIRST"
    NEITHER = "NEITHER"
    AMBIGUOUS_SAME_CANDLE = "AMBIGUOUS_SAME_CANDLE"


@dataclass(frozen=True, slots=True)
class SignalSnapshot:
    signal_id: str
    symbol: str
    direction: Direction
    setup_type: str
    signal_timestamp: datetime
    source_observed_at: datetime
    semantic_fingerprint: str
    signal_price: float
    trigger_price: float | None
    invalidation_price: float | None
    atr: float
    setup_confidence: float | None
    telegram_quality_score: float | None
    quality_components: tuple[tuple[str, float], ...]
    volume_confirmation: bool | None
    volatility_confirmation: bool | None
    liquidity_ok: bool | None
    confirmations: tuple[str, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not self.signal_id.strip() or not symbol or not self.setup_type.strip():
            raise ValueError("Signal identity fields cannot be empty.")
        if self.signal_price <= 0 or not math.isfinite(self.signal_price):
            raise ValueError("Signal price must be finite and positive.")
        if self.atr <= 0 or not math.isfinite(self.atr):
            raise ValueError("Signal ATR must be finite and positive.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "signal_timestamp", _utc(self.signal_timestamp))
        object.__setattr__(self, "source_observed_at", _utc(self.source_observed_at))
        object.__setattr__(
            self,
            "quality_components",
            tuple(sorted(self.quality_components)),
        )


@dataclass(frozen=True, slots=True)
class MarketCandle:
    symbol: str
    opened_at: datetime
    closed_at: datetime
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        opened = _utc(self.opened_at)
        closed = _utc(self.closed_at)
        prices = (self.open, self.high, self.low, self.close)
        if not symbol or any(
            not math.isfinite(value) or value <= 0 for value in prices
        ):
            raise ValueError("Candle symbol and OHLC must be valid.")
        if closed <= opened or self.high < max(self.open, self.close, self.low):
            raise ValueError("Candle high is inconsistent.")
        if self.low > min(self.open, self.close, self.high):
            raise ValueError("Candle low is inconsistent.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "opened_at", opened)
        object.__setattr__(self, "closed_at", closed)


@dataclass(frozen=True, slots=True)
class HorizonOutcome:
    horizon_minutes: int
    close_price: float | None
    directional_close_return_pct: float | None
    directional_close_return_atr: float | None
    mfe_price: float | None
    mae_price: float | None
    mfe_atr: float | None
    mae_atr: float | None


@dataclass(frozen=True, slots=True)
class BarrierHit:
    threshold_atr: float
    hit: bool
    first_hit_timestamp: datetime | None
    first_hit_minutes_from_signal: float | None


@dataclass(frozen=True, slots=True)
class BarrierPairOutcome:
    favorable_atr: float
    adverse_atr: float
    order: BarrierOrder


@dataclass(frozen=True, slots=True)
class SignalOutcome:
    signal: SignalSnapshot
    status: OutcomeStatus
    analyzed_at: datetime
    analyzed_through: datetime | None
    maximum_horizon_minutes: int
    horizons: tuple[HorizonOutcome, ...]
    favorable_barriers: tuple[BarrierHit, ...]
    adverse_barriers: tuple[BarrierHit, ...]
    barrier_orders: tuple[BarrierPairOutcome, ...]
    invalidation_hit: bool
    invalidation_first_hit_timestamp: datetime | None
    invalidation_minutes: float | None
    market_data_error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "analyzed_at", _utc(self.analyzed_at))
        if self.analyzed_through is not None:
            object.__setattr__(self, "analyzed_through", _utc(self.analyzed_through))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware.")
    return value.astimezone(UTC)
