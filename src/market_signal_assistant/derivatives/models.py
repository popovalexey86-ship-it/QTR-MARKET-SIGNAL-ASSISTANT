from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum


class MarketPositioning(Enum):
    SUSTAINABLE_GROWTH = "sustainable_growth"
    OVERHEATED_LONG = "overheated_long"
    SHORT_ACCUMULATION = "short_accumulation"
    SHORT_SQUEEZE = "short_squeeze"
    LONG_SQUEEZE = "long_squeeze"
    UNCONFIRMED_MOVE = "unconfirmed_move"
    NEUTRAL = "neutral"


@dataclass(frozen=True, slots=True)
class DerivativesSnapshot:
    """Normalized point-in-time derivatives observations.

    Rates and changes are decimal fractions; liquidation values are quote
    currency notionals accumulated over the provider's configured window.
    """

    provider: str
    symbol: str
    as_of: datetime
    funding_rate: float
    open_interest: float
    open_interest_change: float
    price_change: float
    volume_change: float
    long_liquidations: float = 0.0
    short_liquidations: float = 0.0

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.symbol.strip():
            raise ValueError("Derivatives provider and symbol cannot be empty.")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None:
            raise ValueError("Derivatives timestamp must be timezone-aware.")
        object.__setattr__(self, "as_of", self.as_of.astimezone(UTC))
        values = (
            self.funding_rate,
            self.open_interest,
            self.open_interest_change,
            self.price_change,
            self.volume_change,
            self.long_liquidations,
            self.short_liquidations,
        )
        if any(isinstance(value, bool) or not math.isfinite(value) for value in values):
            raise ValueError("Derivatives observations must be finite numbers.")
        if self.open_interest < 0:
            raise ValueError("Open interest cannot be negative.")
        if self.long_liquidations < 0 or self.short_liquidations < 0:
            raise ValueError("Liquidation notionals cannot be negative.")


@dataclass(frozen=True, slots=True)
class MarketPositioningSignal:
    regime: MarketPositioning
    directional_score: float
    confidence: float
    snapshot: DerivativesSnapshot
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not -1.0 <= self.directional_score <= 1.0:
            raise ValueError("Directional score must be between -1 and 1.")
        if not 0.0 <= self.confidence <= 100.0:
            raise ValueError("Derivatives confidence must be between 0 and 100.")
        if not self.reasons:
            raise ValueError("Market positioning signal requires explanations.")
