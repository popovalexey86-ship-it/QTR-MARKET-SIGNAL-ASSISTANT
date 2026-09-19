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
    funding_observed_at: datetime | None = None
    funding_available_at: datetime | None = None
    open_interest_observed_at: datetime | None = None
    open_interest_available_at: datetime | None = None

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
        for prefix in ("funding", "open_interest"):
            observed = getattr(self, f"{prefix}_observed_at")
            available = getattr(self, f"{prefix}_available_at")
            if (observed is None) != (available is None):
                raise ValueError(
                    f"{prefix} provenance requires observed and available times."
                )
            if observed is not None and available is not None:
                normalized_observed = _utc(observed)
                normalized_available = _utc(available)
                if normalized_observed > normalized_available:
                    raise ValueError(
                        f"{prefix} cannot be available before observation."
                    )
                if normalized_available > self.as_of:
                    raise ValueError(f"{prefix} availability cannot follow as_of.")
                object.__setattr__(
                    self, f"{prefix}_observed_at", normalized_observed
                )
                object.__setattr__(
                    self, f"{prefix}_available_at", normalized_available
                )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Derivatives provenance time must be timezone-aware.")
    return value.astimezone(UTC)


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
