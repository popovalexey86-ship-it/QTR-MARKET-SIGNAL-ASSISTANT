from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum


@dataclass(frozen=True, slots=True)
class ListingStatus:
    symbol: str
    first_seen: datetime
    is_new_listing: bool
    listing_bonus: float

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("Listing symbol cannot be empty.")
        if self.first_seen.tzinfo is None or self.first_seen.utcoffset() is None:
            raise ValueError("Listing first_seen must be timezone-aware.")
        object.__setattr__(self, "first_seen", self.first_seen.astimezone(UTC))
        if not 0.0 <= self.listing_bonus <= 10.0:
            raise ValueError("Listing bonus must be between 0 and 10.")


@dataclass(frozen=True, slots=True)
class CatalogInstrument:
    symbol: str
    quote_coin: str
    status: str
    turnover_24h: float
    bid: float
    ask: float
    base_coin: str
    settle_coin: str
    contract_type: str
    symbol_type: str
    is_pre_listing: bool

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.symbol,
                self.base_coin,
                self.quote_coin,
                self.settle_coin,
                self.contract_type,
                self.status,
            )
        ):
            raise ValueError("Required catalog metadata cannot be empty.")
        if not isinstance(self.is_pre_listing, bool):
            raise ValueError("Catalog pre-listing flag must be boolean.")
        values = (self.turnover_24h, self.bid, self.ask)
        if any(not math.isfinite(value) or value < 0 for value in values):
            raise ValueError("Catalog market values must be finite and non-negative.")

    @property
    def spread_ratio(self) -> float:
        midpoint = (self.bid + self.ask) / 2.0
        if midpoint <= 0 or self.ask < self.bid:
            return 1.0
        return (self.ask - self.bid) / midpoint

    @property
    def is_crypto_linear_usdt(self) -> bool:
        return (
            self.quote_coin.upper() == "USDT"
            and self.settle_coin.upper() == "USDT"
            and self.contract_type == "LinearPerpetual"
            and self.symbol_type.lower() in {"", "innovation"}
            and not self.is_pre_listing
        )


class InPlayDirection(Enum):
    LONG = "ЛОНГ"
    SHORT = "ШОРТ"
    WATCH = "НАБЛЮДЕНИЕ"


@dataclass(frozen=True, slots=True)
class InPlayResult:
    symbol: str
    direction: InPlayDirection
    inplay_score: float
    directional_score: float | None
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    first_seen: datetime
    is_new_listing: bool = False
    listing_bonus: float = 0.0

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("IN PLAY symbol cannot be empty.")
        if not 0.0 <= self.inplay_score <= 100.0:
            raise ValueError("IN PLAY score must be between 0 and 100.")
        if self.directional_score is not None and not (
            -100.0 <= self.directional_score <= 100.0
        ):
            raise ValueError("Directional score must be between -100 and 100.")
        if not self.reasons:
            raise ValueError("IN PLAY result requires explanations.")
        if self.first_seen.tzinfo is None or self.first_seen.utcoffset() is None:
            raise ValueError("IN PLAY first_seen must be timezone-aware.")
        object.__setattr__(self, "first_seen", self.first_seen.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class InPlayReport:
    generated_at: datetime
    results: tuple[InPlayResult, ...]

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("IN PLAY report timestamp must be timezone-aware.")
        object.__setattr__(self, "generated_at", self.generated_at.astimezone(UTC))
