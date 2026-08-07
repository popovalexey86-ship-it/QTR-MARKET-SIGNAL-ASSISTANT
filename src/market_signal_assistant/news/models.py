from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum


class NewsAssetType(Enum):
    CRYPTO = "CRYPTO"
    STOCK = "STOCK"
    ETF = "ETF"
    COMMODITY = "COMMODITY"
    FOREX = "FOREX"
    UNKNOWN = "UNKNOWN"


class NewsCategory(Enum):
    LISTING = "LISTING"
    DELISTING = "DELISTING"
    MAINTENANCE = "MAINTENANCE"
    SECURITY = "SECURITY"
    NETWORK = "NETWORK"
    TRADING_CHANGE = "TRADING_CHANGE"
    REGULATION = "REGULATION"
    OTHER = "OTHER"


class NewsImportance(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(frozen=True, slots=True)
class NewsSourceRecord:
    source: str
    title: str
    description: str
    url: str
    type_key: str
    tags: tuple[str, ...]
    published_at: datetime
    event_starts_at: datetime | None
    asset_type: NewsAssetType = NewsAssetType.UNKNOWN
    event_start_date: date | None = None

    def __post_init__(self) -> None:
        if not all((self.source.strip(), self.title.strip())):
            raise ValueError("News source record requires source and title.")
        _require_aware(self.published_at, "published_at")
        object.__setattr__(self, "published_at", self.published_at.astimezone(UTC))
        if self.event_starts_at is not None:
            _require_aware(self.event_starts_at, "event_starts_at")
            object.__setattr__(
                self,
                "event_starts_at",
                self.event_starts_at.astimezone(UTC),
            )
        object.__setattr__(self, "tags", _unique_text(self.tags))


@dataclass(frozen=True, slots=True)
class NewsItem:
    stable_id: str
    source: str
    title: str
    description: str
    url: str
    category: NewsCategory
    importance: NewsImportance
    symbols: tuple[str, ...]
    published_at: datetime
    event_starts_at: datetime | None
    tags: tuple[str, ...]
    reason: str
    recommended_action: str
    asset_type: NewsAssetType = NewsAssetType.UNKNOWN
    event_start_date: date | None = None

    def __post_init__(self) -> None:
        required = (
            self.stable_id,
            self.source,
            self.title,
            self.description,
            self.reason,
            self.recommended_action,
        )
        if not all(value.strip() for value in required):
            raise ValueError("News item required fields cannot be empty.")
        _require_aware(self.published_at, "published_at")
        object.__setattr__(self, "published_at", self.published_at.astimezone(UTC))
        if self.event_starts_at is not None:
            _require_aware(self.event_starts_at, "event_starts_at")
            object.__setattr__(
                self,
                "event_starts_at",
                self.event_starts_at.astimezone(UTC),
            )
        object.__setattr__(
            self,
            "symbols",
            tuple(dict.fromkeys(symbol.strip().upper() for symbol in self.symbols)),
        )
        object.__setattr__(self, "tags", _unique_text(self.tags))


@dataclass(frozen=True, slots=True)
class NewsReport:
    generated_at: datetime
    lookback_hours: int
    items: tuple[NewsItem, ...]

    def __post_init__(self) -> None:
        _require_aware(self.generated_at, "generated_at")
        object.__setattr__(self, "generated_at", self.generated_at.astimezone(UTC))
        if not 1 <= self.lookback_hours <= 168:
            raise ValueError("News lookback must be between 1 and 168 hours.")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"News {name} must be timezone-aware.")


def _unique_text(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value.strip() for value in values if value.strip()))
