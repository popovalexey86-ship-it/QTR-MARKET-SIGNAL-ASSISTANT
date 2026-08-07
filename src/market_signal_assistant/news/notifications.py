from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from market_signal_assistant.news.models import (
    NewsCategory,
    NewsImportance,
    NewsItem,
)

DEFAULT_NEWS_NOTIFICATION_STATE_PATH = Path("data/news_notifications.json")
DEFAULT_NEWS_NOTIFICATION_RETENTION_DAYS = 30

_LOGGER = logging.getLogger(__name__)
_SCHEMA = "market_signal_assistant.news_notifications"
_IMPORTANCE_RANK = {
    NewsImportance.LOW: 0,
    NewsImportance.MEDIUM: 1,
    NewsImportance.HIGH: 2,
    NewsImportance.CRITICAL: 3,
}
_PROMOTION_MARKERS = (
    "prize pool",
    "trade to share",
    "trade and share",
    "competition",
    "campaign",
    "bonus",
    "launchpool",
    "launch pool",
    " earn ",
    "vip",
    "referral",
    "ama",
    "copy trading",
    "holiday",
    "rewards",
)
_CANCELLATION_MARKERS = (
    "cancelled",
    "canceled",
    "cancellation",
    "will not proceed",
    "event withdrawn",
    "отменено",
    "отмена",
)
_EMERGENCY_MARKERS = (
    "emergency",
    "urgent",
    "effective immediately",
    "unexpected suspension",
    "services suspended",
    "security incident",
    "hack",
    "exploit",
    "экстрен",
    "аварийн",
)
_NON_WORD = re.compile(r"[^\w]+", flags=re.UNICODE)
_PROMO_SUFFIX = re.compile(r"\s+[—–|]\s+")
_PROMO_NUMBER = re.compile(r"\b\d+(?:[.,]\d+)?\s*[km]?\b", re.IGNORECASE)


class NewsNotificationStateError(RuntimeError):
    """Controlled local news-notification persistence failure."""


class NewsNotificationStatus(Enum):
    SENT = "SENT"
    UPDATED = "UPDATED"
    CANCELLED = "CANCELLED"
    DOWNGRADED = "DOWNGRADED"


class NewsNotificationDecisionKind(Enum):
    INITIAL = "INITIAL"
    UPDATED = "UPDATED"
    CANCELLED = "CANCELLED"
    SUPPRESSED = "SUPPRESSED"
    DOWNGRADED = "DOWNGRADED"
    INELIGIBLE = "INELIGIBLE"


@dataclass(frozen=True, slots=True)
class NewsNotificationRecord:
    stable_id: str
    source: str
    category: NewsCategory
    importance: NewsImportance
    title_fingerprint: str
    content_fingerprint: str
    symbols: tuple[str, ...]
    published_at: datetime
    event_starts_at: datetime | None
    first_sent_at: datetime
    last_sent_at: datetime
    last_seen_at: datetime
    send_count: int
    status: NewsNotificationStatus
    emergency: bool = False

    def __post_init__(self) -> None:
        if not self.stable_id.strip() or not self.source.strip():
            raise ValueError("News notification identity cannot be empty.")
        for name, value in (
            ("published_at", self.published_at),
            ("first_sent_at", self.first_sent_at),
            ("last_sent_at", self.last_sent_at),
            ("last_seen_at", self.last_seen_at),
        ):
            _require_aware(value, name)
            object.__setattr__(self, name, value.astimezone(UTC))
        if self.event_starts_at is not None:
            _require_aware(self.event_starts_at, "event_starts_at")
            object.__setattr__(
                self,
                "event_starts_at",
                self.event_starts_at.astimezone(UTC),
            )
        if self.send_count <= 0:
            raise ValueError("News notification send_count must be positive.")
        object.__setattr__(self, "symbols", _symbols(self.symbols))


@dataclass(frozen=True, slots=True)
class NewsNotificationState:
    updated_at: datetime | None
    records: Mapping[str, NewsNotificationRecord]

    def __post_init__(self) -> None:
        if self.updated_at is not None:
            _require_aware(self.updated_at, "updated_at")
            object.__setattr__(self, "updated_at", self.updated_at.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class NewsNotificationDecision:
    item: NewsItem
    should_notify: bool
    kind: NewsNotificationDecisionKind


@dataclass(frozen=True, slots=True)
class NewsNotificationPlan:
    observed_at: datetime
    decisions: tuple[NewsNotificationDecision, ...]
    original_records: Mapping[str, NewsNotificationRecord]
    passive_records: Mapping[str, NewsNotificationRecord]


class JsonNewsNotificationStore:
    def __init__(
        self,
        path: Path = DEFAULT_NEWS_NOTIFICATION_STATE_PATH,
    ) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> NewsNotificationState:
        if not self._path.exists():
            empty = NewsNotificationState(None, {})
            self.save(empty)
            return empty
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
            return _state_from_json(payload)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            _LOGGER.warning(
                "Файл состояния новостных уведомлений повреждён; "
                "используется безопасное пустое состояние."
            )
            empty = NewsNotificationState(None, {})
            try:
                self.save(empty)
            except NewsNotificationStateError:
                _LOGGER.warning(
                    "Не удалось перезаписать повреждённый файл состояния "
                    "новостных уведомлений."
                )
            return empty

    def save(self, state: NewsNotificationState) -> None:
        payload = {
            "schema": _SCHEMA,
            "version": 1,
            "updated_at": (
                state.updated_at.isoformat() if state.updated_at is not None else None
            ),
            "records": {
                stable_id: _record_to_json(record)
                for stable_id, record in sorted(state.records.items())
            },
        }
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(self._path)
        except OSError:
            raise NewsNotificationStateError(
                "Local news notification state cannot be saved."
            ) from None


class NewsNotificationService:
    def __init__(
        self,
        store: JsonNewsNotificationStore,
        *,
        retention_days: int = DEFAULT_NEWS_NOTIFICATION_RETENTION_DAYS,
        lookback_hours: int = 24,
    ) -> None:
        if not 7 <= retention_days <= 365:
            raise ValueError("News notification retention must be from 7 to 365 days.")
        if not 1 <= lookback_hours <= 168:
            raise ValueError("News notification lookback must be from 1 to 168 hours.")
        self._store = store
        self._retention = timedelta(days=retention_days)
        self._lookback = timedelta(hours=lookback_hours)

    def prepare(
        self,
        items: tuple[NewsItem, ...],
        observed_at: datetime,
    ) -> NewsNotificationPlan:
        _require_aware(observed_at, "observed_at")
        now = observed_at.astimezone(UTC)
        state = self._store.load()
        original = _prune_records(dict(state.records), now, self._retention)
        passive = dict(original)
        decisions: list[NewsNotificationDecision] = []
        for item in _unique_items(items):
            previous = original.get(item.stable_id)
            decision = self._decision(item, previous, now)
            decisions.append(decision)
            if previous is not None:
                if decision.kind is NewsNotificationDecisionKind.DOWNGRADED:
                    passive[item.stable_id] = _passive_record(
                        item,
                        previous,
                        now,
                        status=NewsNotificationStatus.DOWNGRADED,
                        update_content=True,
                    )
                else:
                    passive[item.stable_id] = replace(previous, last_seen_at=now)
        return NewsNotificationPlan(now, tuple(decisions), original, passive)

    def commit(
        self,
        plan: NewsNotificationPlan,
        sent_stable_ids: frozenset[str],
    ) -> None:
        allowed = {
            decision.item.stable_id: decision
            for decision in plan.decisions
            if decision.should_notify
        }
        unknown = sent_stable_ids.difference(allowed)
        if unknown:
            raise ValueError("Cannot commit news absent from notification plan.")
        records = dict(plan.passive_records)
        for stable_id in sent_stable_ids:
            decision = allowed[stable_id]
            records[stable_id] = _sent_record(
                decision.item,
                plan.original_records.get(stable_id),
                plan.observed_at,
                decision.kind,
            )
        self._store.save(NewsNotificationState(plan.observed_at, records))

    def _decision(
        self,
        item: NewsItem,
        previous: NewsNotificationRecord | None,
        now: datetime,
    ) -> NewsNotificationDecision:
        cancelled = _is_cancelled(item)
        if previous is None:
            eligible = (
                item.importance is not NewsImportance.LOW
                and now - self._lookback <= item.published_at <= now
                and not _is_promotion_only(item)
            )
            if not eligible:
                return NewsNotificationDecision(
                    item,
                    False,
                    NewsNotificationDecisionKind.INELIGIBLE,
                )
            return NewsNotificationDecision(
                item,
                True,
                (
                    NewsNotificationDecisionKind.CANCELLED
                    if cancelled
                    else NewsNotificationDecisionKind.INITIAL
                ),
            )

        if cancelled:
            return NewsNotificationDecision(
                item,
                previous.status is not NewsNotificationStatus.CANCELLED,
                (
                    NewsNotificationDecisionKind.CANCELLED
                    if previous.status is not NewsNotificationStatus.CANCELLED
                    else NewsNotificationDecisionKind.SUPPRESSED
                ),
            )
        if _IMPORTANCE_RANK[item.importance] < _IMPORTANCE_RANK[previous.importance]:
            return NewsNotificationDecision(
                item,
                False,
                NewsNotificationDecisionKind.DOWNGRADED,
            )
        if not now - self._lookback <= item.published_at <= now:
            return NewsNotificationDecision(
                item,
                False,
                NewsNotificationDecisionKind.INELIGIBLE,
            )
        if _is_meaningful_update(item, previous):
            return NewsNotificationDecision(
                item,
                True,
                NewsNotificationDecisionKind.UPDATED,
            )
        return NewsNotificationDecision(
            item,
            False,
            NewsNotificationDecisionKind.SUPPRESSED,
        )


def _is_meaningful_update(
    item: NewsItem,
    previous: NewsNotificationRecord,
) -> bool:
    if _IMPORTANCE_RANK[item.importance] > _IMPORTANCE_RANK[previous.importance]:
        return True
    if item.category is not previous.category:
        return True
    if _symbols(item.symbols) != previous.symbols:
        return True
    if _utc(item.event_starts_at) != previous.event_starts_at:
        return True
    if _is_emergency(item) and not previous.emergency:
        return True
    return (
        _title_fingerprint(item) != previous.title_fingerprint
        or _content_fingerprint(item) != previous.content_fingerprint
    )


def _sent_record(
    item: NewsItem,
    previous: NewsNotificationRecord | None,
    sent_at: datetime,
    kind: NewsNotificationDecisionKind,
) -> NewsNotificationRecord:
    status = {
        NewsNotificationDecisionKind.INITIAL: NewsNotificationStatus.SENT,
        NewsNotificationDecisionKind.UPDATED: NewsNotificationStatus.UPDATED,
        NewsNotificationDecisionKind.CANCELLED: NewsNotificationStatus.CANCELLED,
    }[kind]
    return NewsNotificationRecord(
        stable_id=item.stable_id,
        source=item.source,
        category=item.category,
        importance=item.importance,
        title_fingerprint=_title_fingerprint(item),
        content_fingerprint=_content_fingerprint(item),
        symbols=_symbols(item.symbols),
        published_at=item.published_at,
        event_starts_at=_utc(item.event_starts_at),
        first_sent_at=(previous.first_sent_at if previous is not None else sent_at),
        last_sent_at=sent_at,
        last_seen_at=sent_at,
        send_count=(previous.send_count + 1 if previous is not None else 1),
        status=status,
        emergency=_is_emergency(item),
    )


def _passive_record(
    item: NewsItem,
    previous: NewsNotificationRecord,
    seen_at: datetime,
    *,
    status: NewsNotificationStatus,
    update_content: bool,
) -> NewsNotificationRecord:
    return NewsNotificationRecord(
        stable_id=previous.stable_id,
        source=item.source,
        category=item.category,
        importance=item.importance,
        title_fingerprint=(
            _title_fingerprint(item)
            if update_content
            else previous.title_fingerprint
        ),
        content_fingerprint=(
            _content_fingerprint(item)
            if update_content
            else previous.content_fingerprint
        ),
        symbols=_symbols(item.symbols) if update_content else previous.symbols,
        published_at=item.published_at if update_content else previous.published_at,
        event_starts_at=(
            _utc(item.event_starts_at)
            if update_content
            else previous.event_starts_at
        ),
        first_sent_at=previous.first_sent_at,
        last_sent_at=previous.last_sent_at,
        last_seen_at=seen_at,
        send_count=previous.send_count,
        status=status,
        emergency=_is_emergency(item) if update_content else previous.emergency,
    )


def _prune_records(
    records: dict[str, NewsNotificationRecord],
    now: datetime,
    retention: timedelta,
) -> dict[str, NewsNotificationRecord]:
    return {
        stable_id: record
        for stable_id, record in records.items()
        if not (
            now - record.last_seen_at > retention
            and (
                record.event_starts_at is None or record.event_starts_at <= now
            )
            and record.status is not NewsNotificationStatus.CANCELLED
        )
    }


def _title_fingerprint(item: NewsItem) -> str:
    return _fingerprint((_normalized_content(item.title),))


def _content_fingerprint(item: NewsItem) -> str:
    tags = tuple(
        sorted(
            normalized
            for tag in item.tags
            if (normalized := _normalized_content(tag))
            and not _contains_any(normalized, _PROMOTION_MARKERS)
        )
    )
    return _fingerprint(
        (
            item.category.value,
            item.asset_type.value,
            item.event_start_date.isoformat() if item.event_start_date else "",
            _normalized_content(item.description),
            _normalized_content(item.reason),
            _normalized_content(item.recommended_action),
            *tags,
            *_symbols(item.symbols),
        )
    )


def _normalized_content(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    parts = _PROMO_SUFFIX.split(normalized)
    normalized = " ".join(
        part for part in parts if not _contains_any(part, _PROMOTION_MARKERS)
    )
    for marker in _PROMOTION_MARKERS:
        normalized = normalized.replace(marker, " ")
    normalized = _PROMO_NUMBER.sub(" ", normalized)
    return " ".join(_NON_WORD.sub(" ", normalized).split())


def _fingerprint(values: tuple[str, ...]) -> str:
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()


def _is_cancelled(item: NewsItem) -> bool:
    return _contains_any(_item_text(item), _CANCELLATION_MARKERS)


def _is_emergency(item: NewsItem) -> bool:
    return _contains_any(_item_text(item), _EMERGENCY_MARKERS)


def _is_promotion_only(item: NewsItem) -> bool:
    return item.category is NewsCategory.OTHER and _contains_any(
        _item_text(item),
        _PROMOTION_MARKERS,
    )


def _item_text(item: NewsItem) -> str:
    return " ".join(
        (
            item.title,
            item.description,
            item.reason,
            item.recommended_action,
            *item.tags,
        )
    ).casefold()


def _contains_any(text: str, values: tuple[str, ...]) -> bool:
    return any(value.strip() in text for value in values)


def _symbols(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(value.strip().upper() for value in values)))


def _unique_items(items: tuple[NewsItem, ...]) -> tuple[NewsItem, ...]:
    return tuple({item.stable_id: item for item in items}.values())


def _utc(value: datetime | None) -> datetime | None:
    return value.astimezone(UTC) if value is not None else None


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"News notification {name} must be timezone-aware.")


def _record_to_json(record: NewsNotificationRecord) -> dict[str, Any]:
    return {
        "stable_id": record.stable_id,
        "source": record.source,
        "category": record.category.value,
        "importance": record.importance.value,
        "title_fingerprint": record.title_fingerprint,
        "content_fingerprint": record.content_fingerprint,
        "symbols": list(record.symbols),
        "published_at": record.published_at.isoformat(),
        "event_starts_at": (
            record.event_starts_at.isoformat()
            if record.event_starts_at is not None
            else None
        ),
        "first_sent_at": record.first_sent_at.isoformat(),
        "last_sent_at": record.last_sent_at.isoformat(),
        "last_seen_at": record.last_seen_at.isoformat(),
        "send_count": record.send_count,
        "status": record.status.value,
        "emergency": record.emergency,
    }


def _state_from_json(value: Any) -> NewsNotificationState:
    if (
        not isinstance(value, dict)
        or value.get("schema") != _SCHEMA
        or value.get("version") != 1
    ):
        raise ValueError("Unsupported news notification state.")
    raw_records = value["records"]
    if not isinstance(raw_records, dict):
        raise ValueError("News notification records must be an object.")
    records: dict[str, NewsNotificationRecord] = {}
    for stable_id, raw in raw_records.items():
        if not isinstance(stable_id, str) or not isinstance(raw, dict):
            raise ValueError("Invalid news notification record.")
        record = _record_from_json(raw)
        if record.stable_id != stable_id:
            raise ValueError("News notification key mismatch.")
        records[stable_id] = record
    return NewsNotificationState(_optional_datetime(value.get("updated_at")), records)


def _record_from_json(value: Mapping[str, Any]) -> NewsNotificationRecord:
    raw_symbols = value["symbols"]
    if not isinstance(raw_symbols, list):
        raise ValueError("News notification symbols must be a list.")
    return NewsNotificationRecord(
        stable_id=_string(value["stable_id"]),
        source=_string(value["source"]),
        category=NewsCategory(_string(value["category"])),
        importance=NewsImportance(_string(value["importance"])),
        title_fingerprint=_string(value["title_fingerprint"]),
        content_fingerprint=_string(value["content_fingerprint"]),
        symbols=tuple(_string(symbol) for symbol in raw_symbols),
        published_at=_datetime(value["published_at"]),
        event_starts_at=_optional_datetime(value.get("event_starts_at")),
        first_sent_at=_datetime(value["first_sent_at"]),
        last_sent_at=_datetime(value["last_sent_at"]),
        last_seen_at=_datetime(value["last_seen_at"]),
        send_count=_integer(value["send_count"]),
        status=NewsNotificationStatus(_string(value["status"])),
        emergency=_boolean(value.get("emergency", False)),
    )


def _datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected ISO timestamp.")
    parsed = datetime.fromisoformat(value)
    _require_aware(parsed, "stored timestamp")
    return parsed.astimezone(UTC)


def _optional_datetime(value: Any) -> datetime | None:
    return None if value is None else _datetime(value)


def _string(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected string value.")
    return value


def _integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Expected integer value.")
    return value


def _boolean(value: Any) -> bool:
    if not isinstance(value, bool):
        raise ValueError("Expected boolean value.")
    return value
