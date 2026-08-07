from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, urlsplit

from market_signal_assistant.news.models import NewsAssetType, NewsSourceRecord
from market_signal_assistant.news.provider import NewsDataError
from market_signal_assistant.providers import (
    JsonGetter,
    MarketDataError,
    public_json_get,
)

_LOGGER = logging.getLogger(__name__)
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_TEXT_EVENT_TIME = re.compile(
    r"\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
    r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?)\s+"
    r"(?P<day>\d{1,2}),\s*(?P<year>\d{4}),?\s*(?:at\s+)?"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
    r"(?P<ampm>AM|PM)?\s*(?P<zone>UTC|GMT)"
    r"(?P<offset>[+-]\d{1,2}(?::?\d{2})?)?\b",
    re.IGNORECASE,
)
_TEXT_EVENT_DATE = re.compile(
    r"\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
    r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?)\s+"
    r"(?P<day>\d{1,2}),\s*(?P<year>\d{4})\b",
    re.IGNORECASE,
)
_PERPETUAL_PAIR = re.compile(r"\b[A-Z0-9]{2,15}USDT\b")
_SYMBOL_TYPE_ASSET = {
    "": NewsAssetType.CRYPTO,
    "innovation": NewsAssetType.CRYPTO,
    "stock": NewsAssetType.STOCK,
    "xstocks": NewsAssetType.STOCK,
    "etf": NewsAssetType.ETF,
    "fund": NewsAssetType.ETF,
    "commodity": NewsAssetType.COMMODITY,
    "forex": NewsAssetType.FOREX,
}


class BybitAnnouncementProvider:
    """Read official announcements through the existing injectable transport."""

    def __init__(
        self,
        *,
        getter: JsonGetter = public_json_get,
        base_url: str = "https://api.bybit.com",
        timeout: float = 10.0,
    ) -> None:
        self._getter = getter
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._instrument_asset_types: dict[str, NewsAssetType] | None = None

    def fetch(
        self,
        page: int = 1,
        limit: int = 20,
    ) -> tuple[NewsSourceRecord, ...]:
        if page <= 0 or limit <= 0:
            raise ValueError("Announcement page and limit must be positive.")
        query = urlencode({"locale": "en-US", "page": page, "limit": limit})
        url = f"{self._base_url}/v5/announcements/index?{query}"
        try:
            payload = self._getter(url, self._timeout)
        except MarketDataError as error:
            raise NewsDataError("Bybit announcements are unavailable.") from error
        try:
            if payload.get("retCode") != 0:
                raise ValueError
            rows = payload["result"]["list"]
            if not isinstance(rows, list):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise NewsDataError("Malformed Bybit announcement response.") from None

        records: list[NewsSourceRecord] = []
        for index, row in enumerate(rows):
            try:
                if not isinstance(row, Mapping):
                    raise ValueError
                records.append(_record(row))
            except (KeyError, TypeError, ValueError):
                _LOGGER.warning(
                    "Пропущена повреждённая запись объявления Bybit (index=%s).",
                    index,
                )
        return self._enrich_asset_types(tuple(records))

    def _enrich_asset_types(
        self,
        records: tuple[NewsSourceRecord, ...],
    ) -> tuple[NewsSourceRecord, ...]:
        candidates = {
            symbol
            for record in records
            for symbol in _perpetual_symbols(record)
        }
        if not candidates:
            return records
        asset_types = self._load_instrument_asset_types()
        return tuple(
            replace(
                record,
                asset_type=_record_asset_type(record, asset_types),
            )
            for record in records
        )

    def _load_instrument_asset_types(self) -> dict[str, NewsAssetType]:
        if self._instrument_asset_types is not None:
            return self._instrument_asset_types
        query = urlencode({"category": "linear", "limit": 1000})
        url = f"{self._base_url}/v5/market/instruments-info?{query}"
        try:
            payload = self._getter(url, self._timeout)
            if payload.get("retCode") != 0:
                raise ValueError
            rows = payload["result"]["list"]
            if not isinstance(rows, list):
                raise ValueError
            self._instrument_asset_types = _instrument_asset_types(rows)
        except (KeyError, TypeError, ValueError, MarketDataError):
            _LOGGER.warning(
                "Не удалось получить metadata типов инструментов Bybit; "
                "неоднозначные perpetual-активы останутся UNKNOWN."
            )
            self._instrument_asset_types = {}
        return self._instrument_asset_types


def _record(row: Mapping[str, Any]) -> NewsSourceRecord:
    raw_type = row["type"]
    raw_tags = row.get("tags", [])
    if not isinstance(raw_type, Mapping) or not isinstance(raw_tags, list):
        raise ValueError("Invalid announcement metadata.")
    url = _official_url(row["url"])
    title = _text(row["title"])
    description = _text(row.get("description", ""))
    type_key = _text(raw_type.get("key", ""))
    published_value = row.get("publishTime") or row.get("dateTimestamp")
    event_starts_at = _event_start(
        type_key=type_key,
        title=title,
        description=description,
        start_value=(
            row.get("startDateTimestamp")
            or row.get("startDataTimestamp")
        ),
    )
    return NewsSourceRecord(
        source="Bybit",
        title=title,
        description=description,
        url=url,
        type_key=type_key,
        tags=tuple(_text(tag) for tag in raw_tags),
        published_at=_timestamp(published_value),
        event_starts_at=event_starts_at,
        event_start_date=(
            _text_event_date(f"{title}\n{description}")
            if event_starts_at is None
            and type_key.casefold() != "latest_activities"
            else None
        ),
    )


def _event_start(
    *,
    type_key: str,
    title: str,
    description: str,
    start_value: Any,
) -> datetime | None:
    if type_key.casefold() == "latest_activities":
        return (
            _timestamp(start_value)
            if start_value not in (None, "", 0, "0")
            else None
        )
    return _text_event_start(f"{title}\n{description}")


def _text_event_start(text: str) -> datetime | None:
    candidates: set[datetime] = set()
    for match in _TEXT_EVENT_TIME.finditer(text):
        try:
            candidates.add(_matched_datetime(match))
        except ValueError:
            continue
    return next(iter(candidates)) if len(candidates) == 1 else None


def _text_event_date(text: str) -> date | None:
    candidates: set[date] = set()
    for match in _TEXT_EVENT_DATE.finditer(text):
        try:
            candidates.add(
                date(
                    int(match.group("year")),
                    _MONTHS[match.group("month").casefold()],
                    int(match.group("day")),
                )
            )
        except ValueError:
            continue
    return next(iter(candidates)) if len(candidates) == 1 else None


def _perpetual_symbols(record: NewsSourceRecord) -> tuple[str, ...]:
    text = f"{record.title} {record.description}"
    if "perpetual" not in text.casefold():
        return ()
    return tuple(dict.fromkeys(_PERPETUAL_PAIR.findall(text.upper())))


def _record_asset_type(
    record: NewsSourceRecord,
    asset_types: Mapping[str, NewsAssetType],
) -> NewsAssetType:
    found = {
        asset_types[symbol]
        for symbol in _perpetual_symbols(record)
        if symbol in asset_types
    }
    return next(iter(found)) if len(found) == 1 else NewsAssetType.UNKNOWN


def _instrument_asset_types(rows: list[Any]) -> dict[str, NewsAssetType]:
    result: dict[str, NewsAssetType] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        try:
            if (
                _text(row["contractType"]) != "LinearPerpetual"
                or _text(row["quoteCoin"]).upper() != "USDT"
                or _text(row["settleCoin"]).upper() != "USDT"
            ):
                continue
            symbol = _text(row["symbol"]).upper()
            symbol_type = _text(row.get("symbolType", "")).casefold()
        except (KeyError, TypeError, ValueError):
            continue
        asset_type = _SYMBOL_TYPE_ASSET.get(symbol_type)
        if symbol and asset_type is not None:
            result[symbol] = asset_type
    return result


def _matched_datetime(match: re.Match[str]) -> datetime:
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    ampm = match.group("ampm")
    if ampm is not None:
        if not 1 <= hour <= 12:
            raise ValueError("Invalid 12-hour clock value.")
        hour = hour % 12 + (12 if ampm.casefold() == "pm" else 0)
    elif not 0 <= hour <= 23:
        raise ValueError("Invalid 24-hour clock value.")
    offset = _timezone_offset(match.group("offset"))
    local = datetime(
        int(match.group("year")),
        _MONTHS[match.group("month").casefold()],
        int(match.group("day")),
        hour,
        minute,
        tzinfo=timezone(offset),
    )
    return local.astimezone(UTC)


def _timezone_offset(value: str | None) -> timedelta:
    if value is None:
        return timedelta()
    sign = 1 if value[0] == "+" else -1
    raw = value[1:]
    if ":" in raw:
        hours_text, minutes_text = raw.split(":", maxsplit=1)
    elif len(raw) > 2:
        hours_text, minutes_text = raw[:-2], raw[-2:]
    else:
        hours_text, minutes_text = raw, "0"
    hours = int(hours_text)
    minutes = int(minutes_text)
    if hours > 23 or minutes > 59:
        raise ValueError("Invalid timezone offset.")
    return sign * timedelta(hours=hours, minutes=minutes)


def _official_url(value: Any) -> str:
    url = _text(value)
    if not url:
        return ""
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not (
        host == "bybit.com" or host.endswith(".bybit.com")
    ):
        raise ValueError("Announcement URL is not an official Bybit URL.")
    return url


def _timestamp(value: Any) -> datetime:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("Invalid announcement timestamp.")
    milliseconds = int(value)
    if milliseconds <= 0:
        raise ValueError("Invalid announcement timestamp.")
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


def _text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected announcement text.")
    return value.strip()
