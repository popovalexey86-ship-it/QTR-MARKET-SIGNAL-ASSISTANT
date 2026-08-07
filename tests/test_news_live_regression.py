from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from market_signal_assistant.news.bybit_provider import BybitAnnouncementProvider
from market_signal_assistant.news.classifier import NewsClassifier, extract_symbols
from market_signal_assistant.news.models import (
    NewsCategory,
    NewsReport,
    NewsSourceRecord,
)
from market_signal_assistant.news.notifications import (
    NewsNotificationDecision,
    NewsNotificationDecisionKind,
)
from market_signal_assistant.telegram.formatting import (
    format_auto_news_event,
    format_news_report,
)

PUBLISHED = datetime(2026, 8, 3, 1, 53, tzinfo=UTC)
EXPECTED_START = datetime(2026, 8, 3, 3, 0, tzinfo=UTC)
TITLE = (
    "Listing of UNITREEUSDT on Bybit Perpetual Pre-Market "
    "on Aug 3, 2026, 3:00AM UTC"
)


def row(
    *,
    title: str = TITLE,
    description: str = "Official Bybit announcement.",
    type_key: str = "new_crypto",
    start_timestamp: datetime | None = PUBLISHED,
) -> Mapping[str, Any]:
    payload: dict[str, Any] = {
        "title": title,
        "description": description,
        "type": {"title": "New Listings", "key": type_key},
        "tags": ["Derivatives"],
        "url": "https://announcements.bybit.com/en-US/article/unitree/",
        "dateTimestamp": int(PUBLISHED.timestamp() * 1000),
        "publishTime": int(PUBLISHED.timestamp() * 1000),
    }
    if start_timestamp is not None:
        payload["startDateTimestamp"] = int(start_timestamp.timestamp() * 1000)
    return payload


def provider_record(**overrides: Any) -> NewsSourceRecord:
    payload = row(**overrides)
    provider = BybitAnnouncementProvider(
        getter=lambda url, timeout: {
            "retCode": 0,
            "result": {"list": [payload]},
        }
    )
    return provider.fetch()[0]


@pytest.mark.parametrize(
    "noise",
    [
        "UTC",
        "GMT",
        "AM",
        "PM",
        "UTC+7",
        "UTC-4",
        "AUG",
        "AUGUST",
    ],
)
def test_time_and_timezone_tokens_are_not_symbols(noise: str) -> None:
    symbols = extract_symbols(
        f"Listing of UNITREEUSDT at 3:00 {noise}",
        "",
        (),
    )

    assert symbols == ("UNITREEUSDT",)


def test_live_unitree_symbol_is_extracted_without_utc() -> None:
    record = provider_record()
    item = NewsClassifier().classify(record)

    assert item is not None
    assert item.symbols == ("UNITREEUSDT",)


def test_announcement_timestamp_is_not_copied_to_event_start() -> None:
    record = provider_record(
        title="New Listing: UNITREEUSDT",
        start_timestamp=None,
    )

    assert record.published_at == PUBLISHED
    assert record.event_starts_at is None


def test_start_timestamp_is_ignored_for_new_crypto_without_text_confirmation() -> None:
    record = provider_record(
        title="New Listing: UNITREEUSDT",
        type_key="new_crypto",
        start_timestamp=PUBLISHED,
    )

    assert record.event_starts_at is None


def test_latest_activities_keeps_contractual_start_timestamp() -> None:
    record = provider_record(
        title="Official activity",
        type_key="latest_activities",
        start_timestamp=EXPECTED_START,
    )

    assert record.event_starts_at == EXPECTED_START


def test_explicit_english_datetime_is_parsed_as_utc() -> None:
    record = provider_record()

    assert record.event_starts_at == EXPECTED_START
    assert record.event_starts_at.tzinfo is UTC


@pytest.mark.parametrize(
    "title",
    [
        "Listing of UNITREEUSDT on Aug 3, 2026 at 3:00AM",
        "Listing of UNITREEUSDT on Aug 3 at 3:00AM UTC",
        (
            "Listing of UNITREEUSDT on Aug 3, 2026, 3:00AM UTC; "
            "Aug 4, 2026, 3:00AM UTC"
        ),
    ],
)
def test_ambiguous_or_incomplete_datetime_does_not_create_event_start(
    title: str,
) -> None:
    record = provider_record(title=title, start_timestamp=PUBLISHED)

    assert record.event_starts_at is None


def test_pre_market_semantics_reach_manual_and_automatic_output() -> None:
    record = provider_record()
    item = NewsClassifier().classify(record)
    assert item is not None

    manual = format_news_report(NewsReport(PUBLISHED, 24, (item,)))[0]
    automatic = format_auto_news_event(
        NewsNotificationDecision(
            item,
            True,
            NewsNotificationDecisionKind.INITIAL,
        ),
        None,
        PUBLISHED,
    )

    assert item.category is NewsCategory.LISTING
    assert item.description == (
        "Bybit запускает предрыночную торговлю UNITREEUSDT."
    )
    assert "UNITREEUSDT, UTC" not in manual
    assert "ПРЕДРЫНОЧНЫЙ ЛИСТИНГ" in manual
    assert "ПРЕДРЫНОЧНЫЙ ЛИСТИНГ" in automatic
    assert "⏰ Начало события: 2026-08-03 03:00 UTC" in manual
    assert "⏰ Начало события: 2026-08-03 03:00 UTC" in automatic


def test_ordinary_listing_keeps_ordinary_listing_presentation() -> None:
    record = provider_record(
        title="New Listing: UNITREEUSDT",
        start_timestamp=None,
    )
    item = NewsClassifier().classify(record)
    assert item is not None

    automatic = format_auto_news_event(
        NewsNotificationDecision(
            item,
            True,
            NewsNotificationDecisionKind.INITIAL,
        ),
        None,
        PUBLISHED,
    )

    assert item.description == "Bybit объявляет новый листинг UNITREEUSDT."
    assert "🟠 НОВОЕ — НОВЫЙ ЛИСТИНГ" in automatic
    assert "ПРЕДРЫНОЧНЫЙ" not in automatic
    assert "Начало события:" not in automatic
