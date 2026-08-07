from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from market_signal_assistant.news.bybit_provider import BybitAnnouncementProvider
from market_signal_assistant.news.classifier import NewsClassifier, extract_symbols
from market_signal_assistant.news.models import (
    NewsCategory,
    NewsImportance,
    NewsSourceRecord,
)
from market_signal_assistant.news.provider import NewsDataError
from market_signal_assistant.news.service import NewsService
from market_signal_assistant.providers import MarketDataError
from market_signal_assistant.settings import NewsSettings

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def raw_record(
    title: str,
    *,
    description: str = "Official operational announcement.",
    url_slug: str = "notice",
    published_at: datetime = NOW,
    type_key: str = "",
    tags: tuple[str, ...] = (),
) -> NewsSourceRecord:
    return NewsSourceRecord(
        source="Bybit",
        title=title,
        description=description,
        url=f"https://announcements.bybit.com/en-US/article/{url_slug}/",
        type_key=type_key,
        tags=tags,
        published_at=published_at,
        event_starts_at=None,
    )


def response(*rows: Mapping[str, Any]) -> Mapping[str, Any]:
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {"total": len(rows), "list": list(rows)},
    }


def bybit_row(
    *,
    title: str = "New Listing: Alpha (ABC)",
    description: str = "Bybit will list ABC for spot trading.",
    url: str = "https://announcements.bybit.com/en-US/article/abc-listing/",
    published_at: datetime = NOW,
    type_key: str = "new_crypto",
    tags: tuple[str, ...] = ("Spot", "Spot Listings"),
) -> Mapping[str, Any]:
    return {
        "title": title,
        "description": description,
        "type": {"title": "New Listings", "key": type_key},
        "tags": list(tags),
        "url": url,
        "dateTimestamp": int(published_at.timestamp() * 1000),
        "publishTime": int(published_at.timestamp() * 1000),
    }


def test_bybit_provider_maps_public_response_and_uses_required_query() -> None:
    requested: list[str] = []

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        assert timeout == 10
        requested.append(url)
        return response(bybit_row())

    provider = BybitAnnouncementProvider(
        getter=getter,
        base_url="https://api.bybit.com",
    )

    records = provider.fetch(page=2, limit=20)

    assert len(records) == 1
    assert records[0].published_at == NOW
    assert records[0].published_at.tzinfo is UTC
    query = parse_qs(urlsplit(requested[0]).query)
    assert query == {"locale": ["en-US"], "page": ["2"], "limit": ["20"]}
    assert requested[0].startswith("https://api.bybit.com/v5/announcements/index?")


def test_bybit_response_pipeline_produces_normalized_news_item() -> None:
    provider = BybitAnnouncementProvider(
        getter=lambda url, timeout: response(bybit_row())
    )
    service = NewsService(provider, NewsClassifier(), clock=lambda: NOW)

    report = service.get_important()

    assert len(report.items) == 1
    item = report.items[0]
    assert item.source == "Bybit"
    assert item.category is NewsCategory.LISTING
    assert item.importance is NewsImportance.HIGH
    assert item.published_at.tzinfo is UTC


def test_bybit_provider_uses_activity_event_start_and_skips_malformed() -> None:
    valid = dict(bybit_row())
    valid["type"] = {"title": "Activities", "key": "latest_activities"}
    valid["startDateTimestamp"] = int((NOW + timedelta(hours=2)).timestamp() * 1000)
    provider = BybitAnnouncementProvider(
        getter=lambda url, timeout: response({"broken": True}, valid)
    )

    records = provider.fetch()

    assert len(records) == 1
    assert records[0].event_starts_at == NOW + timedelta(hours=2)


def test_bybit_network_failure_becomes_controlled_news_error() -> None:
    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        del url, timeout
        raise MarketDataError("offline")

    with pytest.raises(NewsDataError, match="Bybit"):
        BybitAnnouncementProvider(getter=getter).fetch()


@pytest.mark.parametrize(
    ("title", "category"),
    [
        ("New Listing: Alpha (ABC)", NewsCategory.LISTING),
        ("Delisting of XYZ Trading Pairs", NewsCategory.DELISTING),
        ("Scheduled Trading System Maintenance", NewsCategory.MAINTENANCE),
    ],
)
def test_supported_categories_are_classified(
    title: str,
    category: NewsCategory,
) -> None:
    item = NewsClassifier().classify(raw_record(title))

    assert item is not None
    assert item.category is category


def test_stable_id_does_not_change_between_runs() -> None:
    classifier = NewsClassifier()
    record = raw_record("New Listing: Alpha (ABC)")

    first = classifier.classify(record)
    second = classifier.classify(record)

    assert first is not None and second is not None
    assert first.stable_id == second.stable_id


def test_stable_id_normalizes_url_and_falls_back_when_url_is_absent() -> None:
    classifier = NewsClassifier()
    first = raw_record("New Listing: Alpha (ABC)")
    normalized_variant = NewsSourceRecord(
        source=first.source,
        title=first.title,
        description=first.description,
        url=f"{first.url}?campaign=ignored#fragment",
        type_key=first.type_key,
        tags=first.tags,
        published_at=first.published_at,
        event_starts_at=None,
    )
    without_url = NewsSourceRecord(
        source="Bybit",
        title="Product Update",
        description="Product update details.",
        url="",
        type_key="",
        tags=(),
        published_at=NOW,
        event_starts_at=None,
    )

    classified = classifier.classify(first)
    variant = classifier.classify(normalized_variant)
    fallback_one = classifier.classify(without_url)
    fallback_two = classifier.classify(without_url)

    assert classified is not None and variant is not None
    assert classified.stable_id == variant.stable_id
    assert fallback_one is not None and fallback_two is not None
    assert fallback_one.stable_id == fallback_two.stable_id


def test_pure_promotion_is_discarded() -> None:
    record = raw_record(
        "Trade and Share a 1,000,000 USDT Prize Pool",
        description="Join our VIP trading competition and earn bonuses.",
        tags=("Campaign", "Rewards"),
    )

    assert NewsClassifier().classify(record) is None


def test_listing_with_promotional_text_keeps_only_listing_fact() -> None:
    record = raw_record(
        "New Listing: Alpha (ABC) — Trade to Share 500,000 USDT!",
        description="Deposit, trade and earn bonuses from the prize pool.",
        type_key="new_crypto",
        tags=("Spot Listings", "Campaign"),
    )

    item = NewsClassifier().classify(record)

    assert item is not None
    assert item.category is NewsCategory.LISTING
    assert item.importance is NewsImportance.HIGH
    assert "приз" not in item.description.lower()
    assert "бонус" not in item.description.lower()
    assert "ABC" in item.symbols


def test_symbol_extraction_deduplicates_and_ignores_quote_assets_and_words() -> None:
    symbols = extract_symbols(
        title="New Listing: Alpha (ABC) and ABCUSDT Perpetual Contract",
        description="ABC will trade against USDT and USDC.",
        tags=("Spot", "BTC", "ETH", "ABC"),
    )

    assert symbols == ("ABC", "ABCUSDT")
    assert "USDT" not in symbols
    assert "USDC" not in symbols
    assert "BTC" not in symbols
    assert "ETH" not in symbols
    assert "NEW" not in symbols


def test_perpetual_contract_launch_is_a_listing() -> None:
    item = NewsClassifier().classify(
        raw_record("Bybit to Launch ABCUSDT Perpetual Contract")
    )

    assert item is not None
    assert item.category is NewsCategory.LISTING
    assert item.symbols == ("ABCUSDT",)


def test_news_settings_defaults_and_environment_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NEWS_ENABLED", raising=False)
    monkeypatch.delenv("NEWS_LOOKBACK_HOURS", raising=False)
    monkeypatch.delenv("NEWS_NOTIFICATION_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("BYBIT_PUBLIC_BASE_URL", raising=False)

    assert NewsSettings.from_environment() == NewsSettings()

    monkeypatch.setenv("NEWS_ENABLED", "false")
    monkeypatch.setenv("NEWS_LOOKBACK_HOURS", "168")
    monkeypatch.setenv("NEWS_NOTIFICATION_RETENTION_DAYS", "365")
    monkeypatch.setenv("BYBIT_PUBLIC_BASE_URL", "https://api.bybit.com/")
    assert NewsSettings.from_environment() == NewsSettings(
        False,
        168,
        notification_retention_days=365,
    )


@pytest.mark.parametrize("hours", [0, 169])
def test_news_lookback_outside_supported_range_is_rejected(hours: int) -> None:
    with pytest.raises(ValueError, match="1 до 168"):
        NewsSettings(lookback_hours=hours)


def test_news_settings_reject_testnet_base_url() -> None:
    with pytest.raises(ValueError, match="production"):
        NewsSettings(base_url="https://api-testnet.bybit.com")


@pytest.mark.parametrize("days", [6, 366])
def test_news_notification_retention_outside_supported_range_is_rejected(
    days: int,
) -> None:
    with pytest.raises(ValueError, match="7 до 365"):
        NewsSettings(notification_retention_days=days)


class Pages:
    def __init__(self, records: tuple[NewsSourceRecord, ...]) -> None:
        self.records = records
        self.calls: list[int] = []

    def fetch(self, page: int = 1, limit: int = 20) -> tuple[NewsSourceRecord, ...]:
        assert limit == 20
        self.calls.append(page)
        return self.records if page == 1 else ()


def test_service_sorts_importance_then_recency_and_filters_low() -> None:
    records = (
        raw_record("Product Update", url_slug="medium", published_at=NOW),
        raw_record(
            "New Listing: Alpha (ABC)",
            url_slug="high",
            published_at=NOW - timedelta(hours=2),
        ),
        raw_record(
            "Emergency Suspension of Withdrawals",
            url_slug="critical-old",
            published_at=NOW - timedelta(hours=3),
        ),
        raw_record(
            "Security Incident: Services Suspended",
            url_slug="critical-new",
            published_at=NOW - timedelta(hours=1),
        ),
        raw_record("Weekly Market Recap", url_slug="low"),
    )
    service = NewsService(Pages(records), NewsClassifier(), clock=lambda: NOW)

    report = service.get_important()

    assert tuple(item.importance for item in report.items) == (
        NewsImportance.CRITICAL,
        NewsImportance.CRITICAL,
        NewsImportance.HIGH,
        NewsImportance.MEDIUM,
    )
    assert report.items[0].url.endswith("critical-new/")
    assert report.items[1].url.endswith("critical-old/")


def test_service_applies_lookback_and_maximum_ten() -> None:
    recent = tuple(
        raw_record(
            f"New Listing: Coin (C{index})",
            url_slug=f"listing-{index}",
            published_at=NOW - timedelta(minutes=index),
        )
        for index in range(12)
    )
    old = raw_record(
        "Delisting of OLD Trading Pairs",
        url_slug="old",
        published_at=NOW - timedelta(hours=24, seconds=1),
    )
    service = NewsService(
        Pages((*recent, old)),
        NewsClassifier(),
        lookback_hours=24,
        clock=lambda: NOW,
    )

    report = service.get_important()

    assert len(report.items) == 10
    assert all(item.published_at >= NOW - timedelta(hours=24) for item in report.items)
    assert all(not item.url.endswith("old/") for item in report.items)
