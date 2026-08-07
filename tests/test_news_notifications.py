from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.news.models import (
    NewsCategory,
    NewsImportance,
    NewsItem,
)
from market_signal_assistant.news.notifications import (
    DEFAULT_NEWS_NOTIFICATION_STATE_PATH,
    JsonNewsNotificationStore,
    NewsNotificationDecisionKind,
    NewsNotificationService,
)

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def item(
    *,
    stable_id: str = "news-1",
    title: str = "New Listing: Alpha (ABC)",
    description: str = "Bybit объявляет новый листинг ABC.",
    category: NewsCategory = NewsCategory.LISTING,
    importance: NewsImportance = NewsImportance.HIGH,
    symbols: tuple[str, ...] = ("ABC",),
    published_at: datetime = NOW - timedelta(hours=1),
    event_starts_at: datetime | None = None,
    tags: tuple[str, ...] = ("Spot", "Listings"),
    action: str = "Дождаться стабилизации ликвидности.",
) -> NewsItem:
    return NewsItem(
        stable_id=stable_id,
        source="Bybit",
        title=title,
        description=description,
        url="https://announcements.bybit.com/en-US/article/abc/",
        category=category,
        importance=importance,
        symbols=symbols,
        published_at=published_at,
        event_starts_at=event_starts_at,
        tags=tags,
        reason="Событие может повлиять на торговые условия.",
        recommended_action=action,
    )


def service(
    tmp_path: Path,
    *,
    retention_days: int = 30,
) -> NewsNotificationService:
    return NewsNotificationService(
        JsonNewsNotificationStore(tmp_path / "news_notifications.json"),
        retention_days=retention_days,
        lookback_hours=24,
    )


def sent_once(
    notifications: NewsNotificationService,
    news: NewsItem,
    observed_at: datetime = NOW,
) -> None:
    plan = notifications.prepare((news,), observed_at)
    notifications.commit(plan, frozenset({news.stable_id}))


def test_first_news_is_allowed_and_identical_news_is_suppressed(
    tmp_path: Path,
) -> None:
    notifications = service(tmp_path)

    first = notifications.prepare((item(),), NOW)
    notifications.commit(first, frozenset({"news-1"}))
    repeated = notifications.prepare((item(),), NOW + timedelta(minutes=5))

    assert first.decisions[0].should_notify is True
    assert first.decisions[0].kind is NewsNotificationDecisionKind.INITIAL
    assert repeated.decisions[0].should_notify is False
    assert repeated.decisions[0].kind is NewsNotificationDecisionKind.SUPPRESSED


def test_whitespace_case_and_punctuation_changes_are_suppressed(
    tmp_path: Path,
) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())
    changed = item(
        title="  NEW listing alpha ABC!!! ",
        description="BYBIT   ОБЪЯВЛЯЕТ новый листинг ABC...",
        tags=("Listings", "Spot"),
    )

    decision = notifications.prepare(
        (changed,), NOW + timedelta(minutes=5)
    ).decisions[0]

    assert decision.should_notify is False


def test_promotional_text_change_is_suppressed(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(
        notifications,
        item(title="New Listing: Alpha (ABC) — Trade to Share 1M USDT Prize Pool"),
    )
    changed = item(
        title="New Listing: Alpha (ABC) — Trade to Share 2M USDT Prize Pool",
        tags=("Listings", "Spot", "Campaign", "Rewards"),
    )

    decision = notifications.prepare(
        (changed,), NOW + timedelta(minutes=5)
    ).decisions[0]

    assert decision.should_notify is False


def test_importance_increase_is_an_update(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())

    decision = notifications.prepare(
        (item(importance=NewsImportance.CRITICAL),),
        NOW + timedelta(minutes=5),
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.kind is NewsNotificationDecisionKind.UPDATED


@pytest.mark.parametrize(
    ("category", "first_time", "updated_time"),
    [
        (
            NewsCategory.DELISTING,
            NOW + timedelta(hours=12),
            NOW + timedelta(hours=8),
        ),
        (
            NewsCategory.MAINTENANCE,
            NOW + timedelta(hours=4),
            NOW + timedelta(hours=5),
        ),
    ],
)
def test_changed_event_time_is_an_update(
    tmp_path: Path,
    category: NewsCategory,
    first_time: datetime,
    updated_time: datetime,
) -> None:
    notifications = service(tmp_path)
    original = item(category=category, event_starts_at=first_time)
    sent_once(notifications, original)

    decision = notifications.prepare(
        (item(category=category, event_starts_at=updated_time),),
        NOW + timedelta(minutes=5),
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.kind is NewsNotificationDecisionKind.UPDATED


def test_changed_affected_symbols_is_an_update(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())

    decision = notifications.prepare(
        (item(symbols=("ABC", "ABCUSDT")),),
        NOW + timedelta(minutes=5),
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.kind is NewsNotificationDecisionKind.UPDATED


def test_confirmed_action_change_is_an_update(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())

    decision = notifications.prepare(
        (item(action="Перевести активы до официального срока."),),
        NOW + timedelta(minutes=5),
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.kind is NewsNotificationDecisionKind.UPDATED


def test_new_emergency_information_is_an_update(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())

    decision = notifications.prepare(
        (item(description="Unexpected suspension effective immediately."),),
        NOW + timedelta(minutes=5),
    ).decisions[0]

    assert decision.should_notify is True
    assert decision.kind is NewsNotificationDecisionKind.UPDATED


def test_importance_decrease_is_not_sent_but_updates_state(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item(importance=NewsImportance.CRITICAL))
    plan = notifications.prepare(
        (item(importance=NewsImportance.HIGH),),
        NOW + timedelta(minutes=5),
    )

    assert plan.decisions[0].should_notify is False
    assert plan.decisions[0].kind is NewsNotificationDecisionKind.DOWNGRADED
    notifications.commit(plan, frozenset())
    record = JsonNewsNotificationStore(
        tmp_path / "news_notifications.json"
    ).load().records["news-1"]
    assert record.importance is NewsImportance.HIGH
    assert record.send_count == 1


def test_official_cancellation_is_sent_once(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())
    cancelled = item(
        title="Cancelled: New Listing Alpha (ABC)",
        description="The previously announced listing has been cancelled.",
        action="Не предпринимать действий: событие отменено.",
    )

    first = notifications.prepare((cancelled,), NOW + timedelta(minutes=5))
    notifications.commit(first, frozenset({"news-1"}))
    repeated = notifications.prepare((cancelled,), NOW + timedelta(minutes=10))

    assert first.decisions[0].should_notify is True
    assert first.decisions[0].kind is NewsNotificationDecisionKind.CANCELLED
    assert repeated.decisions[0].should_notify is False


def test_without_commit_news_remains_available_for_retry(tmp_path: Path) -> None:
    notifications = service(tmp_path)

    first = notifications.prepare((item(),), NOW)
    retry = notifications.prepare((item(),), NOW + timedelta(minutes=1))

    assert first.decisions[0].should_notify is True
    assert retry.decisions[0].should_notify is True


def test_commit_only_records_acknowledged_news(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    plan = notifications.prepare(
        (item(), item(stable_id="news-2", symbols=("XYZ",))),
        NOW,
    )

    notifications.commit(plan, frozenset({"news-1"}))

    records = JsonNewsNotificationStore(
        tmp_path / "news_notifications.json"
    ).load().records
    assert tuple(records) == ("news-1",)


def test_missing_file_is_created_and_corrupt_file_recovers(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "news_notifications.json"
    store = JsonNewsNotificationStore(path)

    assert store.load().records == {}
    assert path.exists()
    path.write_text("{broken", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        recovered = store.load()

    assert recovered.records == {}
    assert "повреждён" in caplog.text


def test_state_file_is_separate_and_atomic_json_is_valid(tmp_path: Path) -> None:
    path = tmp_path / "news_notifications.json"
    notifications = service(tmp_path)
    sent_once(notifications, item())

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert Path("data/news_notifications.json") == DEFAULT_NEWS_NOTIFICATION_STATE_PATH
    assert path.name not in {"inplay_notifications.json", "inplay_listings.json"}
    assert payload["version"] == 1
    assert payload["records"]["news-1"]["send_count"] == 1
    assert not path.with_suffix(".json.tmp").exists()


def test_records_survive_current_lookback(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())
    plan = notifications.prepare((), NOW + timedelta(days=2))
    notifications.commit(plan, frozenset())

    assert "news-1" in JsonNewsNotificationStore(
        tmp_path / "news_notifications.json"
    ).load().records


def test_old_unseen_records_are_pruned_after_retention(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    old = NOW - timedelta(days=31)
    old_item = item(published_at=old - timedelta(hours=1))
    sent_once(notifications, old_item, old)

    plan = notifications.prepare((), NOW)
    notifications.commit(plan, frozenset())

    assert JsonNewsNotificationStore(
        tmp_path / "news_notifications.json"
    ).load().records == {}


def test_future_events_are_not_pruned(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    old = NOW - timedelta(days=31)
    future_event = NOW + timedelta(days=1)
    old_item = item(
        published_at=old - timedelta(hours=1),
        event_starts_at=future_event,
    )
    sent_once(notifications, old_item, old)

    plan = notifications.prepare((), NOW)
    notifications.commit(plan, frozenset())

    assert "news-1" in JsonNewsNotificationStore(
        tmp_path / "news_notifications.json"
    ).load().records


def test_news_state_does_not_touch_inplay_files(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    sent_once(notifications, item())

    assert not (tmp_path / "inplay_notifications.json").exists()
    assert not (tmp_path / "inplay_listings.json").exists()
