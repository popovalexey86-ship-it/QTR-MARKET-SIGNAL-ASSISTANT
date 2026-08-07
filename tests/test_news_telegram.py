from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_signal_assistant.application.models import MarketSummary, ScreeningReport
from market_signal_assistant.news.models import (
    NewsCategory,
    NewsImportance,
    NewsItem,
    NewsReport,
)
from market_signal_assistant.news.notifications import (
    JsonNewsNotificationStore,
    NewsNotificationService,
)
from market_signal_assistant.news.provider import NewsDataError
from market_signal_assistant.telegram.bot import HELP_TEXT, execute_command
from market_signal_assistant.telegram.formatting import format_news_report
from market_signal_assistant.telegram.parsing import parse_command

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
EMPTY_SCREENING_REPORT = ScreeningReport(
    NOW, (), (), (), MarketSummary(0, 0, 0, 0, 0, 0)
)


def item(
    *,
    importance: NewsImportance = NewsImportance.HIGH,
    category: NewsCategory = NewsCategory.LISTING,
    published_at: datetime = NOW - timedelta(hours=2),
) -> NewsItem:
    return NewsItem(
        stable_id="stable",
        source="Bybit",
        title="New Listing: Alpha (ABC)",
        description="Bybit объявляет новый листинг ABC.",
        url="https://announcements.bybit.com/en-US/article/abc/",
        category=category,
        importance=importance,
        symbols=("ABC",),
        published_at=published_at,
        event_starts_at=None,
        tags=("Spot",),
        reason="Новый актив становится доступен для торговли.",
        recommended_action=(
            "Не входить сразу после запуска торгов. Дождаться появления "
            "ликвидности и стабилизации спреда."
        ),
    )


class Screening:
    def screen(self, request: object) -> ScreeningReport:
        del request
        return EMPTY_SCREENING_REPORT


class News:
    def __init__(self, report: NewsReport | Exception) -> None:
        self.report = report
        self.calls = 0

    def get_important(self) -> NewsReport:
        self.calls += 1
        if isinstance(self.report, Exception):
            raise self.report
        return self.report


def test_news_command_and_help_are_available() -> None:
    assert parse_command("/news").name == "news"
    assert "/news — важные официальные объявления Bybit" in HELP_TEXT


def test_news_telegram_format_contains_plain_action_and_official_url() -> None:
    message = format_news_report(NewsReport(NOW, 24, (item(),)))[0]

    assert message.startswith("📰 ВАЖНЫЕ НОВОСТИ")
    assert "🟠 ВАЖНО — НОВЫЙ ЛИСТИНГ" in message
    assert "ABC" in message
    assert "🕒 Опубликовано: 2 часа назад" in message
    assert "⚠️ Почему важно:" in message
    assert "🛡 Что делать:" in message
    assert "🔗 Источник: Bybit" in message
    assert "https://announcements.bybit.com/en-US/article/abc/" in message
    assert "ЛОНГ" not in message
    assert "ШОРТ" not in message


def test_news_event_time_is_shown_in_utc() -> None:
    news = item(category=NewsCategory.MAINTENANCE)
    news = NewsItem(
        stable_id=news.stable_id,
        source=news.source,
        title=news.title,
        description=news.description,
        url=news.url,
        category=news.category,
        importance=NewsImportance.MEDIUM,
        symbols=news.symbols,
        published_at=news.published_at,
        event_starts_at=NOW + timedelta(hours=1),
        tags=news.tags,
        reason=news.reason,
        recommended_action=news.recommended_action,
    )

    message = format_news_report(NewsReport(NOW, 24, (news,)))[0]

    assert "Начало события: 2026-08-02 13:00 UTC" in message


def test_empty_news_report_returns_exact_short_message() -> None:
    assert format_news_report(NewsReport(NOW, 24, ())) == (
        "📰 Важных новостей за последние 24 часа не найдено.",
    )


def test_news_network_failure_returns_safe_message() -> None:
    execution = execute_command(
        "/news",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        news_service=News(NewsDataError("raw response must not leak")),
    )

    assert execution.messages == (
        "⚠️ Не удалось получить новости Bybit.\n"
        "Попробуйте повторить команду позже.",
    )


def test_disabled_news_module_does_not_call_service() -> None:
    news = News(NewsReport(NOW, 24, (item(),)))

    execution = execute_command(
        "/news",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        news_service=news,
        news_enabled=False,
    )

    assert execution.messages == ("📰 Новостной модуль отключён.",)
    assert news.calls == 0


def test_news_command_uses_service_and_existing_inplay_command_is_unchanged() -> None:
    news = News(NewsReport(NOW, 24, (item(),)))

    execution = execute_command(
        "/news",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        news_service=news,
    )

    assert news.calls == 1
    assert execution.report is None
    assert "📰 ВАЖНЫЕ НОВОСТИ" in execution.messages[0]


def test_manual_news_is_independent_from_notification_state(tmp_path: Path) -> None:
    news_item = item()
    notifications = NewsNotificationService(
        JsonNewsNotificationStore(tmp_path / "news_notifications.json")
    )
    plan = notifications.prepare((news_item,), NOW)
    notifications.commit(plan, frozenset({news_item.stable_id}))

    execution = execute_command(
        "/news",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        news_service=News(NewsReport(NOW, 24, (news_item,))),
    )

    assert news_item.description in execution.messages[0]
