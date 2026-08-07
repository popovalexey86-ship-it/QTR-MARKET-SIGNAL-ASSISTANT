from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from market_signal_assistant.application.models import (
    MarketSummary,
    ScreeningReport,
    ScreeningRequest,
)
from market_signal_assistant.inplay.models import InPlayReport
from market_signal_assistant.inplay.notifications import (
    InPlayNotificationService,
    JsonInPlayNotificationStore,
)
from market_signal_assistant.news.models import (
    NewsCategory,
    NewsImportance,
    NewsItem,
    NewsReport,
)
from market_signal_assistant.news.notifications import (
    JsonNewsNotificationStore,
    NewsNotificationDecision,
    NewsNotificationRecord,
    NewsNotificationService,
)
from market_signal_assistant.news.provider import NewsDataError
from market_signal_assistant.settings import (
    InPlayAutoSettings,
    NewsAutoSettings,
    TelegramSettings,
)
from market_signal_assistant.telegram.bot import (
    _run_sdk_bot_handlers,
    execute_command,
)
from market_signal_assistant.telegram.formatting import (
    format_auto_news_event,
    pack_auto_news_sections,
)
from market_signal_assistant.telegram.inplay_auto import InPlayAutoNotifier
from market_signal_assistant.telegram.news_auto import (
    NewsAutoLoop,
    NewsAutoNotifier,
)

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def item(
    stable_id: str,
    importance: NewsImportance,
    *,
    minutes_ago: int = 10,
    category: NewsCategory = NewsCategory.LISTING,
    title: str | None = None,
    event_starts_at: datetime | None = None,
    published_at: datetime | None = None,
) -> NewsItem:
    symbol = stable_id.upper()
    return NewsItem(
        stable_id=stable_id,
        source="Bybit",
        title=title or f"New Listing: {symbol}",
        description=f"Bybit объявила важное событие для {symbol}.",
        url=f"https://announcements.bybit.com/{stable_id}",
        category=category,
        importance=importance,
        symbols=(f"{symbol}USDT",),
        published_at=published_at or NOW - timedelta(minutes=minutes_ago),
        event_starts_at=event_starts_at,
        tags=("Spot",),
        reason="Событие может изменить рыночные условия.",
        recommended_action="Проверить официальные условия и учитывать риск.",
    )


class Reader:
    def __init__(self, *items: NewsItem) -> None:
        self.items = items
        self.calls = 0

    def get_important(self) -> NewsReport:
        self.calls += 1
        return NewsReport(NOW, 24, self.items)


class FailingReader:
    def get_important(self) -> NewsReport:
        raise NewsDataError("raw payload must not be logged")


class EmptyInPlayScanner:
    def scan(
        self,
        maximum_results: int = 10,
        *,
        scan_source: str = "manual",
    ) -> InPlayReport:
        del maximum_results, scan_source
        return InPlayReport(NOW, ())


class Screening:
    def screen(self, request: ScreeningRequest) -> ScreeningReport:
        del request
        return ScreeningReport(NOW, (), (), (), MarketSummary(0, 0, 0, 0, 0, 0))


def notifier(
    tmp_path: Path,
    reader: Reader | FailingReader,
    *,
    chat_ids: frozenset[int] = frozenset({100}),
) -> NewsAutoNotifier:
    notifications = NewsNotificationService(
        JsonNewsNotificationStore(tmp_path / "news_notifications.json")
    )
    return NewsAutoNotifier(
        reader=reader,
        notification_service=notifications,
        allowed_chat_ids=chat_ids,
        clock=lambda: NOW,
    )


def run_once(
    auto: NewsAutoNotifier,
    sent: list[tuple[int, str]],
) -> bool:
    async def send(chat_id: int, text: str) -> None:
        sent.append((chat_id, text))

    return asyncio.run(auto.run_once(send))


def test_news_auto_settings_default_to_disabled_and_sixty_minutes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NEWS_AUTO_ENABLED", raising=False)
    monkeypatch.delenv("NEWS_SCAN_INTERVAL_MINUTES", raising=False)

    assert NewsAutoSettings.from_environment() == NewsAutoSettings(False, 60)


def test_news_auto_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEWS_AUTO_ENABLED", "true")
    monkeypatch.setenv("NEWS_SCAN_INTERVAL_MINUTES", "120")

    assert NewsAutoSettings.from_environment() == NewsAutoSettings(True, 120)


@pytest.mark.parametrize("minutes", [14, 1441])
def test_news_auto_interval_outside_range_is_rejected(minutes: int) -> None:
    with pytest.raises(ValueError, match="15 до 1440"):
        NewsAutoSettings(interval_minutes=minutes)


@pytest.mark.parametrize(
    ("importance", "expected"),
    [
        (NewsImportance.CRITICAL, True),
        (NewsImportance.HIGH, True),
        (NewsImportance.MEDIUM, False),
        (NewsImportance.LOW, False),
    ],
)
def test_only_high_and_critical_initial_news_are_automatic(
    tmp_path: Path,
    importance: NewsImportance,
    expected: bool,
) -> None:
    sent: list[tuple[int, str]] = []

    outcome = run_once(notifier(tmp_path, Reader(item("one", importance))), sent)

    assert outcome is expected
    assert bool(sent) is expected


def test_initial_updated_and_cancelled_formats(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    first_item = item("abc", NewsImportance.HIGH)
    auto = notifier(tmp_path, Reader(first_item))
    assert run_once(auto, sent) is True
    assert "🚨 ВАЖНЫЕ НОВОСТИ РЫНКА" in sent[-1][1]
    assert "🟠 НОВОЕ — НОВЫЙ ЛИСТИНГ" in sent[-1][1]

    updated = item("abc", NewsImportance.CRITICAL)
    auto = notifier(tmp_path, Reader(updated))
    assert run_once(auto, sent) is True
    assert "🔄 ОБНОВЛЕНО — НОВЫЙ ЛИСТИНГ" in sent[-1][1]

    cancelled = item(
        "abc",
        NewsImportance.CRITICAL,
        title="Cancelled: New Listing ABC",
    )
    auto = notifier(tmp_path, Reader(cancelled))
    assert run_once(auto, sent) is True
    assert "✅ СОБЫТИЕ ОТМЕНЕНО — НОВЫЙ ЛИСТИНГ" in sent[-1][1]


def test_priority_order_and_maximum_three(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    initial = (
        item("high-old", NewsImportance.HIGH, minutes_ago=40),
        item("critical-old", NewsImportance.CRITICAL, minutes_ago=30),
        item("critical-new", NewsImportance.CRITICAL, minutes_ago=5),
        item("high-new", NewsImportance.HIGH, minutes_ago=2),
    )

    assert run_once(notifier(tmp_path, Reader(*initial)), sent) is True

    message = sent[0][1]
    assert message.index("CRITICAL-NEWUSDT") < message.index("CRITICAL-OLDUSDT")
    assert message.index("CRITICAL-OLDUSDT") < message.index("HIGH-NEWUSDT")
    assert "HIGH-OLDUSDT" not in message
    assert message.count("Источник: Bybit") == 3


def test_cancelled_and_updated_priority_precedes_initial(tmp_path: Path) -> None:
    notifications = NewsNotificationService(
        JsonNewsNotificationStore(tmp_path / "news_notifications.json")
    )
    originals = (
        item("cancel", NewsImportance.HIGH),
        item("critical-update", NewsImportance.HIGH),
        item(
            "high-update",
            NewsImportance.HIGH,
            event_starts_at=NOW + timedelta(hours=2),
        ),
    )
    plan = notifications.prepare(originals, NOW)
    notifications.commit(
        plan,
        frozenset(news.stable_id for news in originals),
    )
    current = (
        item(
            "cancel",
            NewsImportance.HIGH,
            title="Cancelled: scheduled event",
        ),
        item("critical-update", NewsImportance.CRITICAL),
        item("critical-initial", NewsImportance.CRITICAL),
        item(
            "high-update",
            NewsImportance.HIGH,
            event_starts_at=NOW + timedelta(hours=3),
        ),
        item("high-initial", NewsImportance.HIGH),
    )
    auto = NewsAutoNotifier(
        reader=Reader(*current),
        notification_service=notifications,
        allowed_chat_ids=frozenset({100}),
        clock=lambda: NOW,
    )
    sent: list[tuple[int, str]] = []

    assert run_once(auto, sent) is True

    message = sent[0][1]
    assert message.index("CANCELUSDT") < message.index("CRITICAL-UPDATEUSDT")
    assert message.index("CRITICAL-UPDATEUSDT") < message.index(
        "CRITICAL-INITIALUSDT"
    )
    assert "HIGH-UPDATEUSDT" not in message
    assert "HIGH-INITIALUSDT" not in message
    records = JsonNewsNotificationStore(
        tmp_path / "news_notifications.json"
    ).load().records
    assert "high-initial" not in records


def test_updated_event_time_shows_confirmed_before_and_after(tmp_path: Path) -> None:
    notifications = NewsNotificationService(
        JsonNewsNotificationStore(tmp_path / "news_notifications.json")
    )
    before = NOW + timedelta(hours=2)
    original = item(
        "maintenance",
        NewsImportance.HIGH,
        category=NewsCategory.MAINTENANCE,
        event_starts_at=before,
    )
    plan = notifications.prepare((original,), NOW)
    notifications.commit(plan, frozenset({original.stable_id}))
    after = NOW + timedelta(hours=3)
    updated = item(
        "maintenance",
        NewsImportance.HIGH,
        category=NewsCategory.MAINTENANCE,
        event_starts_at=after,
    )
    auto = NewsAutoNotifier(
        reader=Reader(updated),
        notification_service=notifications,
        allowed_chat_ids=frozenset({100}),
        clock=lambda: NOW,
    )
    sent: list[tuple[int, str]] = []

    assert run_once(auto, sent) is True
    assert "Было: 2026-08-02 14:00 UTC" in sent[0][1]
    assert "Стало: 2026-08-02 15:00 UTC" in sent[0][1]


def test_empty_result_sends_no_message(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []

    assert run_once(notifier(tmp_path, Reader()), sent) is False
    assert sent == []


def test_messages_split_only_between_complete_news_events() -> None:
    sections = ("A" * 70, "B" * 70, "C" * 70)

    messages = pack_auto_news_sections(sections, limit=120)

    assert len(messages) == 3
    assert all(len(message) <= 120 for message in messages)
    assert all(message.startswith("🚨 ВАЖНЫЕ НОВОСТИ РЫНКА") for message in messages)
    assert "A" * 70 in messages[0]
    assert "B" * 70 in messages[1]
    assert "C" * 70 in messages[2]


def test_one_formatting_failure_skips_only_that_event(
    tmp_path: Path,
) -> None:
    def format_or_fail(
        decision: NewsNotificationDecision,
        previous: NewsNotificationRecord | None,
        generated_at: datetime,
    ) -> str:
        if decision.item.stable_id == "bad":
            raise ValueError("broken item")
        return format_auto_news_event(decision, previous, generated_at)

    sent: list[tuple[int, str]] = []
    auto = NewsAutoNotifier(
        reader=Reader(
            item("bad", NewsImportance.CRITICAL),
            item("good", NewsImportance.HIGH),
        ),
        notification_service=NewsNotificationService(
            JsonNewsNotificationStore(tmp_path / "news_notifications.json")
        ),
        allowed_chat_ids=frozenset({100}),
        clock=lambda: NOW,
        formatter=format_or_fail,
    )

    assert run_once(auto, sent) is True
    assert "GOODUSDT" in sent[0][1]
    assert "BADUSDT" not in sent[0][1]


def test_duplicate_is_suppressed_after_restart(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    news = item("abc", NewsImportance.HIGH)

    assert run_once(notifier(tmp_path, Reader(news)), sent) is True
    assert run_once(notifier(tmp_path, Reader(news)), sent) is False
    assert len(sent) == 1


def test_commit_happens_only_after_delivery_to_all_chats(tmp_path: Path) -> None:
    path = tmp_path / "news_notifications.json"
    auto = notifier(
        tmp_path,
        Reader(item("abc", NewsImportance.HIGH)),
        chat_ids=frozenset({100, 200}),
    )

    async def failed_send(chat_id: int, text: str) -> None:
        del text
        if chat_id == 200:
            raise RuntimeError("telegram failed")

    assert asyncio.run(auto.run_once(failed_send)) is False
    assert JsonNewsNotificationStore(path).load().records == {}


def test_empty_allowlist_blocks_cycle_even_when_allow_all_exists_elsewhere(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reader = Reader(item("abc", NewsImportance.HIGH))
    sent: list[tuple[int, str]] = []

    with caplog.at_level(logging.WARNING):
        outcome = run_once(
            notifier(tmp_path, reader, chat_ids=frozenset()),
            sent,
        )

    assert outcome is False
    assert reader.calls == 0
    assert "TELEGRAM_ALLOWED_CHAT_IDS" in caplog.text


def test_network_failure_keeps_bot_cycle_alive_and_logs_only_error_type(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sent: list[tuple[int, str]] = []

    with caplog.at_level(logging.WARNING):
        outcome = run_once(notifier(tmp_path, FailingReader()), sent)

    assert outcome is False
    assert sent == []
    assert "NewsDataError" in caplog.text
    assert "raw payload" not in caplog.text


def test_two_news_scans_do_not_run_concurrently(tmp_path: Path) -> None:
    started = threading.Event()
    release = threading.Event()

    class BlockingReader(Reader):
        def get_important(self) -> NewsReport:
            started.set()
            release.wait(timeout=2)
            return super().get_important()

    auto = notifier(
        tmp_path,
        BlockingReader(item("abc", NewsImportance.HIGH)),
    )
    sent: list[tuple[int, str]] = []

    async def scenario() -> tuple[bool, bool]:
        async def send(chat_id: int, text: str) -> None:
            sent.append((chat_id, text))

        first = asyncio.create_task(auto.run_once(send))
        await asyncio.to_thread(started.wait, 1)
        second = await auto.run_once(send)
        release.set()
        return await first, second

    assert asyncio.run(scenario()) == (True, False)
    assert len(sent) == 1


def test_news_loop_runs_immediately_and_stops_cleanly(tmp_path: Path) -> None:
    auto = notifier(tmp_path, Reader(item("abc", NewsImportance.HIGH)))
    sent = asyncio.Event()

    async def scenario() -> None:
        async def send(chat_id: int, text: str) -> None:
            del chat_id, text
            sent.set()

        loop = NewsAutoLoop(auto, send, interval_seconds=3600)
        loop.start()
        await asyncio.wait_for(sent.wait(), timeout=1)
        await loop.stop()
        assert loop.running is False

    asyncio.run(scenario())


def test_news_and_inplay_use_independent_scan_locks(tmp_path: Path) -> None:
    news = notifier(tmp_path, Reader())
    inplay = InPlayAutoNotifier(
        scanner=EmptyInPlayScanner(),
        notification_service=InPlayNotificationService(
            JsonInPlayNotificationStore(tmp_path / "inplay_notifications.json")
        ),
        allowed_chat_ids=frozenset({100}),
    )

    assert news._scan_lock is not inplay._scan_lock


def test_telegram_lifecycle_starts_and_stops_news_loop(tmp_path: Path) -> None:
    reader = Reader(
        item(
            "abc",
            NewsImportance.HIGH,
            published_at=datetime.now(UTC) - timedelta(minutes=10),
        )
    )
    notifications = NewsNotificationService(
        JsonNewsNotificationStore(tmp_path / "news_notifications.json")
    )
    inplay_notifications = InPlayNotificationService(
        JsonInPlayNotificationStore(tmp_path / "inplay_notifications.json")
    )

    class FakeBot:
        def __init__(self) -> None:
            self.messages: list[tuple[int, str]] = []

        async def send_message(self, *, chat_id: int, text: str) -> None:
            self.messages.append((chat_id, text))

    class FakeApplication:
        def __init__(self, builder: FakeBuilder) -> None:
            self._builder = builder
            self.bot = FakeBot()

        def add_handler(self, handler: object) -> None:
            del handler

        def run_polling(self) -> None:
            async def lifecycle() -> None:
                assert self._builder.on_start is not None
                assert self._builder.on_stop is not None
                await self._builder.on_start(self)
                for _ in range(100):
                    if self.bot.messages:
                        break
                    await asyncio.sleep(0.001)
                await self._builder.on_stop(self)

            asyncio.run(lifecycle())

    class FakeBuilder:
        def __init__(self) -> None:
            self.on_start: Any = None
            self.on_stop: Any = None
            self.application: FakeApplication | None = None

        def token(self, value: str) -> FakeBuilder:
            assert value == "token"
            return self

        def post_init(self, callback: Any) -> FakeBuilder:
            self.on_start = callback
            return self

        def post_shutdown(self, callback: Any) -> FakeBuilder:
            self.on_stop = callback
            return self

        def build(self) -> FakeApplication:
            self.application = FakeApplication(self)
            return self.application

    builder = FakeBuilder()
    _run_sdk_bot_handlers(
        TelegramSettings("token", frozenset({100})),
        Screening(),
        EmptyInPlayScanner(),
        False,
        InPlayAutoSettings(False, 15),
        inplay_notifications,
        lambda: builder,
        lambda *args: args,
        SimpleNamespace(COMMAND=object()),
        news_service=reader,
        news_auto_settings=NewsAutoSettings(True, 60),
        news_notification_service=notifications,
    )

    assert builder.application is not None
    assert len(builder.application.bot.messages) == 1
    assert reader.calls == 1


def test_allow_all_does_not_bypass_empty_auto_news_allowlist(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reader = Reader(item("abc", NewsImportance.HIGH))

    class FakeApplication:
        def add_handler(self, handler: object) -> None:
            del handler

        def run_polling(self) -> None:
            return

    class FakeBuilder:
        def token(self, value: str) -> FakeBuilder:
            del value
            return self

        def post_init(self, callback: Any) -> FakeBuilder:
            del callback
            raise AssertionError("news lifecycle must not start")

        def post_shutdown(self, callback: Any) -> FakeBuilder:
            del callback
            return self

        def build(self) -> FakeApplication:
            return FakeApplication()

    with caplog.at_level(logging.WARNING):
        _run_sdk_bot_handlers(
            TelegramSettings("token", frozenset(), allow_all=True),
            Screening(),
            EmptyInPlayScanner(),
            False,
            InPlayAutoSettings(False, 15),
            InPlayNotificationService(
                JsonInPlayNotificationStore(tmp_path / "inplay_notifications.json")
            ),
            FakeBuilder,
            lambda *args: args,
            SimpleNamespace(COMMAND=object()),
            news_service=reader,
            news_auto_settings=NewsAutoSettings(True, 60),
            news_notification_service=NewsNotificationService(
                JsonNewsNotificationStore(tmp_path / "news_notifications.json")
            ),
        )

    assert reader.calls == 0
    assert "TELEGRAM_ALLOWED_CHAT_IDS" in caplog.text


def test_status_shows_news_auto_state_and_interval() -> None:
    execution = execute_command(
        "/status",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        news_auto_enabled=True,
        news_scan_interval_minutes=90,
    )

    assert "Автоновости: включены" in execution.messages[0]
    assert "Интервал новостей: 90 минут" in execution.messages[0]
