from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Protocol

from market_signal_assistant.news.models import NewsImportance, NewsReport
from market_signal_assistant.news.notifications import (
    NewsNotificationDecision,
    NewsNotificationDecisionKind,
    NewsNotificationPlan,
    NewsNotificationRecord,
    NewsNotificationService,
)
from market_signal_assistant.telegram.formatting import (
    format_auto_news_event,
    pack_auto_news_sections,
)

AUTO_NEWS_MAXIMUM_EVENTS = 3

_LOGGER = logging.getLogger(__name__)


class NewsReader(Protocol):
    def get_important(self) -> NewsReport: ...


NewsAutoSender = Callable[[int, str], Awaitable[None]]
NewsEventFormatter = Callable[
    [NewsNotificationDecision, NewsNotificationRecord | None, datetime],
    str,
]


class NewsAutoNotifier:
    """Fetch, deduplicate and acknowledge one automatic news cycle."""

    def __init__(
        self,
        *,
        reader: NewsReader,
        notification_service: NewsNotificationService,
        allowed_chat_ids: frozenset[int],
        clock: Callable[[], datetime] | None = None,
        formatter: NewsEventFormatter = format_auto_news_event,
    ) -> None:
        self._reader = reader
        self._notifications = notification_service
        self._allowed_chat_ids = allowed_chat_ids
        self._clock = clock or (lambda: datetime.now(UTC))
        self._formatter = formatter
        self._scan_lock = asyncio.Lock()

    async def run_once(self, send: NewsAutoSender) -> bool:
        """Return true only after every selected event reaches every chat."""
        if not self._allowed_chat_ids:
            _LOGGER.warning(
                "Автоновости отключены: TELEGRAM_ALLOWED_CHAT_IDS не задан."
            )
            return False
        if self._scan_lock.locked():
            _LOGGER.warning(
                "Предыдущий новостной цикл ещё выполняется; новый цикл пропущен."
            )
            return False

        async with self._scan_lock:
            try:
                report = await asyncio.to_thread(self._reader.get_important)
                plan = await asyncio.to_thread(
                    self._notifications.prepare,
                    report.items,
                    self._clock(),
                )
            except Exception as error:
                _log_failure(
                    "Автоматическое получение новостей завершилось ошибкой",
                    error,
                )
                return False

            sections, selected_ids = _select_and_format(plan, self._formatter)
            if not sections:
                await self._commit_passive(plan)
                return False

            messages = pack_auto_news_sections(sections)
            try:
                for chat_id in sorted(self._allowed_chat_ids):
                    for message in messages:
                        await send(chat_id, message)
            except Exception as error:
                _log_failure("Telegram не доставил новостное уведомление", error)
                return False

            try:
                await asyncio.to_thread(
                    self._notifications.commit,
                    plan,
                    selected_ids,
                )
            except Exception as error:
                _log_failure(
                    "Новости отправлены, но notification state не сохранён",
                    error,
                )
                return False
            return True

    async def _commit_passive(self, plan: NewsNotificationPlan) -> None:
        try:
            await asyncio.to_thread(
                self._notifications.commit,
                plan,
                frozenset(),
            )
        except Exception as error:
            _log_failure("Не удалось обновить пассивное news state", error)


class NewsAutoLoop:
    """In-process periodic task owned by Telegram application lifecycle."""

    def __init__(
        self,
        notifier: NewsAutoNotifier,
        send: NewsAutoSender,
        *,
        interval_seconds: float,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("News interval must be positive.")
        self._notifier = notifier
        self._send = send
        self._interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.running:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while True:
            try:
                await self._notifier.run_once(self._send)
            except Exception as error:
                _log_failure("Необработанная ошибка новостного цикла", error)
            await asyncio.sleep(self._interval_seconds)


def _select_and_format(
    plan: NewsNotificationPlan,
    formatter: NewsEventFormatter,
) -> tuple[tuple[str, ...], frozenset[str]]:
    ranked = sorted(
        (
            decision
            for decision in plan.decisions
            if _automatic_priority(decision, plan.original_records) is not None
        ),
        key=lambda decision: (
            _automatic_priority(decision, plan.original_records),
            -decision.item.published_at.timestamp(),
        ),
    )
    sections: list[str] = []
    selected_ids: set[str] = set()
    for decision in ranked:
        previous = plan.original_records.get(decision.item.stable_id)
        try:
            section = formatter(
                decision,
                previous,
                plan.observed_at,
            )
            pack_auto_news_sections((section,))
        except Exception as error:
            _log_failure(
                f"Пропущено форматирование новости {decision.item.stable_id}",
                error,
            )
            continue
        sections.append(section)
        selected_ids.add(decision.item.stable_id)
        if len(sections) == AUTO_NEWS_MAXIMUM_EVENTS:
            break
    return tuple(sections), frozenset(selected_ids)


def _automatic_priority(
    decision: NewsNotificationDecision,
    previous: Mapping[str, NewsNotificationRecord],
) -> int | None:
    if not decision.should_notify:
        return None
    kind = decision.kind
    importance = decision.item.importance
    if kind is NewsNotificationDecisionKind.CANCELLED:
        return 0 if decision.item.stable_id in previous else None
    priorities = {
        (NewsImportance.CRITICAL, NewsNotificationDecisionKind.UPDATED): 1,
        (NewsImportance.CRITICAL, NewsNotificationDecisionKind.INITIAL): 2,
        (NewsImportance.HIGH, NewsNotificationDecisionKind.UPDATED): 3,
        (NewsImportance.HIGH, NewsNotificationDecisionKind.INITIAL): 4,
    }
    return priorities.get((importance, kind))


def _log_failure(message: str, error: Exception) -> None:
    _LOGGER.warning("%s (%s).", message, type(error).__name__)
