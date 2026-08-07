from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol

from market_signal_assistant.inplay.audit import ScanSource
from market_signal_assistant.inplay.models import (
    InPlayReport,
    InPlayResult,
)
from market_signal_assistant.inplay.notifications import InPlayNotificationService
from market_signal_assistant.inplay.safety import (
    AutomaticDisplayStatus,
    automatic_semantics,
)
from market_signal_assistant.inplay.service import INPLAY_MIN_SCORE
from market_signal_assistant.telegram.formatting import format_auto_inplay_results

AUTO_DIRECTIONAL_MIN_SCORE = 60.0
AUTO_WATCH_MIN_SCORE = 70.0
AUTO_MAXIMUM_RESULTS = 3

_LOGGER = logging.getLogger(__name__)


class InPlayScanner(Protocol):
    def scan(
        self,
        maximum_results: int = 10,
        *,
        scan_source: ScanSource = "manual",
    ) -> InPlayReport: ...


AutoSender = Callable[[int, str], Awaitable[None]]


class InPlayAutoNotifier:
    """Run one automatic scan and acknowledge only delivered notifications."""

    def __init__(
        self,
        *,
        scanner: InPlayScanner,
        notification_service: InPlayNotificationService,
        allowed_chat_ids: frozenset[int],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._scanner = scanner
        self._notifications = notification_service
        self._allowed_chat_ids = allowed_chat_ids
        self._clock = clock or (lambda: datetime.now(UTC))
        self._scan_lock = asyncio.Lock()

    async def run_once(self, send: AutoSender) -> bool:
        """Return true only when one combined message was fully delivered."""
        if not self._allowed_chat_ids:
            _LOGGER.warning(
                "Автосканирование IN PLAY отключено для отправки: "
                "TELEGRAM_ALLOWED_CHAT_IDS не задан."
            )
            return False
        if self._scan_lock.locked():
            _LOGGER.warning(
                "Предыдущий цикл IN PLAY ещё выполняется; новый цикл пропущен."
            )
            return False

        async with self._scan_lock:
            try:
                report = await asyncio.to_thread(
                    self._scanner.scan,
                    maximum_results=10,
                    scan_source="inplay_auto",
                )
                plan = await asyncio.to_thread(
                    self._notifications.prepare,
                    report.results,
                    self._clock(),
                )
            except Exception as error:
                _log_failure("Сканирование IN PLAY завершилось ошибкой", error)
                return False

            selected = tuple(
                sorted(
                    (
                        decision.result
                        for decision in plan.decisions
                        if decision.should_notify
                        and _passes_automatic_threshold(decision.result)
                    ),
                    key=lambda item: item.inplay_score,
                    reverse=True,
                )[:AUTO_MAXIMUM_RESULTS]
            )
            if not selected:
                try:
                    await asyncio.to_thread(
                        self._notifications.commit,
                        plan,
                        frozenset(),
                    )
                except Exception as error:
                    _log_failure("Не удалось обновить состояние IN PLAY", error)
                return False

            message = format_auto_inplay_results(selected)
            try:
                for chat_id in sorted(self._allowed_chat_ids):
                    await send(chat_id, message)
            except Exception as error:
                _log_failure("Telegram не доставил IN PLAY-уведомление", error)
                return False

            sent_symbols = frozenset(item.symbol for item in selected)
            try:
                await asyncio.to_thread(
                    self._notifications.commit,
                    plan,
                    sent_symbols,
                )
            except Exception as error:
                _log_failure(
                    "Уведомление отправлено, но состояние IN PLAY не сохранено",
                    error,
                )
                return False
            return True


class InPlayAutoLoop:
    """An in-process asyncio loop owned by the Telegram application lifecycle."""

    def __init__(
        self,
        notifier: InPlayAutoNotifier,
        send: AutoSender,
        *,
        interval_seconds: float,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("IN PLAY interval must be positive.")
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
            await self._notifier.run_once(self._send)
            await asyncio.sleep(self._interval_seconds)


def _passes_automatic_threshold(result: InPlayResult) -> bool:
    semantics = automatic_semantics(result)
    if semantics.display_status is AutomaticDisplayStatus.LATE_ENTRY:
        return False
    if semantics.display_status is AutomaticDisplayStatus.DO_NOT_CHASE:
        return semantics.protective_watch_allowed
    if semantics.directional_entry_allowed:
        return result.inplay_score >= AUTO_DIRECTIONAL_MIN_SCORE
    if result.is_new_listing:
        return result.inplay_score >= INPLAY_MIN_SCORE
    return result.inplay_score >= AUTO_WATCH_MIN_SCORE


def _log_failure(message: str, error: Exception) -> None:
    _LOGGER.warning("%s (%s).", message, type(error).__name__)
