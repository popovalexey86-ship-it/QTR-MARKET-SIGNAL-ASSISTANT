from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Protocol

from market_signal_assistant.inplay.early_discovery import EarlyDiscoveryScanReport

_LOGGER = logging.getLogger(__name__)


class EarlyDiscoveryScanner(Protocol):
    def scan(self) -> EarlyDiscoveryScanReport: ...


class EarlyDiscoveryRunner:
    """Run silent scans with overlap protection and no notification dependency."""

    def __init__(self, scanner: EarlyDiscoveryScanner) -> None:
        self._scanner = scanner
        self._run_lock = asyncio.Lock()

    async def run_once(self) -> bool:
        if self._run_lock.locked():
            _LOGGER.warning(
                "Предыдущий Early Discovery scan ещё выполняется; цикл пропущен."
            )
            return False
        async with self._run_lock:
            try:
                await asyncio.to_thread(self._scanner.scan)
            except Exception as error:
                _LOGGER.warning(
                    "Early Discovery scan завершился ошибкой (%s).",
                    type(error).__name__,
                )
                return False
            return True


class EarlyDiscoveryLoop:
    """Fixed-schedule Telegram-owned lifecycle for silent discovery scans."""

    def __init__(
        self,
        runner: EarlyDiscoveryRunner,
        *,
        interval_seconds: float,
        monotonic: Callable[[], float] | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("Early Discovery interval must be positive.")
        self._runner = runner
        self._interval_seconds = interval_seconds
        self._monotonic = monotonic or time.monotonic
        self._sleeper = sleeper or asyncio.sleep
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
        next_run = self._monotonic()
        while True:
            delay = max(0.0, next_run - self._monotonic())
            if delay > 0:
                await self._sleeper(delay)
            next_run += self._interval_seconds
            await self._runner.run_once()
            now = self._monotonic()
            if now >= next_run:
                missed = int((now - next_run) // self._interval_seconds) + 1
                next_run += missed * self._interval_seconds
