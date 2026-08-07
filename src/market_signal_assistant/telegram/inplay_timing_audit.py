from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Protocol

from market_signal_assistant.inplay.audit import ScanSource
from market_signal_assistant.inplay.models import InPlayReport

_LOGGER = logging.getLogger(__name__)


class InPlayScanner(Protocol):
    def scan(
        self,
        maximum_results: int = 10,
        *,
        scan_source: ScanSource = "manual",
    ) -> InPlayReport: ...


class InPlayTimingAuditRunner:
    """Run a shadow scan without presentation or notification side effects."""

    def __init__(self, scanner: InPlayScanner) -> None:
        self._scanner = scanner
        self._run_lock = asyncio.Lock()

    async def run_once(self) -> bool:
        if self._run_lock.locked():
            _LOGGER.warning(
                "Предыдущий теневой IN PLAY audit scan ещё выполняется; "
                "новый цикл пропущен."
            )
            return False
        async with self._run_lock:
            try:
                await asyncio.to_thread(
                    self._scanner.scan,
                    maximum_results=10,
                    scan_source="timing_audit_auto",
                )
            except Exception as error:
                _LOGGER.warning(
                    "Теневой IN PLAY audit scan завершился ошибкой (%s).",
                    type(error).__name__,
                )
                return False
            return True


class InPlayTimingAuditLoop:
    """Telegram-owned lifecycle for silent diagnostic scans."""

    def __init__(
        self,
        runner: InPlayTimingAuditRunner,
        *,
        interval_seconds: float,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("IN PLAY timing audit interval must be positive.")
        self._runner = runner
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
            await self._runner.run_once()
            await asyncio.sleep(self._interval_seconds)
