from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from market_signal_assistant.inplay.audit import ScanSource
from market_signal_assistant.inplay.models import InPlayReport
from market_signal_assistant.telegram.inplay_timing_audit import (
    InPlayTimingAuditLoop,
    InPlayTimingAuditRunner,
)

NOW = datetime(2026, 8, 3, 12, tzinfo=UTC)


class Scanner:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.sources: list[ScanSource] = []
        self.called = asyncio.Event()

    def scan(
        self,
        maximum_results: int = 10,
        *,
        scan_source: ScanSource = "manual",
    ) -> InPlayReport:
        assert maximum_results == 10
        self.sources.append(scan_source)
        self.called.set()
        if self.error is not None:
            raise self.error
        return InPlayReport(NOW, ())


def test_audit_runner_scans_without_sender_or_notification_service() -> None:
    scanner = Scanner()

    result = asyncio.run(InPlayTimingAuditRunner(scanner).run_once())

    assert result is True
    assert scanner.sources == ["timing_audit_auto"]


def test_audit_runner_failure_is_controlled() -> None:
    scanner = Scanner(error=RuntimeError("offline"))

    result = asyncio.run(InPlayTimingAuditRunner(scanner).run_once())

    assert result is False


def test_audit_loop_runs_immediately_and_stops_cleanly() -> None:
    scanner = Scanner()

    async def lifecycle() -> bool:
        loop = InPlayTimingAuditLoop(
            InPlayTimingAuditRunner(scanner),
            interval_seconds=300,
        )
        loop.start()
        await asyncio.wait_for(scanner.called.wait(), timeout=1)
        running = loop.running
        await loop.stop()
        return running and not loop.running

    assert asyncio.run(lifecycle()) is True
