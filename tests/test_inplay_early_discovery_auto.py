from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import cast

from market_signal_assistant.inplay.early_discovery import EarlyDiscoveryScanReport
from market_signal_assistant.telegram.inplay_early_discovery import (
    EarlyDiscoveryLoop,
    EarlyDiscoveryRunner,
)

NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)


def report() -> EarlyDiscoveryScanReport:
    return EarlyDiscoveryScanReport(NOW, NOW, 0, 0, 0, 0, ())


class Scanner:
    def __init__(self) -> None:
        self.calls = 0
        self.started = threading.Event()
        self.release = threading.Event()

    def scan(self) -> EarlyDiscoveryScanReport:
        self.calls += 1
        self.started.set()
        self.release.wait(timeout=1)
        return report()


def test_runner_does_not_start_parallel_scans() -> None:
    scanner = Scanner()

    async def scenario() -> tuple[bool, bool]:
        runner = EarlyDiscoveryRunner(scanner)
        first = asyncio.create_task(runner.run_once())
        await asyncio.to_thread(scanner.started.wait, 1)
        second = await runner.run_once()
        scanner.release.set()
        return await first, second

    first, second = asyncio.run(scenario())

    assert first is True
    assert second is False
    assert scanner.calls == 1


class ScheduledRunner:
    def __init__(self, clock: list[float], durations: tuple[float, ...]) -> None:
        self.clock = clock
        self.durations = durations
        self.calls = 0

    async def run_once(self) -> bool:
        self.clock[0] += self.durations[self.calls]
        self.calls += 1
        return True


def run_schedule(durations: tuple[float, ...]) -> tuple[list[float], int]:
    clock = [0.0]
    sleeps: list[float] = []
    runner = ScheduledRunner(clock, durations)

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay
        if runner.calls >= len(durations):
            raise asyncio.CancelledError

    loop = EarlyDiscoveryLoop(
        cast(EarlyDiscoveryRunner, runner),
        interval_seconds=300.0,
        monotonic=lambda: clock[0],
        sleeper=cast(Callable[[float], Awaitable[None]], sleeper),
    )
    with contextlib.suppress(asyncio.CancelledError):
        asyncio.run(loop._run())
    return sleeps, runner.calls


def test_runner_uses_fixed_schedule_without_adding_scan_duration() -> None:
    sleeps, calls = run_schedule((120.0, 120.0, 120.0))

    assert calls == 3
    assert sleeps[:2] == [180.0, 180.0]


def test_missed_schedule_does_not_create_catch_up_burst() -> None:
    sleeps, calls = run_schedule((700.0, 10.0))

    assert calls == 2
    assert sleeps[0] == 200.0
