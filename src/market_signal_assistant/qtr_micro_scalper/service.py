from __future__ import annotations

import argparse
import asyncio
import os
import signal
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    DEFAULT_DECISION_JOURNAL_PATH,
    ShadowDecisionJournal,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowOrchestrator
from market_signal_assistant.qtr_micro_scalper.pipeline import (
    LiveShadowPipeline,
    LiveShadowPipelineMetrics,
)
from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
    DEFAULT_VERIFIED_SETUP_PATH,
    JsonlVerifiedSetupProvider,
    VerifiedPriceContextAdapter,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    DEFAULT_SHADOW_JOURNAL_PATH,
    ShadowTradeJournal,
)
from market_signal_assistant.settings import QtrScalperV2LiveSettings


class ShadowServiceStatus(StrEnum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"
    DISABLED = "DISABLED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class ShadowServiceConfig:
    reconnect_delay_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.reconnect_delay_seconds <= 0:
            raise ValueError("Shadow service reconnect delay must be positive.")


@dataclass(frozen=True, slots=True)
class ShadowServiceHealth:
    ready: bool
    status: ShadowServiceStatus
    shadow_mode: bool
    started_at: datetime | None
    checked_at: datetime
    uptime_seconds: float
    last_error: str | None
    reconnect_attempts: int
    pipeline_errors: int


@dataclass(frozen=True, slots=True)
class ShadowServiceMetrics:
    status: ShadowServiceStatus
    start_requests: int
    pipeline_start_attempts: int
    reconnect_attempts: int
    pipeline_failures: int
    stop_requests: int
    journal_flushes: int
    pipeline: LiveShadowPipelineMetrics


class ShadowPipeline(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def metrics(self) -> LiveShadowPipelineMetrics: ...


class JournalFlusher(Protocol):
    def flush(self) -> None: ...


class ShadowService:
    """Supervise the Scalper V2 public-data pipeline in shadow mode only."""

    def __init__(
        self,
        pipeline: ShadowPipeline,
        journal: JournalFlusher,
        settings: QtrScalperV2LiveSettings,
        *,
        config: ShadowServiceConfig | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._pipeline = pipeline
        self._journal = journal
        self._settings = settings
        self._config = config or ShadowServiceConfig()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._status = ShadowServiceStatus.STOPPED
        self._started_at: datetime | None = None
        self._last_error: str | None = None
        self._supervisor: asyncio.Task[None] | None = None
        self._stop_requested: asyncio.Event | None = None
        self._first_attempt: asyncio.Event | None = None
        self._start_requests = 0
        self._pipeline_start_attempts = 0
        self._reconnect_attempts = 0
        self._pipeline_failures = 0
        self._stop_requests = 0
        self._journal_flushes = 0

    async def start(self) -> None:
        if self._supervisor is not None and not self._supervisor.done():
            return
        self._start_requests += 1
        if not self._settings.enabled:
            self._status = ShadowServiceStatus.DISABLED
            return
        if not self._settings.shadow_mode:
            self._status = ShadowServiceStatus.BLOCKED
            self._last_error = "Scalper V2 service is restricted to shadow mode."
            raise RuntimeError(self._last_error)

        self._status = ShadowServiceStatus.STARTING
        self._started_at = self._now()
        self._stop_requested = asyncio.Event()
        self._first_attempt = asyncio.Event()
        self._supervisor = asyncio.create_task(self._supervise())
        await self._first_attempt.wait()

    async def stop(self) -> None:
        if self._status in {
            ShadowServiceStatus.STOPPED,
            ShadowServiceStatus.DISABLED,
        }:
            return
        self._stop_requests += 1
        self._status = ShadowServiceStatus.STOPPING
        if self._stop_requested is not None:
            self._stop_requested.set()
        supervisor = self._supervisor
        if supervisor is not None:
            await asyncio.gather(supervisor, return_exceptions=True)
        self._supervisor = None
        self._status = ShadowServiceStatus.STOPPED

    def health(self) -> ShadowServiceHealth:
        now = self._now()
        uptime = 0.0
        if self._started_at is not None:
            uptime = max(0.0, (now - self._started_at).total_seconds())
        pipeline_metrics = self._pipeline.metrics()
        return ShadowServiceHealth(
            ready=self._status is ShadowServiceStatus.RUNNING,
            status=self._status,
            shadow_mode=self._settings.shadow_mode,
            started_at=self._started_at,
            checked_at=now,
            uptime_seconds=uptime,
            last_error=self._last_error,
            reconnect_attempts=self._reconnect_attempts,
            pipeline_errors=pipeline_metrics.errors,
        )

    def metrics_snapshot(self) -> ShadowServiceMetrics:
        return ShadowServiceMetrics(
            status=self._status,
            start_requests=self._start_requests,
            pipeline_start_attempts=self._pipeline_start_attempts,
            reconnect_attempts=self._reconnect_attempts,
            pipeline_failures=self._pipeline_failures,
            stop_requests=self._stop_requests,
            journal_flushes=self._journal_flushes,
            pipeline=self._pipeline.metrics(),
        )

    async def wait(self) -> None:
        supervisor = self._supervisor
        if supervisor is not None:
            await supervisor

    async def _supervise(self) -> None:
        stop_requested = self._stop_requested
        first_attempt = self._first_attempt
        if stop_requested is None or first_attempt is None:
            raise RuntimeError("Shadow service lifecycle was not initialized.")
        pipeline_running = False
        try:
            while not stop_requested.is_set():
                if self._pipeline_start_attempts:
                    self._reconnect_attempts += 1
                self._pipeline_start_attempts += 1
                try:
                    await self._pipeline.start()
                    pipeline_running = True
                    self._status = ShadowServiceStatus.RUNNING
                    self._last_error = None
                    first_attempt.set()
                    await stop_requested.wait()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._pipeline_failures += 1
                    self._status = ShadowServiceStatus.DEGRADED
                    self._last_error = f"{type(exc).__name__}: {exc}"
                    first_attempt.set()
                    await self._safe_pipeline_stop()
                    try:
                        await asyncio.wait_for(
                            stop_requested.wait(),
                            timeout=self._config.reconnect_delay_seconds,
                        )
                    except TimeoutError:
                        continue
                else:
                    break
        finally:
            if pipeline_running:
                await self._safe_pipeline_stop()
            self._flush_journal()

    async def _safe_pipeline_stop(self) -> None:
        try:
            await self._pipeline.stop()
        except Exception as exc:
            self._pipeline_failures += 1
            self._last_error = f"{type(exc).__name__}: {exc}"

    def _flush_journal(self) -> None:
        try:
            self._journal.flush()
            self._journal_flushes += 1
        except OSError as exc:
            self._pipeline_failures += 1
            self._last_error = f"{type(exc).__name__}: {exc}"

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Shadow service clock must return an aware timestamp.")
        return value.astimezone(UTC)


def build_shadow_service_from_environment() -> ShadowService:
    """Build the safe systemd runtime; construction itself remains offline."""

    settings = QtrScalperV2LiveSettings.from_environment()
    symbols = _environment_symbols()
    journal_path = Path(
        os.getenv("QTR_SCALPER_V2_JOURNAL_PATH", str(DEFAULT_SHADOW_JOURNAL_PATH))
    )
    decision_journal_path = Path(
        os.getenv(
            "QTR_SCALPER_V2_DECISION_JOURNAL_PATH",
            str(DEFAULT_DECISION_JOURNAL_PATH),
        )
    )
    journal = ShadowTradeJournal(journal_path)
    decision_journal = ShadowDecisionJournal(decision_journal_path)
    orchestrator = ShadowOrchestrator(
        journal=journal,
        decision_journal=decision_journal,
    )
    price_context = VerifiedPriceContextAdapter(
        JsonlVerifiedSetupProvider(_setup_audit_path())
    )
    pipeline = LiveShadowPipeline.with_live_collectors(
        symbols=symbols,
        price_context_provider=price_context,
        target_provider=price_context.target,
        orchestrator=orchestrator,
    )
    return ShadowService(pipeline, journal, settings)


async def serve(service: ShadowService) -> int:
    shutdown = asyncio.Event()
    _install_signal_handlers(shutdown.set)
    await service.start()
    health = service.health()
    if health.status is ShadowServiceStatus.DISABLED:
        return 0
    if health.status is ShadowServiceStatus.BLOCKED:
        return 2
    await shutdown.wait()
    await service.stop()
    return 0 if service.health().status is ShadowServiceStatus.STOPPED else 2


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="QTR Micro Scalper V2 shadow-only runtime service."
    )
    parser.parse_args(argv)
    try:
        return asyncio.run(serve(build_shadow_service_from_environment()))
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"QTR Scalper V2 Shadow Service blocked: {exc}")
        return 2


def _environment_symbols() -> tuple[str, ...]:
    values = tuple(
        dict.fromkeys(
            item.strip().upper()
            for item in os.getenv("QTR_SCALPER_V2_SYMBOLS", "BTCUSDT").split(",")
            if item.strip()
        )
    )
    if not values:
        raise ValueError("QTR_SCALPER_V2_SYMBOLS must contain at least one symbol.")
    return values


def _setup_audit_path() -> Path:
    configured = os.getenv("QTR_SCALPER_V2_SETUP_AUDIT_PATH", "").strip()
    if not configured:
        return DEFAULT_VERIFIED_SETUP_PATH.resolve()
    return Path(configured).expanduser().resolve()


def _install_signal_handlers(callback: Callable[[], None]) -> None:
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_number, callback)
        except (NotImplementedError, RuntimeError):
            signal.signal(signal_number, lambda *_args: callback())


if __name__ == "__main__":
    raise SystemExit(main())
