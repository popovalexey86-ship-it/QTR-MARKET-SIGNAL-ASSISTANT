from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.qtr_micro_scalper.pipeline import (
    LiveShadowPipelineMetrics,
)
from market_signal_assistant.qtr_micro_scalper.service import (
    ShadowService,
    ShadowServiceConfig,
    ShadowServiceStatus,
    build_shadow_service_from_environment,
    main,
)
from market_signal_assistant.settings import QtrScalperV2LiveSettings

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def pipeline_metrics(*, errors: int = 0) -> LiveShadowPipelineMetrics:
    return LiveShadowPipelineMetrics(
        market_events_received=10,
        duplicate_events_suppressed=1,
        snapshots_ready=2,
        scores_created=2,
        shadow_decisions=2,
        journal_updates=1,
        stale_data_suppressed=1,
        errors=errors,
        active_symbols=2,
    )


class FakePipeline:
    def __init__(
        self,
        *,
        start_failures: int = 0,
        stop_error: OSError | None = None,
        errors: int = 0,
    ) -> None:
        self.start_failures = start_failures
        self.stop_error = stop_error
        self.starts = 0
        self.stops = 0
        self._metrics = pipeline_metrics(errors=errors)

    async def start(self) -> None:
        self.starts += 1
        if self.starts <= self.start_failures:
            raise ConnectionError("mock stream unavailable")

    async def stop(self) -> None:
        self.stops += 1
        if self.stop_error is not None:
            raise self.stop_error

    def metrics(self) -> LiveShadowPipelineMetrics:
        return self._metrics


class FakeJournal:
    def __init__(self, *, error: OSError | None = None) -> None:
        self.flushes = 0
        self.error = error

    def flush(self) -> None:
        self.flushes += 1
        if self.error is not None:
            raise self.error


def service(
    pipeline: FakePipeline,
    journal: FakeJournal,
    *,
    enabled: bool = True,
    reconnect_delay: float = 0.001,
) -> ShadowService:
    return ShadowService(
        pipeline,
        journal,
        QtrScalperV2LiveSettings(enabled=enabled, shadow_mode=True),
        config=ShadowServiceConfig(reconnect_delay_seconds=reconnect_delay),
        clock=lambda: NOW,
    )


def test_start_health_and_graceful_stop_flush_journal() -> None:
    async def scenario() -> None:
        pipeline = FakePipeline()
        journal = FakeJournal()
        runtime = service(pipeline, journal)
        await runtime.start()
        assert runtime.health().ready
        assert runtime.health().status is ShadowServiceStatus.RUNNING
        await runtime.stop()
        assert runtime.health().status is ShadowServiceStatus.STOPPED
        assert pipeline.starts == 1
        assert pipeline.stops == 1
        assert journal.flushes == 1

    asyncio.run(scenario())


def test_start_and_stop_are_idempotent() -> None:
    async def scenario() -> None:
        pipeline = FakePipeline()
        journal = FakeJournal()
        runtime = service(pipeline, journal)
        await runtime.start()
        await runtime.start()
        await runtime.stop()
        await runtime.stop()
        assert pipeline.starts == 1
        assert pipeline.stops == 1
        assert journal.flushes == 1

    asyncio.run(scenario())


def test_start_failure_is_reconnected_without_process_exit() -> None:
    async def scenario() -> None:
        pipeline = FakePipeline(start_failures=1)
        journal = FakeJournal()
        runtime = service(pipeline, journal)
        await runtime.start()
        assert runtime.health().status is ShadowServiceStatus.DEGRADED
        for _ in range(50):
            if runtime.health().status is ShadowServiceStatus.RUNNING:
                break
            await asyncio.sleep(0.002)
        assert runtime.health().status is ShadowServiceStatus.RUNNING
        assert pipeline.starts == 2
        assert runtime.metrics_snapshot().reconnect_attempts == 1
        assert runtime.metrics_snapshot().pipeline_failures == 1
        await runtime.stop()
        assert journal.flushes == 1

    asyncio.run(scenario())


def test_pipeline_stop_failure_does_not_skip_journal_flush() -> None:
    async def scenario() -> None:
        pipeline = FakePipeline(stop_error=OSError("mock stop failure"))
        journal = FakeJournal()
        runtime = service(pipeline, journal)
        await runtime.start()
        await runtime.stop()
        assert journal.flushes == 1
        assert runtime.metrics_snapshot().pipeline_failures == 1
        assert "mock stop failure" in (runtime.health().last_error or "")

    asyncio.run(scenario())


def test_journal_flush_failure_is_visible_but_shutdown_completes() -> None:
    async def scenario() -> None:
        runtime = service(
            FakePipeline(),
            FakeJournal(error=OSError("mock fsync failure")),
        )
        await runtime.start()
        await runtime.stop()
        assert runtime.health().status is ShadowServiceStatus.STOPPED
        assert runtime.metrics_snapshot().journal_flushes == 0
        assert "mock fsync failure" in (runtime.health().last_error or "")

    asyncio.run(scenario())


def test_disabled_service_does_not_start_pipeline() -> None:
    async def scenario() -> None:
        pipeline = FakePipeline()
        runtime = service(pipeline, FakeJournal(), enabled=False)
        await runtime.start()
        assert runtime.health().status is ShadowServiceStatus.DISABLED
        assert not runtime.health().ready
        assert pipeline.starts == 0
        await runtime.stop()
        assert pipeline.stops == 0

    asyncio.run(scenario())


def test_health_and_metrics_are_immutable_and_utc_aware() -> None:
    async def scenario() -> None:
        runtime = service(FakePipeline(errors=3), FakeJournal())
        await runtime.start()
        health = runtime.health()
        metrics = runtime.metrics_snapshot()
        assert health.checked_at.tzinfo is UTC
        assert health.pipeline_errors == 3
        assert metrics.pipeline.market_events_received == 10
        with pytest.raises(FrozenInstanceError):
            health.ready = False  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            metrics.start_requests = 99  # type: ignore[misc]
        await runtime.stop()

    asyncio.run(scenario())


def test_health_uptime_uses_aware_clock() -> None:
    async def scenario() -> None:
        moments = iter((NOW, NOW + timedelta(seconds=30)))
        runtime = ShadowService(
            FakePipeline(),
            FakeJournal(),
            QtrScalperV2LiveSettings(enabled=True, shadow_mode=True),
            clock=lambda: next(moments),
        )
        await runtime.start()
        assert runtime.health().uptime_seconds == 30
        await runtime.stop()

    asyncio.run(scenario())


def test_environment_uses_required_enabled_and_shadow_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_SHADOW_MODE", "true")
    settings = QtrScalperV2LiveSettings.from_environment()
    assert settings.enabled
    assert settings.shadow_mode


def test_environment_defaults_are_disabled_and_shadow_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QTR_SCALPER_V2_ENABLED", raising=False)
    monkeypatch.delenv("QTR_SCALPER_V2_LIVE_ENABLED", raising=False)
    monkeypatch.delenv("QTR_SCALPER_V2_SHADOW_MODE", raising=False)
    settings = QtrScalperV2LiveSettings.from_environment()
    assert not settings.enabled
    assert settings.shadow_mode


def test_environment_composition_is_lazy_without_websocket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "false")
    monkeypatch.setenv("QTR_SCALPER_V2_SHADOW_MODE", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_JOURNAL_PATH", str(tmp_path / "shadow.jsonl"))
    monkeypatch.setenv(
        "QTR_SCALPER_V2_DECISION_JOURNAL_PATH",
        str(tmp_path / "decisions.jsonl"),
    )
    runtime = build_shadow_service_from_environment()
    assert runtime.health().status is ShadowServiceStatus.STOPPED


def test_systemd_entrypoint_is_declared() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    assert (
        "market-signal-scalper-shadow = "
        '"market_signal_assistant.qtr_micro_scalper.service:main"'
    ) in text


def test_help_does_not_build_pipeline_or_open_network() -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(("--help",))
    assert exit_info.value.code == 0
