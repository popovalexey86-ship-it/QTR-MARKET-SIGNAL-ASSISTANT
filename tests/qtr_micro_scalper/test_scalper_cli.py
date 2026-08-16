from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_analytics import decision
from test_metrics import record as trade_record
from test_service import pipeline_metrics

from market_signal_assistant.qtr_micro_scalper.analytics import (
    analyze_shadow_journals,
)
from market_signal_assistant.qtr_micro_scalper.cli import (
    JournalObservationSource,
    ShadowCliObservation,
    ShadowCliRunner,
    format_cli_report,
    main,
)
from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    ShadowDecisionEventType,
    ShadowDecisionJournal,
    ShadowDecisionRecord,
)
from market_signal_assistant.qtr_micro_scalper.metrics import (
    aggregate_shadow_metrics,
)
from market_signal_assistant.qtr_micro_scalper.observer import ShadowObserver
from market_signal_assistant.qtr_micro_scalper.service import (
    ShadowServiceHealth,
    ShadowServiceMetrics,
    ShadowServiceStatus,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    ShadowTradeRecord,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def health(
    status: ShadowServiceStatus = ShadowServiceStatus.RUNNING,
    *,
    error: str | None = None,
) -> ShadowServiceHealth:
    return ShadowServiceHealth(
        ready=status is ShadowServiceStatus.RUNNING,
        status=status,
        shadow_mode=True,
        started_at=NOW,
        checked_at=NOW,
        uptime_seconds=10.0,
        last_error=error,
        reconnect_attempts=0,
        pipeline_errors=0,
    )


def service_metrics(*, snapshots: int = 1) -> ShadowServiceMetrics:
    pipeline = pipeline_metrics()
    pipeline = type(pipeline)(
        market_events_received=pipeline.market_events_received,
        duplicate_events_suppressed=pipeline.duplicate_events_suppressed,
        snapshots_ready=snapshots,
        scores_created=pipeline.scores_created,
        shadow_decisions=pipeline.shadow_decisions,
        journal_updates=pipeline.journal_updates,
        stale_data_suppressed=pipeline.stale_data_suppressed,
        errors=pipeline.errors,
        active_symbols=pipeline.active_symbols,
    )
    return ShadowServiceMetrics(
        status=ShadowServiceStatus.RUNNING,
        start_requests=1,
        pipeline_start_attempts=1,
        reconnect_attempts=0,
        pipeline_failures=0,
        stop_requests=0,
        journal_flushes=0,
        pipeline=pipeline,
    )


def observation(*, with_data: bool = True) -> ShadowCliObservation:
    decisions: tuple[ShadowDecisionRecord, ...]
    trades: tuple[ShadowTradeRecord, ...]
    if with_data:
        trade = trade_record("one")
        decisions = (decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED),)
        trades = (trade,)
    else:
        decisions = ()
        trades = ()
    metrics = aggregate_shadow_metrics(trades, generated_at=NOW)
    analytics = analyze_shadow_journals(decisions, trades, generated_at=NOW)
    return ShadowCliObservation(
        metrics=metrics,
        analytics=analytics,
        summary=ShadowObserver().summarize(metrics, analytics),
    )


class FakeService:
    def __init__(
        self,
        *,
        status: ShadowServiceStatus = ShadowServiceStatus.STOPPED,
        start_error: RuntimeError | None = None,
        snapshots: int = 1,
    ) -> None:
        self.status = status
        self.start_error = start_error
        self.starts = 0
        self.stops = 0
        self.snapshots = snapshots

    async def start(self) -> None:
        self.starts += 1
        if self.start_error is not None:
            raise self.start_error
        if self.status is ShadowServiceStatus.STOPPED:
            self.status = ShadowServiceStatus.RUNNING

    async def stop(self) -> None:
        self.stops += 1
        if self.status is not ShadowServiceStatus.DISABLED:
            self.status = ShadowServiceStatus.STOPPED

    def health(self) -> ShadowServiceHealth:
        return health(self.status)

    def metrics_snapshot(self) -> ShadowServiceMetrics:
        return service_metrics(snapshots=self.snapshots)


class FakeSource:
    def __init__(
        self,
        value: ShadowCliObservation,
        *,
        stop_requested: asyncio.Event | None = None,
        stop_after: int | None = None,
        error: OSError | None = None,
    ) -> None:
        self.value = value
        self.stop_requested = stop_requested
        self.stop_after = stop_after
        self.error = error
        self.calls = 0

    def observe(self, *, generated_at: datetime) -> ShadowCliObservation:
        assert generated_at.tzinfo is UTC
        self.calls += 1
        if self.error is not None:
            raise self.error
        if self.stop_after == self.calls and self.stop_requested is not None:
            self.stop_requested.set()
        return self.value


def test_once_starts_service_prints_observer_and_stops_gracefully() -> None:
    async def scenario() -> None:
        runtime = FakeService()
        source = FakeSource(observation())
        output: list[str] = []
        runner = ShadowCliRunner(
            runtime,
            source,
            output=output.append,
            clock=lambda: NOW,
        )
        code = await runner.run(stop_requested=asyncio.Event(), once=True)
        assert code == 0
        assert runtime.starts == 1
        assert runtime.stops == 1
        assert source.calls == 1
        assert "📡 Статус:\n🟢 Работает" in output[0]
        assert "💎 Снимки рынка:\n1" in output[0]
        assert "🎯 Решения:\n1" in output[0]
        assert "⚔️ Теневые сделки:\n1" in output[0]
        assert "📈 Средний результат:\n+2,00R" in output[0]
        assert "🔎 Подробное наблюдение:" in output[0]
        assert output[-1] == "🛑 Теневой наблюдатель остановлен."

    asyncio.run(scenario())


def test_no_data_is_explicit() -> None:
    report = format_cli_report(
        health(),
        service_metrics(snapshots=0),
        observation(with_data=False),
    )
    assert "💎 Снимки рынка:\nНет данных" in report
    assert "🎯 Решения:\nНет данных" in report
    assert "⚔️ Теневые сделки:\nНет данных" in report
    assert "📈 Средний результат:\nНет данных" in report


def test_periodic_refresh_stops_via_shared_event() -> None:
    async def scenario() -> None:
        stop_requested = asyncio.Event()
        source = FakeSource(
            observation(),
            stop_requested=stop_requested,
            stop_after=2,
        )
        runtime = FakeService()
        runner = ShadowCliRunner(
            runtime,
            source,
            refresh_seconds=0.001,
            output=lambda _message: None,
            clock=lambda: NOW,
        )
        code = await runner.run(stop_requested=stop_requested)
        assert code == 0
        assert source.calls == 2
        assert runtime.stops == 1

    asyncio.run(scenario())


def test_blocked_service_returns_nonzero_without_observation() -> None:
    async def scenario() -> None:
        runtime = FakeService(status=ShadowServiceStatus.BLOCKED)
        source = FakeSource(observation())
        output: list[str] = []
        runner = ShadowCliRunner(runtime, source, output=output.append)
        code = await runner.run(stop_requested=asyncio.Event(), once=True)
        assert code == 2
        assert source.calls == 0
        assert runtime.stops == 1
        assert "🔴 Заблокирован" in output[0]

    asyncio.run(scenario())


def test_observation_error_does_not_break_shutdown() -> None:
    async def scenario() -> None:
        runtime = FakeService()
        source = FakeSource(observation(), error=OSError("mock journal error"))
        output: list[str] = []
        runner = ShadowCliRunner(runtime, source, output=output.append)
        code = await runner.run(stop_requested=asyncio.Event(), once=True)
        assert code == 0
        assert runtime.stops == 1
        assert output[0].startswith("⚠️ Нет данных")

    asyncio.run(scenario())


def test_start_error_is_reported_in_russian_and_returns_nonzero() -> None:
    async def scenario() -> None:
        runtime = FakeService(start_error=RuntimeError("mock start failure"))
        output: list[str] = []
        runner = ShadowCliRunner(
            runtime,
            FakeSource(observation()),
            output=output.append,
        )
        code = await runner.run(stop_requested=asyncio.Event(), once=True)
        assert code == 2
        assert runtime.stops == 1
        assert output[0].startswith("🔴 Наблюдение заблокировано")

    asyncio.run(scenario())


def test_journal_source_reads_fresh_durable_records(tmp_path: Path) -> None:
    decision_path = tmp_path / "decisions.jsonl"
    trade_path = tmp_path / "trades.jsonl"
    source = JournalObservationSource(
        decision_journal_path=decision_path,
        trade_journal_path=trade_path,
    )
    empty = source.observe(generated_at=NOW)
    assert empty.analytics.overall.total_decisions == 0
    assert ShadowDecisionJournal(decision_path).append(
        decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED)
    )
    assert ShadowTradeJournal(trade_path).append(trade_record("one"))
    refreshed = source.observe(generated_at=NOW)
    assert refreshed.analytics.overall.total_decisions == 1
    assert refreshed.analytics.overall.completed_trades == 1


def test_invalid_refresh_interval_is_rejected() -> None:
    with pytest.raises(ValueError, match="больше нуля"):
        ShadowCliRunner(FakeService(), FakeSource(observation()), refresh_seconds=0)


def test_help_does_not_build_service_or_open_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def forbidden_builder(*, refresh_seconds: float) -> ShadowCliRunner:
        del refresh_seconds
        nonlocal called
        called = True
        raise AssertionError("CLI composition must not be built for --help")

    monkeypatch.setattr(
        "market_signal_assistant.qtr_micro_scalper.cli.build_cli_runner_from_environment",
        forbidden_builder,
    )
    with pytest.raises(SystemExit) as exit_info:
        main(("--help",))
    assert exit_info.value.code == 0
    assert called is False
