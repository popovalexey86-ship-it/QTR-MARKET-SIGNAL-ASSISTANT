from __future__ import annotations

import argparse
import asyncio
import os
import signal
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from market_signal_assistant.qtr_micro_scalper.analytics import (
    AnalyticsSnapshot,
    ShadowAnalyticsEngine,
)
from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    DEFAULT_DECISION_JOURNAL_PATH,
    ShadowDecisionJournal,
)
from market_signal_assistant.qtr_micro_scalper.metrics import (
    MetricsSnapshot,
    ShadowMetricsAggregator,
)
from market_signal_assistant.qtr_micro_scalper.observer import (
    ShadowObserver,
    ShadowObserverSummary,
)
from market_signal_assistant.qtr_micro_scalper.service import (
    ShadowServiceHealth,
    ShadowServiceMetrics,
    ShadowServiceStatus,
    build_shadow_service_from_environment,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    DEFAULT_SHADOW_JOURNAL_PATH,
    ShadowTradeJournal,
)


class ShadowCliService(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def health(self) -> ShadowServiceHealth: ...

    def metrics_snapshot(self) -> ShadowServiceMetrics: ...


class ShadowObservationSource(Protocol):
    def observe(self, *, generated_at: datetime) -> ShadowCliObservation: ...


@dataclass(frozen=True, slots=True)
class ShadowCliObservation:
    metrics: MetricsSnapshot
    analytics: AnalyticsSnapshot
    summary: ShadowObserverSummary


class JournalObservationSource:
    """Read fresh journal state on every refresh without network activity."""

    def __init__(
        self,
        *,
        decision_journal_path: Path = DEFAULT_DECISION_JOURNAL_PATH,
        trade_journal_path: Path = DEFAULT_SHADOW_JOURNAL_PATH,
        observer: ShadowObserver | None = None,
    ) -> None:
        self._decision_journal_path = decision_journal_path.resolve()
        self._trade_journal_path = trade_journal_path.resolve()
        self._observer = observer or ShadowObserver()

    def observe(self, *, generated_at: datetime) -> ShadowCliObservation:
        decisions = ShadowDecisionJournal(self._decision_journal_path)
        trades = ShadowTradeJournal(self._trade_journal_path)
        metrics = ShadowMetricsAggregator(trades).snapshot(
            generated_at=generated_at,
        )
        analytics = ShadowAnalyticsEngine(decisions, trades).snapshot(
            generated_at=generated_at,
        )
        return ShadowCliObservation(
            metrics=metrics,
            analytics=analytics,
            summary=self._observer.summarize(metrics, analytics),
        )


class ShadowCliRunner:
    """Run the existing shadow service and periodically print observations."""

    def __init__(
        self,
        service: ShadowCliService,
        observations: ShadowObservationSource,
        *,
        refresh_seconds: float = 30.0,
        output: Callable[[str], None] = print,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if refresh_seconds <= 0:
            raise ValueError("Интервал обновления должен быть больше нуля.")
        self._service = service
        self._observations = observations
        self._refresh_seconds = refresh_seconds
        self._output = output
        self._clock = clock or (lambda: datetime.now(UTC))

    async def run(
        self,
        *,
        stop_requested: asyncio.Event,
        once: bool = False,
    ) -> int:
        exit_code = 0
        try:
            await self._service.start()
            health = self._service.health()
            if health.status is ShadowServiceStatus.BLOCKED:
                self._output(_blocked_text(health))
                exit_code = 2
            else:
                while True:
                    self._print_refresh()
                    if once or health.status is ShadowServiceStatus.DISABLED:
                        break
                    try:
                        await asyncio.wait_for(
                            stop_requested.wait(),
                            timeout=self._refresh_seconds,
                        )
                    except TimeoutError:
                        health = self._service.health()
                        continue
                    break
        except (OSError, RuntimeError, ValueError) as exc:
            self._output(f"🔴 Наблюдение заблокировано: {exc}")
            exit_code = 2
        finally:
            try:
                await self._service.stop()
            except (OSError, RuntimeError, ValueError) as exc:
                self._output(f"⚠️ Ошибка при остановке теневого сервиса: {exc}")
                exit_code = 2
            self._output("🛑 Теневой наблюдатель остановлен.")
        return exit_code

    def _print_refresh(self) -> None:
        try:
            observation = self._observations.observe(generated_at=self._now())
            self._output(
                format_cli_report(
                    self._service.health(),
                    self._service.metrics_snapshot(),
                    observation,
                )
            )
        except (OSError, RuntimeError, ValueError) as exc:
            self._output(f"⚠️ Нет данных: не удалось обновить наблюдение ({exc}).")

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Часы CLI должны возвращать время с часовым поясом.")
        return value.astimezone(UTC)


def format_cli_report(
    health: ShadowServiceHealth,
    service_metrics: ShadowServiceMetrics,
    observation: ShadowCliObservation,
) -> str:
    analytics = observation.analytics.overall
    snapshots = service_metrics.pipeline.snapshots_ready
    lines = [
        "🧠 QTR MICRO SCALPER V2",
        "",
        "📡 Статус:",
        _status_text(health.status),
        "",
        "💎 Снимки рынка:",
        _count_or_no_data(snapshots),
        "",
        "🎯 Решения:",
        _count_or_no_data(analytics.total_decisions),
        "",
        "⚔️ Теневые сделки:",
        _count_or_no_data(analytics.shadow_entries),
        "",
        "📈 Средний результат:",
        (
            "Нет данных"
            if analytics.completed_trades == 0
            else f"{_signed_number(analytics.average_r)}R"
        ),
        "",
        "🔎 Подробное наблюдение:",
        observation.summary.text,
    ]
    if health.last_error:
        lines.extend(("", "⚠️ Последняя ошибка:", health.last_error))
    return "\n".join(lines)


def build_cli_runner_from_environment(
    *,
    refresh_seconds: float,
    output: Callable[[str], None] = print,
) -> ShadowCliRunner:
    """Build lazily; public market connections still start only in run()."""

    decision_path = Path(
        os.getenv(
            "QTR_SCALPER_V2_DECISION_JOURNAL_PATH",
            str(DEFAULT_DECISION_JOURNAL_PATH),
        )
    )
    trade_path = Path(
        os.getenv("QTR_SCALPER_V2_JOURNAL_PATH", str(DEFAULT_SHADOW_JOURNAL_PATH))
    )
    return ShadowCliRunner(
        build_shadow_service_from_environment(),
        JournalObservationSource(
            decision_journal_path=decision_path,
            trade_journal_path=trade_path,
        ),
        refresh_seconds=refresh_seconds,
        output=output,
    )


async def run_cli(
    runner: ShadowCliRunner,
    *,
    once: bool,
) -> int:
    stop_requested = asyncio.Event()
    _install_signal_handlers(stop_requested.set)
    return await runner.run(stop_requested=stop_requested, once=once)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        runner = build_cli_runner_from_environment(
            refresh_seconds=args.refresh_seconds,
        )
        return asyncio.run(run_cli(runner, once=args.once))
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"🔴 Не удалось запустить теневой наблюдатель: {exc}")
        return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Наблюдение за QTR Micro Scalper V2 в теневом режиме.",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=float,
        default=30.0,
        help="Интервал обновления в секундах (по умолчанию: 30).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Сформировать один отчёт и завершить работу.",
    )
    return parser


def _status_text(status: ShadowServiceStatus) -> str:
    return {
        ShadowServiceStatus.RUNNING: "🟢 Работает",
        ShadowServiceStatus.DEGRADED: "🟠 Работает с ошибками",
        ShadowServiceStatus.STARTING: "🟡 Запускается",
        ShadowServiceStatus.STOPPING: "🟡 Останавливается",
        ShadowServiceStatus.STOPPED: "⚪ Остановлен",
        ShadowServiceStatus.DISABLED: "⚪ Отключён настройками",
        ShadowServiceStatus.BLOCKED: "🔴 Заблокирован",
    }[status]


def _blocked_text(health: ShadowServiceHealth) -> str:
    reason = health.last_error or "Причина не указана."
    return (
        "🧠 QTR MICRO SCALPER V2\n\n"
        f"📡 Статус:\n🔴 Заблокирован\n\nПричина: {reason}"
    )


def _count_or_no_data(value: int) -> str:
    return "Нет данных" if value == 0 else str(value)


def _signed_number(value: float) -> str:
    return f"{value:+.2f}".replace(".", ",")


def _install_signal_handlers(callback: Callable[[], None]) -> None:
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_number, callback)
        except (NotImplementedError, RuntimeError):
            signal.signal(signal_number, lambda *_args: callback())


if __name__ == "__main__":
    raise SystemExit(main())
