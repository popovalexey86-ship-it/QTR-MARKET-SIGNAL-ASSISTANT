"""Project already-computed observations; never open a source journal."""

from __future__ import annotations

import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from market_signal_assistant.qtr_micro_scalper.control_models import (
    MAX_BLOCKERS,
    MAX_TEXT,
    SCHEMA_VERSION,
    Blocker,
    DecisionsSection,
    Experiment,
    MarketSection,
    ResourcesSection,
    RuntimeDetails,
    RuntimeSection,
    ScalperControlSnapshot,
    TradesSection,
    VersionSection,
)
from market_signal_assistant.qtr_micro_scalper.control_store import ControlSnapshotStore

if TYPE_CHECKING:
    from market_signal_assistant.qtr_micro_scalper.cli import ShadowCliObservation
    from market_signal_assistant.qtr_micro_scalper.service import (
        ShadowServiceHealth,
        ShadowServiceMetrics,
    )


def resolve_code_sha() -> str:
    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(Path(__file__).resolve().parents[3]),
                "rev-parse",
                "HEAD",
            ],
            capture_output=True,
            text=True,
            timeout=1,
            check=False,
        )
        sha = result.stdout.strip()
        return (
            sha
            if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", sha)
            else "unknown"
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return "unknown"


class ScalperControlPlane:
    def __init__(
        self,
        store: ControlSnapshotStore | None = None,
        *,
        refresh_seconds: float = 30,
        code_sha: str | None = None,
    ) -> None:
        self.store = store or ControlSnapshotStore()
        self.stale_after_seconds = float(max(90, min(3600, refresh_seconds * 3)))
        self.code_sha = resolve_code_sha() if code_sha is None else code_sha
        self.last_publish_error: str | None = None

    def build(
        self,
        health: ShadowServiceHealth,
        service: ShadowServiceMetrics,
        observation: ShadowCliObservation,
        details: RuntimeDetails,
        *,
        generated_at: datetime,
    ) -> ScalperControlSnapshot:
        analytics = observation.analytics
        overall = analytics.overall
        rates = observation.metrics.overall
        pipeline = service.pipeline
        market = max(
            analytics.by_market_state,
            key=lambda x: x.metrics.total_decisions,
            default=None,
        )
        score = max(
            analytics.by_score_bucket,
            key=lambda x: x.metrics.total_decisions,
            default=None,
        )
        return ScalperControlSnapshot(
            schema_version=SCHEMA_VERSION,
            generated_at=generated_at,
            observed_at=observation.metrics.generated_at,
            stale_after_seconds=self.stale_after_seconds,
            runtime=RuntimeSection(
                str(health.status),
                health.ready,
                health.shadow_mode,
                details.live_enabled,
                health.started_at,
                health.uptime_seconds,
                health.reconnect_attempts,
                health.pipeline_errors,
                "Ошибка runtime; подробности в служебном журнале."
                if health.last_error
                else None,
                service.quarantined_legacy_trades,
                "Исторические незавершённые сделки изолированы "
                "и не восстановлены в runtime."
                if health.legacy_quarantine_warning
                else None,
            ),
            market=MarketSection(
                pipeline.snapshots_ready,
                pipeline.active_symbols,
                details.symbols,
                pipeline.last_target_refresh_at,
            ),
            decisions=DecisionsSection(
                overall.total_decisions,
                overall.blocked_decisions,
                overall.shadow_entries,
                market.key if market and market.metrics.total_decisions else None,
                score.key if score and score.metrics.total_decisions else None,
            ),
            trades=TradesSection(
                details.active_count,
                overall.completed_trades,
                overall.win_rate,
                overall.average_r,
                overall.total_r,
                rates.expired_rate,
                rates.tp1_hit_rate,
                rates.tp2_hit_rate,
                rates.stop_rate,
                details.active,
            ),
            blockers=tuple(
                Blocker(x.reason[:MAX_TEXT], x.count)
                for x in analytics.top_blocked_reasons[:MAX_BLOCKERS]
            ),
            experiments=(
                Experiment(
                    "Holding Experiment",
                    details.holding_enabled,
                    "Предупреждение; см. служебный журнал."
                    if pipeline.holding_experiment_warning
                    else None,
                    pipeline.holding_experiment_errors,
                ),
                Experiment(
                    "Micro Profit",
                    details.micro_profit_enabled,
                    "Предупреждение; см. служебный журнал."
                    if pipeline.micro_profit_warning
                    else None,
                    pipeline.micro_profit_errors,
                ),
                Experiment(
                    "Protected Runner",
                    details.protected_runner_enabled,
                    "Предупреждение; см. служебный журнал."
                    if pipeline.protected_runner_warning
                    else None,
                    pipeline.protected_runner_errors,
                ),
            ),
            resources=ResourcesSection(
                pipeline.queue_depth,
                pipeline.retained_events,
                pipeline.stale_data_suppressed,
                self.last_publish_error,
            ),
            version=VersionSection(SCHEMA_VERSION, self.code_sha),
        )

    def publish(self, snapshot: ScalperControlSnapshot) -> bool:
        try:
            self.store.publish(snapshot)
            self.last_publish_error = None
            return True
        except (OSError, ValueError, TypeError, RecursionError):
            # Publish failures degrade only the operator channel. Do not touch
            # runtime status, journals, service lifecycle or exception details.
            self.last_publish_error = "Публикация Control Plane недоступна."
            return False
