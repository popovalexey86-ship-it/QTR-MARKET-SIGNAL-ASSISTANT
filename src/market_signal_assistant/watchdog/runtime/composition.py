from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.providers import BybitPublicProvider
from market_signal_assistant.providers.bybit_derivatives import (
    BybitDerivativesProvider,
)
from market_signal_assistant.providers.bybit_liquidations import (
    BybitLiquidationAccumulator,
)
from market_signal_assistant.watchdog.aggregation import ExplainableAnomalyAggregator
from market_signal_assistant.watchdog.baselines import (
    JsonBaselineStore,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.detectors import (
    DetectorPipeline,
    default_detectors,
)
from market_signal_assistant.watchdog.engine import WatchdogDetectionEngine
from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.features import WatchdogFeatureBuilder
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.price_journal import (
    WatchdogPriceJournal,
)
from market_signal_assistant.watchdog.outcomes.scheduler import (
    ForwardOutcomeScheduler,
    JsonOutcomeCheckpointStore,
)
from market_signal_assistant.watchdog.runtime.audit import OperationalAuditJournal
from market_signal_assistant.watchdog.runtime.gaps import GapLedger
from market_signal_assistant.watchdog.runtime.health import JsonRuntimeHealthStore
from market_signal_assistant.watchdog.runtime.index import (
    JournalIndexSource,
    WatchdogIndexManager,
)
from market_signal_assistant.watchdog.runtime.lock import SingleInstanceLock
from market_signal_assistant.watchdog.runtime.models import (
    PollingPolicy,
    ShadowRuntimeConfig,
)
from market_signal_assistant.watchdog.runtime.retry import ApiRequestBudget
from market_signal_assistant.watchdog.runtime.schedule import JsonBucketCursorStore
from market_signal_assistant.watchdog.runtime.service import WatchdogShadowRuntime
from market_signal_assistant.watchdog.runtime.storage import (
    StorageMonitor,
    StorageTelemetryJournal,
)
from market_signal_assistant.watchdog.state_machine import WatchdogStateMachine
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
    WatchdogStateRepository,
)
from market_signal_assistant.watchdog.universe import DynamicUniverse, UniversePolicy


@dataclass(frozen=True, slots=True)
class ShadowRuntimeBundle:
    runtime: WatchdogShadowRuntime
    request_budget: ApiRequestBudget


def build_bybit_shadow_runtime(
    data_root: Path,
    *,
    config: ShadowRuntimeConfig | None = None,
    polling: PollingPolicy | None = None,
    universe_policy: UniversePolicy | None = None,
    clock: Callable[[], datetime] | None = None,
) -> ShadowRuntimeBundle:
    """Compose the public-data runtime without starting it or opening sockets."""
    settings = config or ShadowRuntimeConfig()
    runtime_clock = clock or (lambda: datetime.now(UTC))
    holder: dict[str, WatchdogShadowRuntime] = {}
    budget = ApiRequestBudget(
        settings.api_calls_per_minute,
        on_wait=lambda delay: holder["runtime"].record_throttle_wait(delay),
    )
    def observer() -> None:
        holder["runtime"].record_api_call()
    market = BybitPublicProvider(
        timeout=settings.provider_timeout,
        request_gate=budget.acquire,
        request_observer=observer,
    )
    # The accumulator is intentionally never connected to a liquidation stream.
    # It only satisfies the reused derivatives provider contract; liquidation data
    # is not exposed to the Watchdog feature builder.
    derivatives = BybitDerivativesProvider(
        BybitLiquidationAccumulator(clock=runtime_clock),
        timeout=settings.provider_timeout,
        clock=runtime_clock,
        request_gate=budget.acquire,
        request_observer=observer,
    )
    baselines = RollingBaselineEngine(
        JsonBaselineStore(data_root / "state" / "baselines.json")
    )
    states = WatchdogStateRepository(
        JsonWatchdogStateStore(data_root / "state" / "symbols.json")
    )
    engine = WatchdogDetectionEngine(
        WatchdogFeatureBuilder(baselines),
        DetectorPipeline(default_detectors()),
        ExplainableAnomalyAggregator(),
        WatchdogStateMachine(),
        states,
    )
    events = WatchdogEventJournal(data_root / "events" / "events.jsonl")
    outcome_journal = WatchdogOutcomeJournal(
        data_root / "outcomes" / "outcomes.jsonl"
    )
    price_journal = WatchdogPriceJournal(
        data_root / "outcomes" / "price_observations.jsonl"
    )
    outcome_scheduler = ForwardOutcomeScheduler(
        events,
        outcome_journal,
        JsonOutcomeCheckpointStore(data_root / "state" / "pending.json"),
        prices=price_journal,
    )
    indexes = WatchdogIndexManager(
        data_root / "state" / "evidence.sqlite3",
        (
            JournalIndexSource("events", events.path, "event_id"),
            JournalIndexSource("prices", price_journal.path, "observation_id"),
            JournalIndexSource("outcomes", outcome_journal.path, "outcome_id"),
        ),
    )
    runtime = WatchdogShadowRuntime(
        universe_provider=market,
        market_provider=market,
        derivatives_provider=derivatives,
        universe=DynamicUniverse(universe_policy),
        baseline_counts=lambda now: baselines.sample_counts(
            "normalized_range_5", detected_at=now
        ),
        engine=engine,
        states=states,
        events=events,
        outcomes=outcome_scheduler,
        cursors=JsonBucketCursorStore(data_root / "state" / "buckets.json"),
        audit=OperationalAuditJournal(
            data_root / "operational" / "runtime.jsonl"
        ),
        health_store=JsonRuntimeHealthStore(data_root / "state" / "health.json"),
        config=settings,
        polling=polling,
        clock=runtime_clock,
        instance_lock=SingleInstanceLock(data_root / "state" / "writer.lock"),
        gaps=GapLedger(data_root / "operational" / "gaps.jsonl"),
        indexes=indexes,
        storage_monitor=StorageMonitor(data_root),
        storage_telemetry=StorageTelemetryJournal(
            data_root / "operational" / "storage.jsonl"
        ),
        baseline_retained_counts=lambda: baselines.retained_counts,
    )
    holder["runtime"] = runtime
    return ShadowRuntimeBundle(runtime, budget)
