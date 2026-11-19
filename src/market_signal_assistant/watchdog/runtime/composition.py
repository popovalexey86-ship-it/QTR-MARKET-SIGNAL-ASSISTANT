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
from market_signal_assistant.watchdog.runtime.control_snapshot import (
    ControlSnapshotPublisher,
)
from market_signal_assistant.watchdog.runtime.detection_only import (
    DetectionOnlyOutcomes,
    ensure_dataset_mode,
)
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
from market_signal_assistant.watchdog.runtime.retention import (
    InactiveRetention,
    InactiveSymbolArchive,
)
from market_signal_assistant.watchdog.runtime.retry import ApiRequestBudget
from market_signal_assistant.watchdog.runtime.schedule import (
    BucketCursorError,
    JsonBucketCursorStore,
)
from market_signal_assistant.watchdog.runtime.service import WatchdogShadowRuntime
from market_signal_assistant.watchdog.runtime.storage import (
    StorageMonitor,
    StorageTelemetryJournal,
)
from market_signal_assistant.watchdog.runtime.universe_evidence import (
    UniverseTransitionJournal,
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


def _has_prior_progress(root: Path) -> bool:
    if any(
        (root / relative).exists()
        for relative in (
            "state/health.json",
            "state/symbols.json",
            "state/baselines.json",
            "state/baselines.updates.jsonl",
            "state/pending.json",
            "state/soak.json",
            "state/inactive.sqlite3",
        )
    ):
        return True
    return any(
        path.exists() and path.stat().st_size > 0
        for path in (
            root / "events/events.jsonl",
            root / "outcomes/outcomes.jsonl",
            root / "outcomes/price_observations.jsonl",
        )
    )


def build_bybit_shadow_runtime(
    data_root: Path,
    *,
    config: ShadowRuntimeConfig | None = None,
    polling: PollingPolicy | None = None,
    universe_policy: UniversePolicy | None = None,
    clock: Callable[[], datetime] | None = None,
    detection_only: bool = False,
) -> ShadowRuntimeBundle:
    """Compose the public-data runtime without starting it or opening sockets."""
    ensure_dataset_mode(data_root, detection_only=detection_only)
    events = WatchdogEventJournal(data_root / "events" / "events.jsonl")
    events.assert_complete()
    outcome_scheduler: ForwardOutcomeScheduler | DetectionOnlyOutcomes
    sources: tuple[JournalIndexSource, ...]
    if detection_only:
        outcome_scheduler = DetectionOnlyOutcomes(events)
        sources = (JournalIndexSource("events", events.path, "event_id"),)
    else:
        outcome_journal = WatchdogOutcomeJournal(
            data_root / "outcomes" / "outcomes.jsonl"
        )
        price_journal = WatchdogPriceJournal(
            data_root / "outcomes" / "price_observations.jsonl"
        )
        for critical in (outcome_journal, price_journal):
            critical.assert_complete()
        outcome_scheduler = ForwardOutcomeScheduler(
            events,
            outcome_journal,
            JsonOutcomeCheckpointStore(data_root / "state" / "pending.json"),
            prices=price_journal,
        )
        sources = (
            JournalIndexSource("events", events.path, "event_id"),
            JournalIndexSource("prices", price_journal.path, "observation_id"),
            JournalIndexSource("outcomes", outcome_journal.path, "outcome_id"),
        )
    if not (data_root / "state" / "buckets.json").exists() and _has_prior_progress(
        data_root
    ):
        raise BucketCursorError(
            "Bucket cursor missing for existing runtime; recovery requires review."
        )
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
    cursors = JsonBucketCursorStore(data_root / "state" / "buckets.json")
    transitions = UniverseTransitionJournal(
        data_root / "operational" / "universe-transitions.jsonl"
    )
    retention = InactiveRetention(
        baselines,
        states,
        cursors,
        InactiveSymbolArchive(data_root / "state" / "inactive.sqlite3"),
        transitions,
    )
    indexes = WatchdogIndexManager(
        data_root / "state" / "evidence.sqlite3",
        sources,
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
        outcome_mode="DETECTION_ONLY" if detection_only else "FULL",
        cursors=cursors,
        audit=OperationalAuditJournal(data_root / "operational" / "runtime.jsonl"),
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
        universe_transitions=transitions,
        inactive_retention=retention,
        control_snapshot=(
            ControlSnapshotPublisher(data_root / "state" / "control-panel.json")
            if detection_only
            else None
        ),
    )
    holder["runtime"] = runtime
    return ShadowRuntimeBundle(runtime, budget)
