from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pytest

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.models import AssetClass, Candle, Instrument, MarketSeries
from market_signal_assistant.providers import MarketDataError
from market_signal_assistant.watchdog.aggregation import ExplainableAnomalyAggregator
from market_signal_assistant.watchdog.baselines import (
    BaselineObservation,
    JsonBaselineStore,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.detectors import (
    DetectorPipeline,
    VolumeShockDetector,
)
from market_signal_assistant.watchdog.engine import WatchdogDetectionEngine
from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.features import WatchdogFeatureBuilder
from market_signal_assistant.watchdog.journal import JournalConflictError
from market_signal_assistant.watchdog.models import WatchdogState
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.price_journal import WatchdogPriceJournal
from market_signal_assistant.watchdog.outcomes.scheduler import (
    ForwardOutcomeScheduler,
    JsonOutcomeCheckpointStore,
)
from market_signal_assistant.watchdog.runtime.audit import OperationalAuditJournal
from market_signal_assistant.watchdog.runtime.gaps import GapLedger
from market_signal_assistant.watchdog.runtime.health import JsonRuntimeHealthStore
from market_signal_assistant.watchdog.runtime.models import ShadowRuntimeConfig
from market_signal_assistant.watchdog.runtime.retention import (
    InactiveRetention,
    InactiveSymbolArchive,
)
from market_signal_assistant.watchdog.runtime.schedule import JsonBucketCursorStore
from market_signal_assistant.watchdog.runtime.service import WatchdogShadowRuntime
from market_signal_assistant.watchdog.runtime.universe_evidence import (
    UniverseTransitionJournal,
)
from market_signal_assistant.watchdog.state_machine import (
    WatchdogStateMachine,
    WatchdogSymbolState,
)
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
    WatchdogRuntimeState,
    WatchdogStateRepository,
)
from market_signal_assistant.watchdog.universe import DynamicUniverse

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


class MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class FixtureProvider:
    def __init__(self, clock: MutableClock, symbols: tuple[str, ...]) -> None:
        self.clock = clock
        self.symbols = symbols
        self.catalog_calls = 0
        self.market_calls: list[tuple[str, str]] = []
        self.failures: dict[str, int] = {}

    def list_instruments(self) -> tuple[CatalogInstrument, ...]:
        self.catalog_calls += 1
        return tuple(_catalog(symbol) for symbol in self.symbols)

    def load(self, instrument: Instrument, interval: str, limit: int) -> MarketSeries:
        del limit
        self.market_calls.append((instrument.symbol, interval))
        remaining = self.failures.get(instrument.symbol, 0)
        if remaining:
            self.failures[instrument.symbol] = remaining - 1
            raise TimeoutError("fixture timeout")
        return _series(instrument.symbol, interval, self.clock.now)


class FailingDerivativesProvider:
    def collect(self, symbol: str) -> DerivativesSnapshot:
        del symbol
        raise TimeoutError("derivatives unavailable")


class BlockingProvider(FixtureProvider):
    def __init__(self, clock: MutableClock, symbols: tuple[str, ...]) -> None:
        super().__init__(clock, symbols)
        self.release = Event()

    def load(self, instrument: Instrument, interval: str, limit: int) -> MarketSeries:
        self.market_calls.append((instrument.symbol, interval))
        self.release.wait(timeout=2.0)
        return _series(instrument.symbol, interval, self.clock.now)


class SlowProvider(FixtureProvider):
    def __init__(
        self,
        clock: MutableClock,
        symbols: tuple[str, ...],
        delay: float,
    ) -> None:
        super().__init__(clock, symbols)
        self.delay = delay

    def load(self, instrument: Instrument, interval: str, limit: int) -> MarketSeries:
        del limit
        self.market_calls.append((instrument.symbol, interval))
        time.sleep(self.delay)
        return _series(instrument.symbol, interval, self.clock.now)


class RateLimitedProvider(FixtureProvider):
    def load(self, instrument: Instrument, interval: str, limit: int) -> MarketSeries:
        del limit
        self.market_calls.append((instrument.symbol, interval))
        raise MarketDataError("Bybit public API error code 10006.")


def test_runtime_persists_event_outcomes_and_restart_state(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    runtime = _runtime(tmp_path, provider, clock)

    first = runtime.run_once(now=NOW)

    assert first.symbols_in_universe == 1
    assert first.symbols_processed == 1
    assert first.events_today == 1
    assert first.pending_outcomes == 5
    assert provider.market_calls == [("ABCUSDT", "5m")]

    restarted = _runtime(tmp_path, provider, clock)
    same_boundary = restarted.run_once(now=NOW)

    assert same_boundary.events_today == 1
    assert same_boundary.pending_outcomes == 5
    persisted_events = WatchdogEventJournal(
        tmp_path / "events" / "events.jsonl"
    ).records()
    assert len(persisted_events) == 1

    clock.now = NOW + timedelta(minutes=5)
    after_horizon = restarted.run_once(now=clock.now)
    outcomes = WatchdogOutcomeJournal(
        tmp_path / "outcomes" / "outcomes.jsonl"
    ).records()

    assert after_horizon.pending_outcomes == 4
    assert {item.horizon_minutes for item in outcomes} == {1}
    assert any(item.lateness_seconds == 240.0 for item in outcomes)
    assert JsonRuntimeHealthStore(tmp_path / "state" / "health.json").load()
    audit = OperationalAuditJournal(
        tmp_path / "operational" / "runtime.jsonl"
    ).records()
    assert {item["event_type"] for item in audit} >= {
        "RUNTIME_START",
        "RECOVERY",
        "UNIVERSE_REFRESH",
    }


def test_symbol_failure_is_isolated_without_global_degradation(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT", "BADUSDT"))
    provider.failures["BADUSDT"] = 3
    sleeps: list[float] = []
    runtime = _runtime(tmp_path, provider, clock, sleep=sleeps.append)

    health = runtime.run_once(now=NOW)

    assert health.symbols_processed == 1
    assert health.symbols_failed == 1
    assert health.provider_errors == 3
    assert health.provider_failures == 3
    assert health.degraded is False
    assert health.degraded_reasons == ()
    assert health.acceptance_blocked is False
    failures = [
        item
        for item in OperationalAuditJournal(
            tmp_path / "operational" / "runtime.jsonl"
        ).records()
        if item["event_type"] == "SYMBOL_FAILURE"
    ]
    assert len(failures) == 1
    assert failures[0]["symbol"] == "BADUSDT"
    details = failures[0]["details"]
    assert isinstance(details, dict)
    assert details["error_type"] == "TimeoutError"
    assert "traceback" in details
    assert sleeps == [0.5, 1.0]
    rollups = OperationalAuditJournal(
        tmp_path / "operational" / "runtime.jsonl"
    ).rollups()
    provider_rollup = next(
        item
        for item in rollups
        if str(item["signature"]).startswith("provider:failure")
    )
    assert provider_rollup["count"] == 3
    assert provider_rollup["affected_symbols"] == ["BADUSDT"]
    latest = provider_rollup["latest_details"]
    assert isinstance(latest, dict)
    assert latest == {
        "endpoint_class": "market",
        "attempt": "3",
        "error": "TimeoutError",
        "http_status": "unknown",
        "ret_code": "unknown",
        "backoff_seconds": "0.000000",
    }
    persisted_events = WatchdogEventJournal(
        tmp_path / "events" / "events.jsonl"
    ).records()
    assert len(persisted_events) == 1


def test_dynamic_universe_refresh_adds_and_removes_without_restart(
    tmp_path: Path,
) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    runtime = _runtime(tmp_path, provider, clock)
    runtime.run_once(now=NOW)

    provider.symbols = ("NEWUSDT",)
    clock.now = NOW + timedelta(minutes=16)
    health = runtime.run_once(now=clock.now)

    assert health.symbols_in_universe == 1
    assert runtime.universe_snapshot is not None
    assert tuple(
        item.instrument.symbol for item in runtime.universe_snapshot.eligible
    ) == ("NEWUSDT",)
    refreshes = [
        item
        for item in OperationalAuditJournal(
            tmp_path / "operational" / "runtime.jsonl"
        ).records()
        if item["event_type"] == "UNIVERSE_REFRESH"
    ]
    assert refreshes[-1]["details"] == {
        "added": "1",
        "eligible": "1",
        "rejected": "0",
        "removed": "1",
    }


def test_construction_has_no_provider_or_network_side_effects(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))

    runtime = _runtime(tmp_path, provider, clock)

    assert provider.catalog_calls == 0
    assert provider.market_calls == []
    assert runtime.running is False


def test_runtime_start_and_stop_are_explicit_and_clean(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    runtime = _runtime(tmp_path, provider, clock)

    runtime.start()
    deadline = time.monotonic() + 2.0
    while provider.catalog_calls == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    runtime.stop(timeout=2.0)

    assert runtime.running is False
    audit_types = {
        item["event_type"]
        for item in OperationalAuditJournal(
            tmp_path / "operational" / "runtime.jsonl"
        ).records()
    }
    assert {"RUNTIME_START", "RUNTIME_STOP"} <= audit_types


def test_per_loop_batch_is_bounded_without_capping_universe(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    symbols = tuple(f"COIN{index}USDT" for index in range(6))
    provider = FixtureProvider(clock, symbols)
    config = ShadowRuntimeConfig(maximum_symbols_per_loop=2, maximum_workers=2)
    runtime = _runtime(tmp_path, provider, clock, config=config)

    health = runtime.run_once(now=NOW)

    assert health.symbols_in_universe == 6
    assert health.symbols_processed == 2
    assert len(provider.market_calls) == 2


def test_derivatives_failure_degrades_but_market_pipeline_continues(
    tmp_path: Path,
) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    sleeps: list[float] = []
    runtime = _runtime(
        tmp_path,
        provider,
        clock,
        sleep=sleeps.append,
        derivatives_provider=FailingDerivativesProvider(),
    )

    health = runtime.run_once(now=NOW)

    assert health.symbols_processed == 1
    assert health.symbols_failed == 0
    assert health.provider_errors == 3
    assert health.degraded is True
    assert health.acceptance_blocked is False
    assert health.events_today == 1
    assert any(
        reason.startswith("derivatives:ABCUSDT") for reason in health.degraded_reasons
    )


def test_total_market_provider_outage_blocks_acceptance_without_crashing_runtime(
    tmp_path: Path,
) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("BADUSDT",))
    provider.failures["BADUSDT"] = 3
    runtime = _runtime(tmp_path, provider, clock, sleep=lambda _delay: None)

    health = runtime.run_once(now=NOW)

    assert health.symbols_failed == 1
    assert health.symbols_processed == 0
    assert health.acceptance_blocked is True
    assert health.acceptance_blocking_reasons == ("market:no-symbol-progress",)


def test_provider_rollup_preserves_safe_rate_limit_evidence(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    provider = RateLimitedProvider(clock, ("ABCUSDT",))
    runtime = _runtime(tmp_path, provider, clock, sleep=lambda _delay: None)

    health = runtime.run_once(now=NOW)

    assert health.provider_errors == 3
    assert health.provider_failures == 0
    assert health.rate_limit_events == 3
    rate_rollup = next(
        item
        for item in OperationalAuditJournal(
            tmp_path / "operational" / "runtime.jsonl"
        ).rollups()
        if str(item["signature"]).startswith("provider:rate_limit")
    )
    assert rate_rollup["count"] == 3
    details = rate_rollup["latest_details"]
    assert isinstance(details, dict)
    assert details["endpoint_class"] == "market"
    assert details["ret_code"] == "10006"
    assert details["http_status"] == "unknown"
    assert details["attempt"] == "3"
    assert details["backoff_seconds"] == "0.000000"


def test_failed_latest_bucket_acknowledges_gap_without_reemitting_it(
    tmp_path: Path,
) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    cursor_path = tmp_path / "state" / "buckets.json"
    JsonBucketCursorStore(cursor_path).save(
        "ABCUSDT", "5m", NOW - timedelta(minutes=30)
    )
    provider.failures["ABCUSDT"] = 3
    config = ShadowRuntimeConfig(
        maximum_catchup_buckets=3,
        maximum_workers=1,
        retry_jitter=0.0,
    )
    first = _runtime(tmp_path, provider, clock, config=config)

    failed = first.run_once(now=NOW)

    assert failed.symbols_failed == 1
    assert JsonBucketCursorStore(cursor_path).get("ABCUSDT", "5m") == (
        NOW - timedelta(minutes=5)
    )
    gaps = GapLedger(tmp_path / "operational" / "gaps.jsonl")
    assert len(gaps.records()) == 1
    assert gaps.records()[0]["recorded_at"] == NOW.isoformat()

    restarted = _runtime(tmp_path, provider, clock, config=config)
    recovered = restarted.run_once(now=NOW)

    assert recovered.symbols_processed == 1
    assert len(gaps.records()) == 1
    assert JsonBucketCursorStore(cursor_path).get("ABCUSDT", "5m") == NOW


def test_interval_transition_realigns_cursor_to_global_symbol_chronology(
    tmp_path: Path,
) -> None:
    """Reproduce the live chronology -> retry backlog -> gap causal chain."""
    previous_detection = NOW - timedelta(seconds=30)
    states = WatchdogStateRepository(
        JsonWatchdogStateStore(tmp_path / "state" / "symbols.json")
    )
    states.save(
        WatchdogRuntimeState(
            WatchdogSymbolState(
                symbol="ABCUSDT",
                state=WatchdogState.NORMAL,
                changed_at=previous_detection,
                last_detected_at=previous_detection,
                last_score=0.0,
            )
        )
    )
    cursor_path = tmp_path / "state" / "buckets.json"
    JsonBucketCursorStore(cursor_path).save(
        "ABCUSDT", "5m", NOW - timedelta(minutes=30)
    )
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    runtime = _runtime(
        tmp_path,
        provider,
        clock,
        config=ShadowRuntimeConfig(
            maximum_catchup_buckets=2,
            maximum_workers=1,
            retry_jitter=0.0,
        ),
    )

    health = runtime.run_once(now=NOW)

    assert health.symbols_failed == 0
    assert health.symbols_processed == 1
    assert health.scheduling_gaps == 0
    assert JsonBucketCursorStore(cursor_path).get("ABCUSDT", "5m") == NOW
    audits = OperationalAuditJournal(
        tmp_path / "operational" / "runtime.jsonl"
    ).records()
    realignments = [
        item for item in audits if item["event_type"] == "CURSOR_REALIGNMENT"
    ]
    assert len(realignments) == 1
    assert realignments[0]["details"] == {
        "chronology_floor": (NOW - timedelta(minutes=5)).isoformat(),
        "interval": "5m",
        "last_detected_at": previous_detection.isoformat(),
        "previous_cursor": (NOW - timedelta(minutes=30)).isoformat(),
        "reason": "state_ahead_of_interval_cursor",
    }
    assert not any(item["event_type"] == "SYMBOL_FAILURE" for item in audits)


def test_timed_out_worker_prevents_overlapping_stale_symbol_plan(
    tmp_path: Path,
) -> None:
    clock = MutableClock(NOW)
    provider = BlockingProvider(clock, ("ABCUSDT",))
    runtime = _runtime(
        tmp_path,
        provider,
        clock,
        config=ShadowRuntimeConfig(
            provider_timeout=0.01,
            retry_attempts=1,
            maximum_workers=1,
            retry_jitter=0.0,
        ),
    )

    first = runtime.run_once(now=NOW)
    assert first.symbols_failed == 0
    assert first.scheduler_timeouts == 1
    assert first.late_worker_completions == 0
    assert provider.market_calls == [("ABCUSDT", "5m")]

    clock.now = NOW + timedelta(minutes=5)
    second = runtime.run_once(now=clock.now)
    assert second.symbols_failed == 0
    assert provider.market_calls == [("ABCUSDT", "5m")]

    provider.release.set()
    deadline = time.monotonic() + 2.0
    cursor_path = tmp_path / "state" / "buckets.json"
    while (
        JsonBucketCursorStore(cursor_path).get("ABCUSDT", "5m") is None
        and time.monotonic() < deadline
    ):
        time.sleep(0.01)

    journal = OperationalAuditJournal(tmp_path / "operational" / "runtime.jsonl")
    audits = journal.records()
    reconciliations = tuple(
        item
        for item in audits
        if item["event_type"] == "SCHEDULER_RECONCILIATION"
        and item["symbol"] == "ABCUSDT"
    )
    while not reconciliations and time.monotonic() < deadline:
        time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
        audits = journal.records()
        reconciliations = tuple(
            item
            for item in audits
            if item["event_type"] == "SCHEDULER_RECONCILIATION"
            and item["symbol"] == "ABCUSDT"
        )
    assert len(reconciliations) == 1, (
        "Durable scheduler reconciliation was not recorded."
    )
    assert reconciliations[0]["occurred_at"] == clock.now.isoformat()
    assert reconciliations[0]["details"] == {
        "interval": "5m",
        "boundaries": NOW.isoformat(),
        "status": "completed",
        "committed_after_timeout": "true",
        "failure": "none",
    }

    health = runtime.health_snapshot()
    assert health.symbols_processed == 1
    assert health.symbols_failed == 0
    assert health.late_worker_completions == 1
    assert health.committed_after_timeout == 1

    assert not any(
        item["event_type"] == "SYMBOL_FAILURE"
        and "State evaluations must be chronological." in str(item["details"])
        for item in audits
    )
    assert {item["event_type"] for item in audits} >= {
        "SCHEDULER_TIMEOUT",
        "SCHEDULER_RECONCILIATION",
    }


def test_queued_worker_gets_its_own_running_deadline(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    provider = SlowProvider(clock, ("ABCUSDT", "XYZUSDT"), 0.04)
    runtime = _runtime(
        tmp_path,
        provider,
        clock,
        config=ShadowRuntimeConfig(
            provider_timeout=0.06,
            retry_attempts=1,
            maximum_workers=1,
            maximum_symbols_per_loop=2,
            retry_jitter=0.0,
        ),
    )

    health = runtime.run_once(now=NOW)

    assert health.symbols_processed == 2
    assert health.symbols_failed == 0
    assert health.scheduler_timeouts == 0
    assert health.cancelled_before_start == 0
    assert provider.market_calls == [("ABCUSDT", "5m"), ("XYZUSDT", "5m")]


def test_queued_work_is_cancelled_separately_from_running_timeout(
    tmp_path: Path,
) -> None:
    clock = MutableClock(NOW)
    provider = BlockingProvider(clock, ("ABCUSDT", "XYZUSDT"))
    runtime = _runtime(
        tmp_path,
        provider,
        clock,
        config=ShadowRuntimeConfig(
            provider_timeout=0.01,
            retry_attempts=1,
            maximum_workers=1,
            maximum_symbols_per_loop=2,
            retry_jitter=0.0,
        ),
    )

    health = runtime.run_once(now=NOW)

    assert health.scheduler_timeouts == 1
    assert health.cancelled_before_start == 1
    assert health.symbols_failed == 1
    assert provider.market_calls == [("ABCUSDT", "5m")]
    rollups = OperationalAuditJournal(
        tmp_path / "operational" / "runtime.jsonl"
    ).rollups()
    assert {item["signature"] for item in rollups} >= {
        "scheduler_timeout:running",
        "scheduler_cancelled:before_start",
    }
    provider.release.set()


def test_tier_interval_transition_starts_after_global_chronology_floor(
    tmp_path: Path,
) -> None:
    previous_detection = NOW - timedelta(seconds=20)
    states = WatchdogStateRepository(
        JsonWatchdogStateStore(tmp_path / "state" / "symbols.json")
    )
    states.save(
        WatchdogRuntimeState(
            WatchdogSymbolState(
                symbol="ABCUSDT",
                state=WatchdogState.NORMAL,
                changed_at=previous_detection,
                last_detected_at=previous_detection,
                last_score=0.0,
            )
        )
    )
    # The prior STANDARD tier used 15m. The fresh universe snapshot promotes
    # the baseline-ready symbol to ACTIVE/5m, for which no cursor exists yet.
    JsonBucketCursorStore(tmp_path / "state" / "buckets.json").save(
        "ABCUSDT", "15m", NOW - timedelta(minutes=15)
    )
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))

    health = _runtime(tmp_path, provider, clock).run_once(now=NOW)

    assert health.symbols_failed == 0
    assert provider.market_calls == [("ABCUSDT", "5m")]
    audit = OperationalAuditJournal(
        tmp_path / "operational" / "runtime.jsonl"
    ).records()
    transition_alignment = next(
        item for item in audit if item["event_type"] == "CURSOR_REALIGNMENT"
    )
    details = transition_alignment["details"]
    assert isinstance(details, dict)
    assert details["previous_cursor"] == "none"


@pytest.mark.parametrize("failure_type", (RuntimeError, JournalConflictError))
def test_background_runtime_exposes_fatal_worker_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    runtime = _runtime(tmp_path, provider, clock)
    failure = (
        JournalConflictError(
            journal_path=tmp_path / "events.jsonl",
            record_id="event-1",
            existing_payload={"event_id": "event-1", "value": 1},
            attempted_payload={"event_id": "event-1", "value": 2},
        )
        if failure_type is JournalConflictError
        else RuntimeError("injected market loop failure")
    )

    def explode(*, now: datetime | None = None) -> object:
        del now
        raise failure

    monkeypatch.setattr(runtime, "run_once", explode)
    runtime.start()
    deadline = time.monotonic() + 2.0
    while runtime.running and time.monotonic() < deadline:
        time.sleep(0.01)

    assert runtime.running is False
    assert runtime.fatal_error is failure
    runtime.stop(timeout=2.0)


def _runtime(
    root: Path,
    provider: FixtureProvider,
    clock: MutableClock,
    *,
    sleep: Callable[[float], None] | None = None,
    config: ShadowRuntimeConfig | None = None,
    derivatives_provider: FailingDerivativesProvider | None = None,
    with_retention: bool = False,
) -> WatchdogShadowRuntime:
    baselines = RollingBaselineEngine(
        JsonBaselineStore(root / "state" / "baselines.json"),
        minimum_samples=3,
        maximum_samples=30,
    )

    if not baselines.observations:
        for index in range(3):
            available = NOW - timedelta(minutes=30 - index * 5)
            baselines.observe(
                BaselineObservation(
                    "ABCUSDT",
                    "relative_volume_20",
                    1.0,
                    available - timedelta(minutes=5),
                    available,
                ),
                detected_at=available,
            )
    states = WatchdogStateRepository(
        JsonWatchdogStateStore(root / "state" / "symbols.json")
    )
    events = WatchdogEventJournal(root / "events" / "events.jsonl")
    outcome_journal = WatchdogOutcomeJournal(root / "outcomes" / "outcomes.jsonl")
    outcomes = ForwardOutcomeScheduler(
        events,
        outcome_journal,
        JsonOutcomeCheckpointStore(root / "state" / "pending.json"),
        prices=WatchdogPriceJournal(root / "outcomes" / "price_observations.jsonl"),
    )
    cursors = JsonBucketCursorStore(root / "state" / "buckets.json")
    transitions = (
        UniverseTransitionJournal(root / "operational" / "universe-transitions.jsonl")
        if with_retention
        else None
    )
    retention = (
        InactiveRetention(
            baselines,
            states,
            cursors,
            InactiveSymbolArchive(root / "state" / "inactive.sqlite3"),
            transitions,
            window=timedelta(hours=1),
        )
        if transitions is not None
        else None
    )
    return WatchdogShadowRuntime(
        universe_provider=provider,
        market_provider=provider,
        derivatives_provider=derivatives_provider,
        universe=DynamicUniverse(),
        baseline_counts=(
            lambda now: (
                baselines.sample_counts("relative_volume_20", detected_at=now)
                if with_retention
                else {symbol: 20 for symbol in provider.symbols}
            )
        ),
        engine=WatchdogDetectionEngine(
            WatchdogFeatureBuilder(baselines),
            DetectorPipeline((VolumeShockDetector(),)),
            ExplainableAnomalyAggregator(),
            WatchdogStateMachine(),
            states,
        ),
        states=states,
        events=events,
        outcomes=outcomes,
        cursors=cursors,
        audit=OperationalAuditJournal(root / "operational" / "runtime.jsonl"),
        health_store=JsonRuntimeHealthStore(root / "state" / "health.json"),
        gaps=GapLedger(root / "operational" / "gaps.jsonl"),
        config=config
        or ShadowRuntimeConfig(
            universe_refresh=timedelta(minutes=15),
            retry_jitter=0.0,
            maximum_workers=2,
        ),
        clock=clock,
        monotonic=time_counter(),
        sleep=sleep or time.sleep,
        random_value=lambda: 0.5,
        baseline_retained_counts=lambda: baselines.retained_counts,
        universe_transitions=transitions,
        inactive_retention=retention,
    )


def test_universe_reentry_after_expiry_is_cold_and_skips_inactive_buckets(
    tmp_path: Path,
) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT",))
    runtime = _runtime(tmp_path, provider, clock, with_retention=True)
    retention = runtime._inactive_retention
    assert retention is not None
    baselines = retention._baselines
    for index in range(17):
        available = NOW - timedelta(minutes=19 - index)
        baselines.observe(
            BaselineObservation(
                "ABCUSDT",
                "relative_volume_20",
                1.0,
                available - timedelta(minutes=1),
                available,
            ),
            detected_at=available,
        )
    runtime._refresh_universe(NOW)
    assert runtime._universe is not None
    assert runtime._universe.eligible[0].tier.value == "ACTIVE"
    runtime._cursors.save("ABCUSDT", "15m", NOW)
    runtime._states.save(
        WatchdogRuntimeState(WatchdogSymbolState.initial("ABCUSDT", detected_at=NOW))
    )

    provider.symbols = ()
    runtime._refresh_universe(NOW + timedelta(minutes=15))
    runtime._refresh_universe(NOW + timedelta(minutes=75))
    assert baselines.retained_counts[1] == 0
    provider.symbols = ("ABCUSDT",)
    runtime._refresh_universe(NOW + timedelta(minutes=90))
    assert runtime._universe is not None
    assert runtime._universe.eligible[0].tier.value == "COLD_START"
    assert runtime._states.persisted("ABCUSDT") is not None
    assert runtime._cursors.get("ABCUSDT", "15m") == NOW + timedelta(minutes=75)
    assert runtime._gaps is not None
    gaps = runtime._gaps.records()
    assert len(gaps) == 1
    assert gaps[0]["reason"] == "inactive_reentry"
    assert (
        gaps[0]["first_missing_boundary"] == (NOW + timedelta(minutes=15)).isoformat()
    )
    assert gaps[0]["last_missing_boundary"] == (NOW + timedelta(minutes=75)).isoformat()
    retained = dict(runtime._retained_counts())
    for name in (
        "event_journal_ram_index",
        "outcome_journal_ram_index",
        "price_journal_ram_index",
        "audit_journal_ram_index",
        "storage_journal_ram_index",
        "universe_transition_ram_index",
    ):
        assert retained[name] == 0


def time_counter() -> Callable[[], float]:
    value = [0.0]

    def counter() -> float:
        value[0] += 0.001
        return value[0]

    return counter


def _catalog(symbol: str) -> CatalogInstrument:
    return CatalogInstrument(
        symbol=symbol,
        quote_coin="USDT",
        status="Trading",
        turnover_24h=30_000_000.0,
        bid=100.0,
        ask=100.1,
        base_coin=symbol.removesuffix("USDT"),
        settle_coin="USDT",
        contract_type="LinearPerpetual",
        symbol_type="",
        is_pre_listing=False,
        launch_time=NOW - timedelta(days=30),
    )


def _series(symbol: str, interval: str, now: datetime) -> MarketSeries:
    minutes = int(interval.removesuffix("m"))
    duration = timedelta(minutes=minutes)
    candles = []
    for index in range(25):
        timestamp = now - duration * (25 - index)
        close = 100.0 + index * 0.01
        volume = 200.0 if index == 24 and now == NOW else 100.0
        candles.append(
            Candle(timestamp, close, close + 0.1, close - 0.1, close, volume)
        )
    return MarketSeries(Instrument(symbol, AssetClass.CRYPTO), interval, tuple(candles))
