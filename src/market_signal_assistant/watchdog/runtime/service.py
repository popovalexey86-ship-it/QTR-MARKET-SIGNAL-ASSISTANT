from __future__ import annotations

import hashlib
import re
import time
import traceback
from collections import deque
from collections.abc import Callable, Mapping
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from threading import Event, Lock, Thread
from typing import Protocol, TypeVar

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.models import AssetClass, Candle, Instrument, MarketSeries
from market_signal_assistant.watchdog.engine import WatchdogDetectionEngine
from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.journal import JournalConflictError
from market_signal_assistant.watchdog.outcomes.models import PriceObservation
from market_signal_assistant.watchdog.outcomes.scheduler import ForwardOutcomeScheduler
from market_signal_assistant.watchdog.runtime.audit import (
    OperationalAuditJournal,
    OperationalEvent,
    OperationalEventType,
)
from market_signal_assistant.watchdog.runtime.gaps import GapLedger, SchedulingGap
from market_signal_assistant.watchdog.runtime.health import JsonRuntimeHealthStore
from market_signal_assistant.watchdog.runtime.index import WatchdogIndexManager
from market_signal_assistant.watchdog.runtime.lock import SingleInstanceLock
from market_signal_assistant.watchdog.runtime.models import (
    PollingPolicy,
    RuntimeHealthSnapshot,
    ShadowRuntimeConfig,
    interest_tier,
)
from market_signal_assistant.watchdog.runtime.retry import (
    RetryPolicy,
    is_rate_limit_error,
    retry_call,
)
from market_signal_assistant.watchdog.runtime.schedule import (
    CompletedBucketScheduler,
    JsonBucketCursorStore,
    completed_boundary,
    interval_duration,
)
from market_signal_assistant.watchdog.runtime.storage import (
    StorageMonitor,
    StorageSnapshot,
    StorageTelemetryJournal,
)
from market_signal_assistant.watchdog.state_store import WatchdogStateRepository
from market_signal_assistant.watchdog.universe import (
    DynamicUniverse,
    UniverseEntry,
    UniverseSnapshot,
)

T = TypeVar("T")


class RuntimeUniverseProvider(Protocol):
    def list_instruments(self) -> tuple[CatalogInstrument, ...]: ...


class RuntimeMarketProvider(Protocol):
    def load(
        self, instrument: Instrument, interval: str, limit: int
    ) -> MarketSeries: ...


class RuntimeDerivativesProvider(Protocol):
    def collect(self, symbol: str) -> DerivativesSnapshot: ...


@dataclass(frozen=True, slots=True)
class SymbolRunResult:
    symbol: str
    processed_buckets: int
    events: int
    outcomes: int
    missing_data: int
    stale_data: int
    latency_seconds: float
    failed: bool = False
    failure: str | None = None


@dataclass(slots=True)
class _ScheduledSymbolWork:
    entry: UniverseEntry
    interval: str
    boundaries: tuple[datetime, ...]
    submitted_at: float
    started_at: float | None = None
    timed_out: bool = False
    cancelled_before_start: bool = False
    lock: Lock = field(default_factory=Lock)

    @property
    def symbol(self) -> str:
        return self.entry.instrument.symbol


class WatchdogShadowRuntime:
    """Explicit, bounded, shadow-only orchestration over Phase 1-3 services."""

    def __init__(
        self,
        *,
        universe_provider: RuntimeUniverseProvider,
        market_provider: RuntimeMarketProvider,
        universe: DynamicUniverse,
        baseline_counts: Callable[[datetime], Mapping[str, int]],
        engine: WatchdogDetectionEngine,
        states: WatchdogStateRepository,
        events: WatchdogEventJournal,
        outcomes: ForwardOutcomeScheduler,
        cursors: JsonBucketCursorStore,
        audit: OperationalAuditJournal,
        health_store: JsonRuntimeHealthStore,
        derivatives_provider: RuntimeDerivativesProvider | None = None,
        config: ShadowRuntimeConfig | None = None,
        polling: PollingPolicy | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] | None = None,
        instance_lock: SingleInstanceLock | None = None,
        gaps: GapLedger | None = None,
        indexes: WatchdogIndexManager | None = None,
        storage_monitor: StorageMonitor | None = None,
        storage_telemetry: StorageTelemetryJournal | None = None,
        baseline_retained_counts: Callable[[], tuple[int, int, int]] | None = None,
    ) -> None:
        self._universe_provider = universe_provider
        self._market_provider = market_provider
        self._derivatives_provider = derivatives_provider
        self._universe_builder = universe
        self._baseline_counts = baseline_counts
        self._engine = engine
        self._states = states
        self._events = events
        self._outcomes = outcomes
        self._cursors = cursors
        self._audit_journal = audit
        self._health_store = health_store
        self._config = config or ShadowRuntimeConfig()
        self._polling = polling or PollingPolicy()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic
        self._sleep = sleep
        self._random_value = random_value
        self._instance_lock = instance_lock
        self._gaps = gaps
        self._indexes = indexes
        self._storage_monitor = storage_monitor
        self._storage_telemetry = storage_telemetry
        self._baseline_retained_counts = baseline_retained_counts
        self._scheduler = CompletedBucketScheduler(self._config.maximum_catchup_buckets)
        self._retry = RetryPolicy(
            self._config.retry_attempts,
            self._config.retry_base_delay,
            self._config.retry_max_delay,
            self._config.retry_jitter,
        )
        self._stop = Event()
        self._thread: Thread | None = None
        self._pipeline_lock = Lock()
        self._metrics_lock = Lock()
        self._inflight_lock = Lock()
        self._inflight_symbols: set[str] = set()
        self._inflight_futures: set[Future[SymbolRunResult]] = set()
        self._started_at: datetime | None = None
        self._last_loop_at: datetime | None = None
        self._last_market_update: datetime | None = None
        self._last_universe_refresh: datetime | None = None
        self._universe: UniverseSnapshot | None = None
        self._last_checks: dict[str, datetime] = {}
        self._symbols_processed = 0
        self._symbols_failed = 0
        self._provider_errors = 0
        self._provider_failures = 0
        self._rate_limit_events = 0
        self._api_calls = 0
        self._api_call_times: deque[float] = deque()
        self._peak_api_calls_per_minute = 0
        self._minimum_api_spacing: float | None = None
        self._last_api_call_at: float | None = None
        self._throttle_waits = 0
        self._scheduler_timeouts = 0
        self._cancelled_before_start = 0
        self._late_worker_completions = 0
        self._committed_after_timeout = 0
        self._loop_duration = 0.0
        self._max_symbol_latency = 0.0
        self._stale_data = 0
        self._missing_data = 0
        self._degraded_reasons: set[str] = set()
        self._rotation_offset = 0
        self._loop_durations: list[float] = []
        self._storage_snapshot: StorageSnapshot | None = None
        self._failure_lock = Lock()
        self._fatal_error: Exception | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def fatal_error(self) -> Exception | None:
        with self._failure_lock:
            return self._fatal_error

    @property
    def universe_snapshot(self) -> UniverseSnapshot | None:
        return self._universe

    @property
    def loop_durations(self) -> tuple[float, ...]:
        return tuple(self._loop_durations)

    def record_api_call(self) -> None:
        now = self._monotonic()
        with self._metrics_lock:
            self._api_calls += 1
            while self._api_call_times and now - self._api_call_times[0] >= 60.0:
                self._api_call_times.popleft()
            self._api_call_times.append(now)
            self._peak_api_calls_per_minute = max(
                self._peak_api_calls_per_minute, len(self._api_call_times)
            )
            if self._last_api_call_at is not None:
                spacing = max(0.0, now - self._last_api_call_at)
                self._minimum_api_spacing = (
                    spacing
                    if self._minimum_api_spacing is None
                    else min(self._minimum_api_spacing, spacing)
                )
            self._last_api_call_at = now

    def record_throttle_wait(self, wait_seconds: float) -> None:
        now = self._clock()
        with self._metrics_lock:
            self._throttle_waits += 1
        self._audit(
            OperationalEventType.THROTTLE_WAIT,
            now,
            (("wait_seconds", f"{wait_seconds:.6f}"),),
            rollup_signature="throttle_wait:global_api_budget",
        )

    def start(self) -> None:
        if self.running:
            return
        if self._instance_lock is not None:
            self._instance_lock.acquire()
        self._stop.clear()
        with self._failure_lock:
            self._fatal_error = None
        self._mark_started(self._clock())
        self._thread = Thread(
            target=self._run_forever,
            name="watchdog-shadow-runtime",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = 30.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
            if thread.is_alive():
                raise TimeoutError("Watchdog runtime did not stop cleanly.")
        with self._inflight_lock:
            inflight = tuple(self._inflight_futures)
        if inflight:
            _done, pending = wait(inflight, timeout=timeout)
            if pending:
                raise TimeoutError("Watchdog symbol workers did not stop cleanly.")
        self._thread = None
        if self._started_at is not None:
            self._audit(
                OperationalEventType.RUNTIME_STOP,
                self._clock(),
                (("status", "clean"),),
            )
        self._write_health()
        if self._instance_lock is not None:
            self._instance_lock.release()

    def run_once(self, *, now: datetime | None = None) -> RuntimeHealthSnapshot:
        acquired_here = False
        if self._instance_lock is not None and not self._instance_lock.held:
            self._instance_lock.acquire()
            acquired_here = True
        loop_started = self._monotonic()
        loop_time = _utc(now or self._clock())
        self._mark_started(loop_time)
        self._degraded_reasons.clear()
        if self._storage_monitor is not None:
            try:
                self._storage_snapshot = self._storage_monitor.snapshot(
                    recorded_at=loop_time
                )
                if self._storage_telemetry is not None:
                    self._storage_telemetry.append(self._storage_snapshot)
            except OSError:
                self._degraded("storage_telemetry_unavailable", loop_time)
            if self._storage_snapshot is not None and self._storage_snapshot.pressure:
                self._degraded("disk_pressure", loop_time)
                self._finish_loop(loop_time, loop_started, ())
                snapshot = self.health_snapshot()
                if acquired_here and self._instance_lock is not None:
                    self._instance_lock.release()
                return snapshot
        self._refresh_universe(loop_time)
        universe = self._universe
        if universe is None:
            self._degraded("universe_unavailable", loop_time)
            self._finish_loop(loop_time, loop_started, ())
            snapshot = self.health_snapshot()
            if acquired_here and self._instance_lock is not None:
                self._instance_lock.release()
            return snapshot

        due_candidates: list[tuple[UniverseEntry, str, tuple[datetime, ...]]] = []
        for entry in universe.eligible:
            symbol = entry.instrument.symbol
            with self._inflight_lock:
                if symbol in self._inflight_symbols:
                    continue
            state = self._states.get(symbol, detected_at=loop_time)
            tier = interest_tier(entry.tier, state.symbol_state.state)
            rule = self._polling.rule(tier)
            last_check = self._last_checks.get(symbol)
            if last_check is not None and loop_time - last_check < rule.check_every:
                continue
            cursor = self._cursors.get(symbol, rule.interval)
            persisted = self._states.persisted(symbol)
            if persisted is not None:
                chronology_floor = completed_boundary(
                    persisted.symbol_state.last_detected_at,
                    rule.interval,
                )
                if cursor is None or cursor < chronology_floor:
                    self._cursors.save(symbol, rule.interval, chronology_floor)
                    self._audit(
                        OperationalEventType.CURSOR_REALIGNMENT,
                        loop_time,
                        (
                            ("interval", rule.interval),
                            (
                                "previous_cursor",
                                cursor.isoformat() if cursor is not None else "none",
                            ),
                            ("chronology_floor", chronology_floor.isoformat()),
                            (
                                "last_detected_at",
                                persisted.symbol_state.last_detected_at.isoformat(),
                            ),
                            ("reason", "state_ahead_of_interval_cursor"),
                        ),
                        symbol=symbol,
                    )
                    cursor = chronology_floor
            plan = self._scheduler.plan(
                now=loop_time,
                interval=rule.interval,
                last_completed=cursor,
            )
            boundaries = plan.buckets
            if plan.omitted_bucket_count and self._gaps is not None:
                duration = interval_duration(rule.interval)
                previous = self._cursors.get(symbol, rule.interval)
                if previous is not None:
                    first = previous + duration
                    missing_count = plan.omitted_bucket_count + max(
                        0, len(boundaries) - 1
                    )
                    missing_through = first + duration * (missing_count - 1)
                    self._gaps.append(
                        SchedulingGap(
                            symbol,
                            rule.interval,
                            first,
                            missing_through,
                            missing_count,
                            missing_through + duration,
                        )
                    )
                    # Persist acknowledgement of the intentionally skipped
                    # historical range before processing the latest bucket.
                    # If that latest bucket fails, the next loop retries only
                    # it instead of emitting the same immutable gap again.
                    self._cursors.save(
                        symbol,
                        rule.interval,
                        missing_through,
                    )
                    # Do not manufacture retrospective detections from candles
                    # fetched only after a long outage. Resume at the latest
                    # completed bucket; the ledger preserves the skipped range.
                    boundaries = boundaries[-1:]
            if boundaries:
                due_candidates.append((entry, rule.interval, boundaries))

        due = _rotating_batch(
            due_candidates,
            offset=self._rotation_offset,
            limit=self._config.maximum_symbols_per_loop,
        )
        if due_candidates:
            self._rotation_offset = (self._rotation_offset + len(due)) % len(
                due_candidates
            )
        results: list[SymbolRunResult] = []
        executor = ThreadPoolExecutor(
            max_workers=self._config.maximum_workers,
            thread_name_prefix="watchdog-symbol",
        )
        try:
            futures: dict[Future[SymbolRunResult], _ScheduledSymbolWork] = {}
            for entry, interval, boundaries in due:
                symbol = entry.instrument.symbol
                if not self._claim_symbol(symbol):
                    continue
                work = _ScheduledSymbolWork(
                    entry,
                    interval,
                    boundaries,
                    self._monotonic(),
                )
                try:
                    future = executor.submit(self._execute_work, work)
                except Exception:
                    self._release_symbol(symbol)
                    raise
                with self._inflight_lock:
                    self._inflight_futures.add(future)
                future.add_done_callback(partial(self._complete_work, work=work))
                futures[future] = work
                self._last_checks[symbol] = loop_time
            pending = set(futures)
            timeout = self._config.provider_timeout * self._config.retry_attempts
            while pending:
                done, _ = wait(pending, timeout=min(0.01, timeout))
                for future in done:
                    pending.discard(future)
                    results.append(self._resolved_result(future, futures[future]))
                now_monotonic = self._monotonic()
                for future in tuple(pending):
                    work = futures[future]
                    with work.lock:
                        completed_now = future.done()
                        started_at = work.started_at
                        should_timeout = (
                            not completed_now
                            and started_at is not None
                            and now_monotonic - started_at >= timeout
                        )
                        if should_timeout:
                            work.timed_out = True
                    if completed_now:
                        pending.discard(future)
                        results.append(self._resolved_result(future, work))
                        continue
                    if not should_timeout:
                        continue
                    pending.discard(future)
                    cancelled = future.cancel()
                    with self._metrics_lock:
                        self._scheduler_timeouts += 1
                    self._audit_scheduler_timeout(work, cancelled=cancelled)
                    if cancelled:
                        self._record_cancelled_before_start(work, results)
                running_detached = sum(
                    work.timed_out and not future.done()
                    for future, work in futures.items()
                )
                queued = tuple(
                    (future, futures[future])
                    for future in pending
                    if futures[future].started_at is None
                )
                if queued and running_detached >= self._config.maximum_workers:
                    for future, work in queued:
                        if not future.cancel():
                            continue
                        pending.discard(future)
                        self._record_cancelled_before_start(work, results)
        finally:
            executor.shutdown(wait=False, cancel_futures=False)
        self._finish_loop(loop_time, loop_started, tuple(results))
        snapshot = self.health_snapshot()
        if acquired_here and self._instance_lock is not None:
            self._instance_lock.release()
        return snapshot

    def health_snapshot(self) -> RuntimeHealthSnapshot:
        now = self._last_loop_at or self._clock()
        today = _utc(now).date()
        events_today = self._events.count_detected_on(today)
        entries = self._universe.eligible if self._universe is not None else ()
        tiers = sorted({item.tier.value for item in entries})
        readiness = tuple(
            (
                tier,
                sum(item.tier.value == tier for item in entries),
                sum(
                    item.tier.value == tier and item.baseline_samples >= 20
                    for item in entries
                ),
            )
            for tier in tiers
        )
        blocking_reasons = tuple(
            sorted(
                reason
                for reason in self._degraded_reasons
                if reason
                in {
                    "disk_pressure",
                    "market:no-symbol-progress",
                    "storage_telemetry_unavailable",
                    "universe_unavailable",
                }
                or reason.startswith("fatal:")
            )
        )
        return RuntimeHealthSnapshot(
            started_at=self._started_at,
            last_loop_at=self._last_loop_at,
            last_successful_market_update=self._last_market_update,
            symbols_in_universe=(
                len(self._universe.eligible) if self._universe is not None else 0
            ),
            symbols_processed=self._symbols_processed,
            symbols_failed=self._symbols_failed,
            events_today=events_today,
            pending_outcomes=self._outcomes.pending_horizon_count(),
            provider_errors=self._provider_errors,
            rate_limit_events=self._rate_limit_events,
            loop_duration_seconds=self._loop_duration,
            max_symbol_latency_seconds=self._max_symbol_latency,
            stale_data_count=self._stale_data,
            missing_data_count=self._missing_data,
            api_calls=self._api_calls,
            scheduling_gaps=(self._gaps.record_count if self._gaps else 0),
            storage_bytes=(
                self._storage_snapshot.total_bytes
                if self._storage_snapshot is not None
                else 0
            ),
            disk_free_bytes=(
                self._storage_snapshot.disk_free_bytes
                if self._storage_snapshot is not None
                else 0
            ),
            disk_pressure=(
                self._storage_snapshot.pressure
                if self._storage_snapshot is not None
                else False
            ),
            process_rss_bytes=(
                self._storage_snapshot.process_rss_bytes
                if self._storage_snapshot is not None
                else None
            ),
            baseline_ready_symbols=sum(item.baseline_samples >= 20 for item in entries),
            cold_start_symbols=sum(item.tier.value == "COLD_START" for item in entries),
            readiness_by_tier=readiness,
            degraded=bool(self._degraded_reasons),
            degraded_reasons=tuple(sorted(self._degraded_reasons)),
            next_market_update_due_at=self._next_market_update_due(now),
            throttle_waits=self._throttle_waits,
            api_calls_last_minute=len(self._api_call_times),
            peak_api_calls_per_minute=self._peak_api_calls_per_minute,
            minimum_api_spacing_seconds=self._minimum_api_spacing,
            acceptance_blocked=bool(blocking_reasons),
            acceptance_blocking_reasons=blocking_reasons,
            scheduler_timeouts=self._scheduler_timeouts,
            cancelled_before_start=self._cancelled_before_start,
            late_worker_completions=self._late_worker_completions,
            committed_after_timeout=self._committed_after_timeout,
            retained_counts=self._retained_counts(),
            provider_failures=self._provider_failures,
        )

    def _retained_counts(self) -> tuple[tuple[str, int], ...]:
        pending_events, pending_points, completed, used = (
            self._outcomes.retained_counts
        )
        outcome_journal_index, price_journal_index = (
            self._outcomes.journal_retained_counts
        )
        baseline_keys = baseline_observations = baseline_bound = 0
        if self._baseline_retained_counts is not None:
            baseline_keys, baseline_observations, baseline_bound = (
                self._baseline_retained_counts()
            )
        with self._inflight_lock:
            inflight_symbols = len(self._inflight_symbols)
            inflight_futures = len(self._inflight_futures)
        return (
            ("baseline_keys", baseline_keys),
            ("baseline_observations", baseline_observations),
            ("baseline_per_key_bound", baseline_bound),
            ("pending_events", pending_events),
            ("pending_price_points", pending_points),
            ("pending_completed_horizons", completed),
            ("pending_used_observations", used),
            ("state_symbols", self._states.retained_count),
            ("scheduler_cursors", self._cursors.retained_count),
            ("scheduler_last_checks", len(self._last_checks)),
            ("inflight_symbols", inflight_symbols),
            ("inflight_futures", inflight_futures),
            ("event_journal_ram_index", self._events.retained_index_entries),
            ("outcome_journal_ram_index", outcome_journal_index),
            ("price_journal_ram_index", price_journal_index),
            ("audit_journal_ram_index", self._audit_journal.retained_index_entries),
            (
                "storage_journal_ram_index",
                self._storage_telemetry.retained_index_entries
                if self._storage_telemetry is not None
                else 0,
            ),
        )

    def _next_market_update_due(self, now: datetime) -> datetime | None:
        if self._universe is None:
            return None
        due_times: list[datetime] = []
        for entry in self._universe.eligible:
            state = self._states.get(entry.instrument.symbol, detected_at=now)
            rule = self._polling.rule(
                interest_tier(entry.tier, state.symbol_state.state)
            )
            last_check = self._last_checks.get(entry.instrument.symbol)
            due_times.append(
                now if last_check is None else last_check + rule.check_every
            )
        return min(due_times, default=None)

    def _run_forever(self) -> None:
        try:
            while not self._stop.is_set():
                self.run_once()
                self._stop.wait(self._config.loop_interval.total_seconds())
        except Exception as error:
            with self._failure_lock:
                self._fatal_error = error
            occurred_at = self._clock()
            details = [("error", type(error).__name__)]
            if isinstance(error, JournalConflictError):
                details.extend(
                    (
                        ("journal", str(error.journal_path)),
                        ("record_id", error.record_id),
                    )
                )
            try:
                self._audit(
                    OperationalEventType.FATAL_ERROR,
                    occurred_at,
                    tuple(details),
                )
                self._degraded(f"fatal:{type(error).__name__}", occurred_at)
                self._write_health()
            except Exception:
                # The original failure remains authoritative for the supervisor.
                return

    def _refresh_universe(self, now: datetime) -> None:
        if (
            self._last_universe_refresh is not None
            and now - self._last_universe_refresh < self._config.universe_refresh
        ):
            return
        previous = (
            {item.instrument.symbol for item in self._universe.eligible}
            if self._universe is not None
            else set()
        )
        try:
            instruments = self._provider_call(
                "universe", None, self._universe_provider.list_instruments
            )
            snapshot = self._universe_builder.build(
                instruments,
                observed_at=now,
                baseline_samples=self._baseline_counts(now),
            )
        except Exception as error:
            self._degraded(f"universe:{type(error).__name__}", now)
            return
        current = {item.instrument.symbol for item in snapshot.eligible}
        self._universe = snapshot
        self._last_checks = {
            symbol: checked_at
            for symbol, checked_at in self._last_checks.items()
            if symbol in current
        }
        self._last_universe_refresh = now
        self._audit(
            OperationalEventType.UNIVERSE_REFRESH,
            now,
            (
                ("eligible", str(len(current))),
                ("added", str(len(current - previous))),
                ("removed", str(len(previous - current))),
                ("rejected", str(len(snapshot.rejected))),
            ),
        )

    def _process_symbol(
        self,
        entry: UniverseEntry,
        interval: str,
        boundaries: tuple[datetime, ...],
    ) -> SymbolRunResult:
        started = self._monotonic()
        symbol = entry.instrument.symbol
        instrument = Instrument(symbol, AssetClass.CRYPTO)
        try:
            series = self._provider_call(
                "market",
                symbol,
                lambda: self._market_provider.load(
                    instrument, interval, self._config.candle_limit
                ),
            )
            with self._metrics_lock:
                self._last_market_update = self._clock()
            derivatives: DerivativesSnapshot | None = None
            state = self._states.get(symbol, detected_at=boundaries[-1])
            tier = interest_tier(entry.tier, state.symbol_state.state)
            derivatives_provider = self._derivatives_provider
            if (
                derivatives_provider is not None
                and self._polling.rule(tier).derivatives
            ):
                try:
                    derivatives = self._provider_call(
                        "derivatives",
                        symbol,
                        lambda: derivatives_provider.collect(symbol),
                    )
                except Exception as error:
                    self._degraded(
                        f"derivatives:{symbol}:{type(error).__name__}", self._clock()
                    )

            event_count = outcome_count = missing = stale = processed = 0
            with self._pipeline_lock:
                for index, boundary in enumerate(boundaries):
                    detected_at = (
                        _utc(self._clock())
                        if index == len(boundaries) - 1
                        else boundary
                    )
                    usable_derivatives = (
                        derivatives
                        if derivatives is not None and derivatives.as_of <= detected_at
                        else None
                    )
                    result = self._engine.evaluate(
                        series,
                        detected_at=detected_at,
                        derivatives=usable_derivatives,
                    )
                    missing += len(result.snapshot.missing_data)
                    stale += len(result.snapshot.stale_data)
                    if result.event is not None:
                        candle = _last_completed_candle(series, detected_at)
                        evidence = WatchdogEventEvidence.from_detection(
                            result,
                            price_at_detection=candle.close,
                            universe_tier=entry.tier.value,
                        )
                        if self._events.append(evidence):
                            event_count += 1
                        self._outcomes.register(evidence)
                    self._cursors.save(symbol, interval, boundary)
                    processed += 1
                pending_symbols = {
                    item.symbol for item in self._outcomes.pending_horizons()
                }
                if symbol in pending_symbols:
                    candle = _last_completed_candle(series, boundaries[-1])
                    point = PriceObservation(
                        symbol=symbol,
                        observed_at=candle.timestamp + interval_duration(interval),
                        available_at=max(
                            _utc(self._clock()),
                            candle.timestamp + interval_duration(interval),
                        ),
                        price=candle.close,
                        high=candle.high,
                        low=candle.low,
                        source=f"bybit-public-{interval}",
                    )
                    outcome_count += len(self._outcomes.observe(point))
            return SymbolRunResult(
                symbol,
                processed,
                event_count,
                outcome_count,
                missing,
                stale,
                self._monotonic() - started,
            )
        except Exception as error:
            self._audit(
                OperationalEventType.SYMBOL_FAILURE,
                self._clock(),
                (
                    ("error_type", type(error).__name__),
                    ("error_message", str(error)[:1000]),
                    ("interval", interval),
                    ("boundaries", ",".join(item.isoformat() for item in boundaries)),
                    ("traceback", traceback.format_exc(limit=20)[-8000:]),
                ),
                symbol=symbol,
                rollup_signature=(
                    f"symbol_pipeline:{type(error).__name__}:"
                    f"{_safe_error_signature(error)}"
                ),
            )
            return SymbolRunResult(
                symbol,
                0,
                0,
                0,
                0,
                0,
                self._monotonic() - started,
                True,
                type(error).__name__,
            )

    def _execute_work(self, work: _ScheduledSymbolWork) -> SymbolRunResult:
        with work.lock:
            work.started_at = self._monotonic()
        return self._process_symbol(work.entry, work.interval, work.boundaries)

    @staticmethod
    def _resolved_result(
        future: Future[SymbolRunResult], work: _ScheduledSymbolWork
    ) -> SymbolRunResult:
        try:
            return future.result()
        except (CancelledError, Exception) as error:
            return SymbolRunResult(
                work.symbol,
                0,
                0,
                0,
                0,
                0,
                0.0,
                True,
                type(error).__name__,
            )

    def _audit_scheduler_timeout(
        self, work: _ScheduledSymbolWork, *, cancelled: bool
    ) -> None:
        self._audit(
            OperationalEventType.SCHEDULER_TIMEOUT,
            self._clock(),
            (
                ("stage", "running"),
                ("interval", work.interval),
                ("boundaries", ",".join(item.isoformat() for item in work.boundaries)),
                ("cancel_requested", "true"),
                ("cancel_succeeded", str(cancelled).lower()),
            ),
            symbol=work.symbol,
            rollup_signature="scheduler_timeout:running",
        )

    def _record_cancelled_before_start(
        self,
        work: _ScheduledSymbolWork,
        results: list[SymbolRunResult],
    ) -> None:
        with work.lock:
            work.cancelled_before_start = True
        with self._metrics_lock:
            self._cancelled_before_start += 1
        self._audit(
            OperationalEventType.SCHEDULER_TIMEOUT,
            self._clock(),
            (
                ("stage", "queued"),
                ("interval", work.interval),
                ("boundaries", ",".join(item.isoformat() for item in work.boundaries)),
                ("cancel_requested", "true"),
                ("cancel_succeeded", "true"),
            ),
            symbol=work.symbol,
            rollup_signature="scheduler_cancelled:before_start",
        )
        results.append(
            SymbolRunResult(
                work.symbol,
                0,
                0,
                0,
                0,
                0,
                0.0,
                True,
                "CancelledBeforeStart",
            )
        )

    def _provider_call(
        self,
        operation: str,
        symbol: str | None,
        callback: Callable[[], T],
    ) -> T:
        def on_error(error: Exception, attempt: int, backoff: float) -> None:
            rate_limited = is_rate_limit_error(error)
            with self._metrics_lock:
                self._provider_errors += 1
                if rate_limited:
                    self._rate_limit_events += 1
                else:
                    self._provider_failures += 1
            status, ret_code = _provider_codes(error)
            self._audit(
                OperationalEventType.RATE_LIMIT
                if rate_limited
                else OperationalEventType.PROVIDER_FAILURE,
                self._clock(),
                (
                    ("endpoint_class", operation),
                    ("attempt", str(attempt)),
                    ("error", type(error).__name__),
                    ("http_status", status),
                    ("ret_code", ret_code),
                    ("backoff_seconds", f"{backoff:.6f}"),
                ),
                symbol=symbol,
                rollup_signature=(
                    f"provider:{'rate_limit' if rate_limited else 'failure'}:"
                    f"{operation}:{type(error).__name__}:{status}:{ret_code}"
                ),
            )

        return retry_call(
            callback,
            policy=self._retry,
            sleep=self._sleep,
            random_value=self._random_value or __import__("random").random,
            on_error=on_error,
        )

    def _claim_symbol(self, symbol: str) -> bool:
        with self._inflight_lock:
            if symbol in self._inflight_symbols:
                return False
            self._inflight_symbols.add(symbol)
            return True

    def _release_symbol(self, symbol: str) -> None:
        with self._inflight_lock:
            self._inflight_symbols.discard(symbol)

    def _complete_work(
        self,
        future: Future[SymbolRunResult],
        *,
        work: _ScheduledSymbolWork,
    ) -> None:
        with self._inflight_lock:
            self._inflight_futures.discard(future)
            self._inflight_symbols.discard(work.symbol)
        with work.lock:
            reconcile = work.timed_out and not work.cancelled_before_start
        if not reconcile:
            return
        try:
            result = future.result()
        except CancelledError:
            return
        except Exception as error:
            result = SymbolRunResult(
                work.symbol,
                0,
                0,
                0,
                0,
                0,
                0.0,
                True,
                type(error).__name__,
            )
        committed = result.processed_buckets > 0
        with self._metrics_lock:
            self._late_worker_completions += 1
            self._committed_after_timeout += int(committed)
            self._symbols_processed += int(not result.failed)
            self._symbols_failed += int(result.failed)
            self._missing_data += result.missing_data
            self._stale_data += result.stale_data
        self._audit(
            OperationalEventType.SCHEDULER_RECONCILIATION,
            self._clock(),
            (
                ("interval", work.interval),
                ("boundaries", ",".join(item.isoformat() for item in work.boundaries)),
                ("status", "failed" if result.failed else "completed"),
                ("committed_after_timeout", str(committed).lower()),
                ("failure", result.failure or "none"),
            ),
            symbol=work.symbol,
            rollup_signature=(
                "scheduler_reconciliation:failed"
                if result.failed
                else "scheduler_reconciliation:completed"
            ),
        )
        self._write_health()

    def _finish_loop(
        self,
        now: datetime,
        started: float,
        results: tuple[SymbolRunResult, ...],
    ) -> None:
        self._last_loop_at = now
        self._loop_duration = max(0.0, self._monotonic() - started)
        self._loop_durations.append(self._loop_duration)
        self._loop_durations = self._loop_durations[-10_000:]
        self._max_symbol_latency = max(
            (item.latency_seconds for item in results), default=0.0
        )
        self._symbols_processed += sum(not item.failed for item in results)
        self._symbols_failed += sum(item.failed for item in results)
        self._missing_data += sum(item.missing_data for item in results)
        self._stale_data += sum(item.stale_data for item in results)
        if results and all(item.failed for item in results):
            self._degraded("market:no-symbol-progress", now)
        # Timed-out provider futures may still finish. Serialize recovery and
        # indexing with their evidence transaction instead of racing mutable
        # outcome indexes or observing a half-finished journal append.
        with self._pipeline_lock:
            self._outcomes.recover_pending()
            if self._indexes is not None:
                self._indexes.ensure()
            self._write_health()

    def _mark_started(self, now: datetime) -> None:
        if self._started_at is not None:
            return
        self._started_at = _utc(now)
        recovered = len(self._outcomes.recover_pending())
        self._audit(
            OperationalEventType.RUNTIME_START,
            self._started_at,
            (("mode", "shadow-only"),),
        )
        self._audit(
            OperationalEventType.RECOVERY,
            self._started_at,
            (
                ("events", str(self._events.recovery.record_count)),
                ("pending", str(self._outcomes.pending_horizon_count())),
                ("outcomes_recovered", str(recovered)),
            ),
        )

    def _degraded(self, reason: str, now: datetime) -> None:
        first = reason not in self._degraded_reasons
        self._degraded_reasons.add(reason)
        if first:
            self._audit(
                OperationalEventType.DEGRADED_MODE,
                _utc(now),
                (("reason", reason),),
            )

    def _audit(
        self,
        event_type: OperationalEventType,
        occurred_at: datetime,
        details: tuple[tuple[str, str], ...],
        *,
        symbol: str | None = None,
        rollup_signature: str | None = None,
    ) -> None:
        event = OperationalEvent(event_type, _utc(occurred_at), details, symbol)
        if rollup_signature is None:
            self._audit_journal.append(event)
        else:
            self._audit_journal.append_rollup(event, signature=rollup_signature)

    def _write_health(self) -> None:
        self._health_store.save(self.health_snapshot())


def _last_completed_candle(series: MarketSeries, detected_at: datetime) -> Candle:
    duration = interval_duration(series.interval)
    selected = tuple(
        item for item in series.candles if item.timestamp + duration <= detected_at
    )
    if not selected:
        raise ValueError("Market series has no completed candle at detection time.")
    return selected[-1]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Runtime time must be timezone-aware.")
    return value.astimezone(UTC)


def _provider_codes(error: Exception) -> tuple[str, str]:
    status_value = getattr(error, "status_code", None)
    status = str(status_value) if isinstance(status_value, int) else "unknown"
    match = re.search(r"(?:retcode|error code)\s*[:=]?\s*(-?\d+)", str(error), re.I)
    ret_code = match.group(1) if match is not None else "unknown"
    if status == "unknown":
        http_match = re.search(r"\b(?:http\s*)?(4\d\d|5\d\d)\b", str(error), re.I)
        if http_match is not None:
            status = http_match.group(1)
    return status, ret_code


def _safe_error_signature(error: Exception) -> str:
    message = " ".join(str(error).split()).lower()
    return hashlib.sha256(message.encode("utf-8")).hexdigest()[:16]


def _rotating_batch(
    values: list[tuple[UniverseEntry, str, tuple[datetime, ...]]],
    *,
    offset: int,
    limit: int,
) -> list[tuple[UniverseEntry, str, tuple[datetime, ...]]]:
    if len(values) <= limit:
        return values
    start = offset % len(values)
    ordered = values[start:] + values[:start]
    return ordered[:limit]
