from __future__ import annotations

import time
import traceback
from collections import deque
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
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
        self._started_at: datetime | None = None
        self._last_loop_at: datetime | None = None
        self._last_market_update: datetime | None = None
        self._last_universe_refresh: datetime | None = None
        self._universe: UniverseSnapshot | None = None
        self._last_checks: dict[str, datetime] = {}
        self._symbols_processed = 0
        self._symbols_failed = 0
        self._provider_errors = 0
        self._rate_limit_events = 0
        self._api_calls = 0
        self._api_call_times: deque[float] = deque()
        self._peak_api_calls_per_minute = 0
        self._minimum_api_spacing: float | None = None
        self._last_api_call_at: float | None = None
        self._throttle_waits = 0
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
            state = self._states.get(entry.instrument.symbol, detected_at=loop_time)
            tier = interest_tier(entry.tier, state.symbol_state.state)
            rule = self._polling.rule(tier)
            last_check = self._last_checks.get(entry.instrument.symbol)
            if last_check is not None and loop_time - last_check < rule.check_every:
                continue
            plan = self._scheduler.plan(
                now=loop_time,
                interval=rule.interval,
                last_completed=self._cursors.get(
                    entry.instrument.symbol, rule.interval
                ),
            )
            boundaries = plan.buckets
            if plan.omitted_bucket_count and self._gaps is not None:
                duration = interval_duration(rule.interval)
                previous = self._cursors.get(entry.instrument.symbol, rule.interval)
                if previous is not None:
                    first = previous + duration
                    missing_count = plan.omitted_bucket_count + max(
                        0, len(boundaries) - 1
                    )
                    missing_through = first + duration * (missing_count - 1)
                    self._gaps.append(
                        SchedulingGap(
                            entry.instrument.symbol,
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
                        entry.instrument.symbol,
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
        for entry, _interval, _boundaries in due:
            self._last_checks[entry.instrument.symbol] = loop_time

        results: list[SymbolRunResult] = []
        executor = ThreadPoolExecutor(
            max_workers=self._config.maximum_workers,
            thread_name_prefix="watchdog-symbol",
        )
        try:
            futures: dict[Future[SymbolRunResult], str] = {
                executor.submit(
                    self._process_symbol, entry, interval, boundaries
                ): entry.instrument.symbol
                for entry, interval, boundaries in due
            }
            done, pending = wait(
                futures,
                timeout=self._config.provider_timeout * self._config.retry_attempts,
            )
            for future in done:
                try:
                    results.append(future.result())
                except Exception as error:
                    results.append(
                        SymbolRunResult(
                            futures[future],
                            0,
                            0,
                            0,
                            0,
                            0,
                            0.0,
                            True,
                            type(error).__name__,
                        )
                    )
            for future in pending:
                future.cancel()
                results.append(
                    SymbolRunResult(
                        futures[future],
                        0,
                        0,
                        0,
                        0,
                        0,
                        self._config.provider_timeout,
                        True,
                        "TimeoutError",
                    )
                )
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        self._finish_loop(loop_time, loop_started, tuple(results))
        snapshot = self.health_snapshot()
        if acquired_here and self._instance_lock is not None:
            self._instance_lock.release()
        return snapshot

    def health_snapshot(self) -> RuntimeHealthSnapshot:
        now = self._last_loop_at or self._clock()
        today = _utc(now).date()
        events_today = sum(
            1 for item in self._events.records() if item.detected_at.date() == today
        )
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
            pending_outcomes=len(self._outcomes.pending_horizons()),
            provider_errors=self._provider_errors,
            rate_limit_events=self._rate_limit_events,
            loop_duration_seconds=self._loop_duration,
            max_symbol_latency_seconds=self._max_symbol_latency,
            stale_data_count=self._stale_data,
            missing_data_count=self._missing_data,
            api_calls=self._api_calls,
            scheduling_gaps=(len(self._gaps.records()) if self._gaps else 0),
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

    def _provider_call(
        self,
        operation: str,
        symbol: str | None,
        callback: Callable[[], T],
    ) -> T:
        def on_error(error: Exception, attempt: int) -> None:
            with self._metrics_lock:
                self._provider_errors += 1
                if is_rate_limit_error(error):
                    self._rate_limit_events += 1
            self._audit(
                OperationalEventType.RATE_LIMIT
                if is_rate_limit_error(error)
                else OperationalEventType.PROVIDER_FAILURE,
                self._clock(),
                (
                    ("operation", operation),
                    ("attempt", str(attempt)),
                    ("error", type(error).__name__),
                ),
                symbol=symbol,
            )

        return retry_call(
            callback,
            policy=self._retry,
            sleep=self._sleep,
            random_value=self._random_value or __import__("random").random,
            on_error=on_error,
        )

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
                ("events", str(len(self._events.records()))),
                ("pending", str(len(self._outcomes.pending_horizons()))),
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
    ) -> None:
        self._audit_journal.append(
            OperationalEvent(event_type, _utc(occurred_at), details, symbol)
        )

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
