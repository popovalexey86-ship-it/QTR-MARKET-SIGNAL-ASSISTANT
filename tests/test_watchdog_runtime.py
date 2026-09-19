from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.models import AssetClass, Candle, Instrument, MarketSeries
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
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.price_journal import WatchdogPriceJournal
from market_signal_assistant.watchdog.outcomes.scheduler import (
    ForwardOutcomeScheduler,
    JsonOutcomeCheckpointStore,
)
from market_signal_assistant.watchdog.runtime.audit import OperationalAuditJournal
from market_signal_assistant.watchdog.runtime.health import JsonRuntimeHealthStore
from market_signal_assistant.watchdog.runtime.models import ShadowRuntimeConfig
from market_signal_assistant.watchdog.runtime.schedule import JsonBucketCursorStore
from market_signal_assistant.watchdog.runtime.service import WatchdogShadowRuntime
from market_signal_assistant.watchdog.state_machine import WatchdogStateMachine
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
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

    def load(
        self, instrument: Instrument, interval: str, limit: int
    ) -> MarketSeries:
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

    assert after_horizon.pending_outcomes == 3
    assert {item.horizon_minutes for item in outcomes} == {1, 5}
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


def test_symbol_failure_is_isolated_and_runtime_is_degraded(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    provider = FixtureProvider(clock, ("ABCUSDT", "BADUSDT"))
    provider.failures["BADUSDT"] = 3
    sleeps: list[float] = []
    runtime = _runtime(tmp_path, provider, clock, sleep=sleeps.append)

    health = runtime.run_once(now=NOW)

    assert health.symbols_processed == 1
    assert health.symbols_failed == 1
    assert health.provider_errors == 3
    assert health.degraded is True
    assert any(
        reason.startswith("symbol:BADUSDT")
        for reason in health.degraded_reasons
    )
    assert sleeps == [0.5, 1.0]
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
    assert health.events_today == 1
    assert any(
        reason.startswith("derivatives:ABCUSDT")
        for reason in health.degraded_reasons
    )


def _runtime(
    root: Path,
    provider: FixtureProvider,
    clock: MutableClock,
    *,
    sleep: Callable[[float], None] | None = None,
    config: ShadowRuntimeConfig | None = None,
    derivatives_provider: FailingDerivativesProvider | None = None,
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
    outcome_journal = WatchdogOutcomeJournal(
        root / "outcomes" / "outcomes.jsonl"
    )
    outcomes = ForwardOutcomeScheduler(
        events,
        outcome_journal,
        JsonOutcomeCheckpointStore(root / "state" / "pending.json"),
        prices=WatchdogPriceJournal(
            root / "outcomes" / "price_observations.jsonl"
        ),
    )
    kwargs = {}
    if sleep is not None:
        kwargs["sleep"] = sleep
    return WatchdogShadowRuntime(
        universe_provider=provider,
        market_provider=provider,
        derivatives_provider=derivatives_provider,
        universe=DynamicUniverse(),
        baseline_counts=lambda _now: {symbol: 20 for symbol in provider.symbols},
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
        cursors=JsonBucketCursorStore(root / "state" / "buckets.json"),
        audit=OperationalAuditJournal(root / "operational" / "runtime.jsonl"),
        health_store=JsonRuntimeHealthStore(root / "state" / "health.json"),
        config=config
        or ShadowRuntimeConfig(
            universe_refresh=timedelta(minutes=15),
            retry_jitter=0.0,
            maximum_workers=2,
        ),
        clock=clock,
        monotonic=time_counter(),
        random_value=lambda: 0.5,
        **kwargs,
    )


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
    return MarketSeries(
        Instrument(symbol, AssetClass.CRYPTO), interval, tuple(candles)
    )
