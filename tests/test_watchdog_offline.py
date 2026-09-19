import builtins
from pathlib import Path
from typing import Any

import pytest

from market_signal_assistant.providers import BybitPublicProvider
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
from market_signal_assistant.watchdog.outcomes.scheduler import (
    ForwardOutcomeScheduler,
    JsonOutcomeCheckpointStore,
)
from market_signal_assistant.watchdog.runtime.composition import (
    build_bybit_shadow_runtime,
)
from market_signal_assistant.watchdog.state_machine import WatchdogStateMachine
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
    WatchdogStateRepository,
)
from market_signal_assistant.watchdog.universe import DynamicUniverse


def test_watchdog_import_and_construction_have_no_network_io(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith(("pybit", "websockets", "telegram")):
            raise AssertionError(f"network dependency imported: {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    provider = BybitPublicProvider(
        getter=lambda url, timeout: (_ for _ in ()).throw(
            AssertionError(f"network opened: {url} {timeout}")
        )
    )
    baseline = RollingBaselineEngine(JsonBaselineStore(tmp_path / "state.json"))
    engine = WatchdogDetectionEngine(
        WatchdogFeatureBuilder(baseline),
        DetectorPipeline(default_detectors()),
        ExplainableAnomalyAggregator(),
        WatchdogStateMachine(),
        WatchdogStateRepository(
            JsonWatchdogStateStore(tmp_path / "runtime-state.json")
        ),
    )
    evidence = WatchdogEventJournal(tmp_path / "events.jsonl")
    outcomes = WatchdogOutcomeJournal(tmp_path / "outcomes.jsonl")
    scheduler = ForwardOutcomeScheduler(
        evidence,
        outcomes,
        JsonOutcomeCheckpointStore(tmp_path / "pending.json"),
    )
    runtime_bundle = build_bybit_shadow_runtime(tmp_path / "shadow-runtime")

    assert provider is not None
    assert baseline.observations == ()
    assert DynamicUniverse() is not None
    assert DetectorPipeline(()) is not None
    assert WatchdogStateMachine() is not None
    assert engine is not None
    assert scheduler.pending_horizons() == ()
    assert runtime_bundle.runtime.running is False
    assert runtime_bundle.request_budget.total_calls == 0
