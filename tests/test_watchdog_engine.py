from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

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
    default_detectors,
)
from market_signal_assistant.watchdog.engine import WatchdogDetectionEngine
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.features import WatchdogFeatureBuilder
from market_signal_assistant.watchdog.models import AnomalyType, WatchdogState
from market_signal_assistant.watchdog.state_machine import WatchdogStateMachine
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
    WatchdogStateRepository,
)

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def market(*, last_volume: float) -> MarketSeries:
    candles = []
    for index in range(25):
        timestamp = NOW - timedelta(minutes=125 - index * 5)
        close = 100.0 + index * 0.01
        volume = last_volume if index == 24 else 100.0
        candles.append(
            Candle(timestamp, close, close + 0.1, close - 0.1, close, volume)
        )
    return MarketSeries(
        Instrument("ABCUSDT", AssetClass.CRYPTO), "5m", tuple(candles)
    )


def detection_engine(
    root: Path,
    *,
    seeded: bool,
    all_detectors: bool = False,
) -> WatchdogDetectionEngine:
    baseline = RollingBaselineEngine(
        JsonBaselineStore(root / "baseline.json"),
        minimum_samples=3,
        maximum_samples=30,
    )
    if seeded:
        for index in range(3):
            available_at = NOW - timedelta(minutes=30 - index * 5)
            baseline.observe(
                BaselineObservation(
                    "ABCUSDT",
                    "relative_volume_20",
                    1.0,
                    available_at - timedelta(minutes=5),
                    available_at,
                ),
                detected_at=available_at,
            )
    pipeline = DetectorPipeline(
        default_detectors() if all_detectors else (VolumeShockDetector(),)
    )
    return WatchdogDetectionEngine(
        WatchdogFeatureBuilder(baseline),
        pipeline,
        ExplainableAnomalyAggregator(),
        WatchdogStateMachine(),
        WatchdogStateRepository(JsonWatchdogStateStore(root / "state.json")),
    )


def test_same_replay_is_deterministic(tmp_path: Path) -> None:
    first = detection_engine(tmp_path / "first", seeded=True).evaluate(
        market(last_volume=200.0), detected_at=NOW
    )
    second = detection_engine(tmp_path / "second", seeded=True).evaluate(
        market(last_volume=200.0), detected_at=NOW
    )

    assert first == second
    assert first.event is not None
    assert first.candidate is not None
    assert first.event.anomalies[0].anomaly_type is AnomalyType.VOLUME_SHOCK
    assert first.event.anomaly_score == first.aggregation.anomaly_score
    assert not hasattr(first.candidate, "direction")
    captured = WatchdogEventEvidence.from_detection(
        first,
        price_at_detection=100.24,
        universe_tier="ACTIVE",
    )
    assert captured.event_id == first.event.event_id
    assert captured.features == first.snapshot.features
    assert captured.baseline_sample_counts == first.snapshot.baseline_sample_counts


def test_quiet_market_stays_normal(tmp_path: Path) -> None:
    result = detection_engine(tmp_path, seeded=True).evaluate(
        market(last_volume=100.0), detected_at=NOW
    )

    assert result.event is None
    assert result.candidate is None
    assert result.aggregation.anomaly_score == 0.0
    assert result.state.state is WatchdogState.NORMAL


def test_cold_start_builds_history_without_fake_event(tmp_path: Path) -> None:
    result = detection_engine(
        tmp_path, seeded=False, all_detectors=True
    ).evaluate(market(last_volume=10_000.0), detected_at=NOW)

    assert result.event is None
    assert result.aggregation.anomaly_score == 0.0
    assert result.state.state is WatchdogState.NORMAL
    assert any(item.startswith("baseline:") for item in result.snapshot.missing_data)


def test_rejected_baseline_commit_cannot_partially_advance_symbol_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = detection_engine(tmp_path, seeded=True)

    def reject_commit(_snapshot: object) -> None:
        raise ValueError("Conflicting baseline observation timestamp.")

    monkeypatch.setattr(engine._feature_builder, "commit", reject_commit)

    with pytest.raises(ValueError, match="Conflicting baseline"):
        engine.evaluate(market(last_volume=200.0), detected_at=NOW)

    assert not (tmp_path / "state.json").exists()
