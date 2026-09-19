from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.models import AssetClass, Candle, Instrument, MarketSeries
from market_signal_assistant.watchdog.baselines import (
    BaselineObservation,
    JsonBaselineStore,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.detectors import (
    DetectorInput,
    DetectorPipeline,
    VolumeShockDetector,
)
from market_signal_assistant.watchdog.features import WatchdogFeatureBuilder

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def series(*, future_volume: float, completed_volume: float = 200.0) -> MarketSeries:
    candles = []
    for index in range(25):
        timestamp = NOW - timedelta(minutes=125 - index * 5)
        close = 100.0 + index * 0.01
        volume = completed_volume if index == 24 else 100.0
        candles.append(
            Candle(timestamp, close, close + 0.1, close - 0.1, close, volume)
        )
    candles.append(Candle(NOW, 100.0, 110.0, 90.0, 105.0, future_volume))
    return MarketSeries(
        Instrument("ABCUSDT", AssetClass.CRYPTO),
        "5m",
        tuple(candles),
    )


def engine(path: Path) -> RollingBaselineEngine:
    result = RollingBaselineEngine(
        JsonBaselineStore(path), minimum_samples=3, maximum_samples=30
    )
    for index in range(3):
        available_at = NOW - timedelta(minutes=30 - index * 5)
        result.observe(
            BaselineObservation(
                "ABCUSDT",
                "relative_volume_20",
                1.0,
                available_at - timedelta(minutes=5),
                available_at,
            ),
            detected_at=available_at,
        )
    return result


def derivatives(as_of: datetime) -> DerivativesSnapshot:
    return DerivativesSnapshot(
        provider="fixture",
        symbol="ABCUSDT",
        as_of=as_of,
        funding_rate=0.0001,
        open_interest=1_000_000.0,
        open_interest_change=0.03,
        price_change=0.01,
        volume_change=0.20,
    )


def test_unfinished_candle_and_changed_future_do_not_change_result_at_t(
    tmp_path: Path,
) -> None:
    builder = WatchdogFeatureBuilder(engine(tmp_path / "baseline.json"))
    first = builder.build(series(future_volume=100.0), detected_at=NOW)
    second = builder.build(series(future_volume=100_000.0), detected_at=NOW)
    detector = DetectorPipeline((VolumeShockDetector(),))

    first_result = detector.evaluate(
        DetectorInput(
            first.symbol,
            NOW,
            first.features,
            first.baselines,
            first.missing_data,
            first.interval,
        )
    )
    second_result = detector.evaluate(
        DetectorInput(
            second.symbol,
            NOW,
            second.features,
            second.baselines,
            second.missing_data,
            second.interval,
        )
    )

    assert first.completed_candle_count == 25
    assert first.features == second.features
    assert first.baseline_sample_counts == second.baseline_sample_counts
    assert first_result == second_result
    assert first_result.observations


def test_snapshot_exposes_missing_and_baseline_sample_counts(tmp_path: Path) -> None:
    snapshot = WatchdogFeatureBuilder(engine(tmp_path / "baseline.json")).build(
        series(future_volume=100.0), detected_at=NOW
    )
    counts = dict(snapshot.baseline_sample_counts)

    assert counts["relative_volume_20"] == 3
    assert "open_interest" in snapshot.missing_data
    assert "funding" in snapshot.missing_data
    assert snapshot.available_at <= snapshot.detected_at


def test_fresh_derivatives_are_adapted_and_stale_values_are_not_used(
    tmp_path: Path,
) -> None:
    builder = WatchdogFeatureBuilder(engine(tmp_path / "baseline.json"))
    fresh = builder.build(
        series(future_volume=100.0),
        detected_at=NOW,
        derivatives=derivatives(NOW - timedelta(minutes=5)),
    )
    stale = builder.build(
        series(future_volume=100.0),
        detected_at=NOW,
        derivatives=derivatives(NOW - timedelta(minutes=11)),
    )

    assert {item.name for item in fresh.features} >= {
        "open_interest_change",
        "funding_rate",
    }
    assert "open_interest_change" not in {item.name for item in stale.features}
    assert stale.stale_data == ("open_interest", "funding")
    assert "open_interest" in stale.missing_data
    assert "funding" in stale.missing_data


def test_future_derivatives_are_rejected(tmp_path: Path) -> None:
    builder = WatchdogFeatureBuilder(engine(tmp_path / "baseline.json"))

    with pytest.raises(ValueError, match="Future derivatives"):
        builder.build(
            series(future_volume=100.0),
            detected_at=NOW,
            derivatives=derivatives(NOW + timedelta(seconds=1)),
        )
