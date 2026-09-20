from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.watchdog.baselines import (
    BaselineObservation,
    JsonBaselineStore,
    RollingBaselineEngine,
)

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def observation(index: int) -> BaselineObservation:
    available_at = NOW + timedelta(minutes=index)
    return BaselineObservation(
        symbol="ABCUSDT",
        feature="volume",
        value=float(index + 1),
        observed_at=available_at - timedelta(minutes=1),
        available_at=available_at,
    )


def test_cold_start_does_not_invent_baseline(tmp_path: Path) -> None:
    engine = RollingBaselineEngine(
        JsonBaselineStore(tmp_path / "baseline.json"),
        minimum_samples=3,
    )
    engine.observe(observation(0), detected_at=NOW)
    engine.observe(observation(1), detected_at=NOW + timedelta(minutes=1))

    snapshot = engine.snapshot(
        "ABCUSDT",
        "volume",
        detected_at=NOW + timedelta(minutes=1),
    )

    assert snapshot.cold_start is True
    assert snapshot.sample_count == 2
    assert snapshot.mean is None
    assert snapshot.standard_deviation is None


def test_pit_query_excludes_later_available_observations(tmp_path: Path) -> None:
    engine = RollingBaselineEngine(
        JsonBaselineStore(tmp_path / "baseline.json"),
        minimum_samples=2,
    )
    for index in range(3):
        engine.observe(
            observation(index),
            detected_at=NOW + timedelta(minutes=index),
        )

    earlier = engine.snapshot(
        "ABCUSDT",
        "volume",
        detected_at=NOW + timedelta(minutes=1),
    )

    assert earlier.sample_count == 2
    assert earlier.mean == 1.5


def test_restart_recovers_exact_bounded_baseline(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    first = RollingBaselineEngine(
        JsonBaselineStore(path),
        minimum_samples=2,
        maximum_samples=3,
    )
    for index in range(5):
        first.observe(
            observation(index),
            detected_at=NOW + timedelta(minutes=index),
        )

    restarted = RollingBaselineEngine(
        JsonBaselineStore(path),
        minimum_samples=2,
        maximum_samples=3,
    )
    snapshot = restarted.snapshot(
        "ABCUSDT",
        "volume",
        detected_at=NOW + timedelta(minutes=10),
    )

    assert [item.value for item in restarted.observations] == [3.0, 4.0, 5.0]
    assert snapshot.sample_count == 3
    assert snapshot.mean == 4.0


def test_future_observation_cannot_enter_baseline(tmp_path: Path) -> None:
    engine = RollingBaselineEngine(JsonBaselineStore(tmp_path / "baseline.json"))

    with pytest.raises(ValueError, match="Future observation"):
        engine.observe(observation(1), detected_at=NOW)


def test_batch_commit_is_idempotent_for_same_available_observation(
    tmp_path: Path,
) -> None:
    engine = RollingBaselineEngine(
        JsonBaselineStore(tmp_path / "baseline.json"), minimum_samples=2
    )
    item = observation(0)

    engine.observe_many((item,), detected_at=NOW)
    engine.observe_many((item,), detected_at=NOW)

    assert engine.observations == (item,)


def test_interval_scopes_prevent_live_tier_change_timestamp_conflict(
    tmp_path: Path,
) -> None:
    engine = RollingBaselineEngine(JsonBaselineStore(tmp_path / "baseline.json"))
    available_at = NOW + timedelta(minutes=5)
    five_minute = BaselineObservation(
        "ABCUSDT", "volume", 2.0, NOW, available_at, scope="5m"
    )
    one_minute = BaselineObservation(
        "ABCUSDT",
        "volume",
        3.0,
        NOW + timedelta(minutes=4),
        available_at,
        scope="1m",
    )

    engine.observe_many((five_minute,), detected_at=available_at)
    engine.observe_many((one_minute,), detected_at=available_at)

    assert (
        engine.snapshot(
            "ABCUSDT", "volume", detected_at=available_at, scope="5m"
        ).sample_count
        == 1
    )
    assert (
        engine.snapshot("ABCUSDT", "volume", detected_at=available_at, scope="1m").mean
        is None
    )
