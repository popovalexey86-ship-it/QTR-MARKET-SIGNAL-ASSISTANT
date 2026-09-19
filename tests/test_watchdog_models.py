from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.watchdog.detectors import (
    AnomalyDetector,
    DetectorInput,
    DetectorPipeline,
)
from market_signal_assistant.watchdog.models import (
    AnomalyObservation,
    AnomalyType,
    FeatureObservation,
    WatchdogCandidate,
    WatchdogEvent,
    WatchdogState,
)

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def feature(*, available_at: datetime = NOW) -> FeatureObservation:
    return FeatureObservation(
        name="relative_volume_5m",
        value=3.2,
        baseline_value=1.0,
        normalized_value=2.2,
        observed_at=available_at - timedelta(minutes=5),
        available_at=available_at,
        unit="ratio",
    )


def anomaly(*, available_at: datetime = NOW) -> AnomalyObservation:
    return AnomalyObservation(
        anomaly_type=AnomalyType.VOLUME_SHOCK,
        severity=72.0,
        window="5m",
        detected_at=NOW,
        available_at=available_at,
        features=(feature(available_at=available_at),),
        reasons=("relative volume is unusual",),
    )


def test_event_and_candidate_preserve_non_directional_anomaly_contract() -> None:
    event = WatchdogEvent(
        event_id="event-1",
        symbol="abcusdt",
        event_time=NOW - timedelta(minutes=5),
        available_at=NOW,
        detected_at=NOW,
        previous_state=WatchdogState.NORMAL,
        state=WatchdogState.WATCH,
        anomaly_score=72.0,
        anomalies=(anomaly(),),
        features=(feature(),),
        reasons=("volume shock",),
        missing_data=("open_interest",),
    )

    candidate = WatchdogCandidate.from_event(event)

    assert event.symbol == "ABCUSDT"
    assert candidate.anomaly_score == 72.0
    assert candidate.anomaly_types == (AnomalyType.VOLUME_SHOCK,)
    assert not hasattr(candidate, "direction")
    assert not hasattr(candidate, "profit_probability")


def test_event_rejects_feature_available_after_detection() -> None:
    future = NOW + timedelta(seconds=1)
    with pytest.raises(ValueError, match="detection time"):
        anomaly(available_at=future)


def test_detector_pipeline_is_pure_and_explainable() -> None:
    class Detector:
        name = "volume"

        def detect(self, data: DetectorInput) -> AnomalyObservation | None:
            assert data.symbol == "ABCUSDT"
            return anomaly()

    detector: AnomalyDetector = Detector()
    result = DetectorPipeline((detector,)).evaluate(
        DetectorInput("abcusdt", NOW, (feature(),), ())
    )

    assert result.observations[0].reasons == ("relative volume is unusual",)
