from datetime import UTC, datetime, timedelta

from market_signal_assistant.watchdog.aggregation import SequenceContext, SequenceStep
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.models import (
    AnomalyContribution,
    AnomalyType,
    FeatureObservation,
    WatchdogState,
)

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def evidence(
    event_id: str = "watchdog:ABCUSDT:20260918T080000.000000Z",
    *,
    anomaly_types: tuple[AnomalyType, ...] = (AnomalyType.COMPRESSION,),
    score: float = 65.0,
    detected_at: datetime = NOW,
) -> WatchdogEventEvidence:
    feature = FeatureObservation(
        "normalized_range_5",
        0.005,
        detected_at - timedelta(minutes=5),
        detected_at,
        baseline_value=0.01,
        normalized_value=-3.0,
    )
    contribution = AnomalyContribution(
        anomaly_types[0], 11.7, 11.7, "range_structure", "fixture"
    )
    return WatchdogEventEvidence(
        event_id=event_id,
        symbol="ABCUSDT",
        event_time=detected_at - timedelta(minutes=5),
        detected_at=detected_at,
        available_at=detected_at,
        state_before=WatchdogState.NORMAL,
        state_after=WatchdogState.WATCH,
        anomaly_score=score,
        anomaly_types=anomaly_types,
        contributors=(contribution,),
        reasons=("fixture anomaly",),
        features=(feature,),
        baseline_sample_counts=(("normalized_range_5", 30),),
        missing_data=("funding",),
        stale_data=(),
        sequence_context=SequenceContext(
            (SequenceStep(anomaly_types[0], detected_at),)
        ),
        price_at_detection=100.0,
        universe_tier="ACTIVE",
    )
