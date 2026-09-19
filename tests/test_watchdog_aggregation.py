from datetime import UTC, datetime, timedelta

import pytest

from market_signal_assistant.watchdog.aggregation import (
    ExplainableAnomalyAggregator,
    SequenceContext,
    SequenceStep,
)
from market_signal_assistant.watchdog.models import AnomalyObservation, AnomalyType

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def anomaly(kind: AnomalyType, severity: float = 100.0) -> AnomalyObservation:
    return AnomalyObservation(kind, severity, "5m", NOW, NOW, (), (kind.value,))


def test_explainable_formula_matches_full_severity_example() -> None:
    result = ExplainableAnomalyAggregator().aggregate(
        (
            anomaly(AnomalyType.COMPRESSION),
            anomaly(AnomalyType.VOLUME_ACCELERATION),
            anomaly(AnomalyType.VOLATILITY_EXPANSION),
            anomaly(AnomalyType.OI_SHOCK),
        ),
        detected_at=NOW,
        missing_data=("funding",),
    )

    assert result.anomaly_score == pytest.approx(78.0)
    points = {item.anomaly_type: item.adjusted_points for item in result.contributors}
    assert points == {
        AnomalyType.COMPRESSION: 18.0,
        AnomalyType.VOLUME_ACCELERATION: 24.0,
        AnomalyType.VOLATILITY_EXPANSION: 21.0,
        AnomalyType.OI_SHOCK: 15.0,
    }
    assert result.missing_data == ("funding",)


def test_correlated_features_are_attenuated_not_double_counted() -> None:
    result = ExplainableAnomalyAggregator().aggregate(
        (
            anomaly(AnomalyType.COMPRESSION),
            anomaly(AnomalyType.RANGE_BUILDUP),
            anomaly(AnomalyType.VOLUME_SHOCK),
            anomaly(AnomalyType.VOLUME_ACCELERATION),
        ),
        detected_at=NOW,
    )
    values = {item.anomaly_type: item for item in result.contributors}

    assert values[AnomalyType.RANGE_BUILDUP].adjusted_points == pytest.approx(5.6)
    assert values[AnomalyType.VOLUME_SHOCK].adjusted_points == pytest.approx(4.5)
    assert result.anomaly_score == pytest.approx(52.1)


def test_compression_volume_expansion_sequence_is_causal() -> None:
    context = SequenceContext(
        (
            SequenceStep(AnomalyType.COMPRESSION, NOW - timedelta(minutes=10)),
            SequenceStep(
                AnomalyType.VOLUME_ACCELERATION, NOW - timedelta(minutes=5)
            ),
        )
    )
    result = ExplainableAnomalyAggregator().aggregate(
        (anomaly(AnomalyType.VOLATILITY_EXPANSION),),
        detected_at=NOW,
        sequence_context=context,
    )

    sequence = result.contributors[-1]
    assert sequence.anomaly_type is AnomalyType.MULTI_FACTOR_ANOMALY
    assert sequence.adjusted_points == 12.0
    assert sequence.reason == "COMPRESSION->VOLUME->VOLATILITY_EXPANSION"


def test_future_sequence_step_cannot_influence_result_at_t() -> None:
    clean = ExplainableAnomalyAggregator().aggregate(
        (anomaly(AnomalyType.VOLATILITY_EXPANSION),), detected_at=NOW
    )
    future_context = SequenceContext(
        (SequenceStep(AnomalyType.RANGE_BUILDUP, NOW + timedelta(minutes=5)),)
    )
    changed_future = ExplainableAnomalyAggregator().aggregate(
        (anomaly(AnomalyType.VOLATILITY_EXPANSION),),
        detected_at=NOW,
        sequence_context=future_context,
    )

    assert changed_future.anomaly_score == clean.anomaly_score
    assert changed_future.contributors == clean.contributors
