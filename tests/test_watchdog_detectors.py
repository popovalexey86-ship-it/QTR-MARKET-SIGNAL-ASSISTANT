from datetime import UTC, datetime

import pytest

from market_signal_assistant.watchdog.baselines import BaselineSnapshot
from market_signal_assistant.watchdog.detectors import (
    CompressionDetector,
    DetectorInput,
    FundingAnomalyDetector,
    OpenInterestAnomalyDetector,
    PriceAccelerationDetector,
    RangeBuildupDetector,
    VolatilityExpansionDetector,
    VolumeAccelerationDetector,
    VolumeShockDetector,
)
from market_signal_assistant.watchdog.models import AnomalyType, FeatureObservation

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def feature(
    name: str, value: float, *, normalized: float | None = None
) -> FeatureObservation:
    return FeatureObservation(
        name=name,
        value=value,
        observed_at=NOW,
        available_at=NOW,
        normalized_value=normalized,
    )


def baseline(name: str, mean: float = 1.0) -> BaselineSnapshot:
    return BaselineSnapshot("ABCUSDT", name, NOW, 30, 20, False, mean, 0.1, 0.0, 2.0)


def detector_input(
    features: tuple[FeatureObservation, ...],
    baselines: tuple[BaselineSnapshot, ...] = (),
    *,
    missing: tuple[str, ...] = (),
) -> DetectorInput:
    return DetectorInput("ABCUSDT", NOW, features, baselines, missing, "5m")


@pytest.mark.parametrize(
    ("detector", "features", "baselines", "expected"),
    [
        (
            CompressionDetector(),
            (feature("normalized_range_5", 0.50),),
            (baseline("normalized_range_5"),),
            AnomalyType.COMPRESSION,
        ),
        (
            RangeBuildupDetector(),
            (
                feature("range_width_10", 0.50),
                feature("range_touch_density_10", 0.60),
            ),
            (baseline("range_width_10"),),
            AnomalyType.RANGE_BUILDUP,
        ),
        (
            VolumeShockDetector(),
            (feature("relative_volume_20", 2.0),),
            (baseline("relative_volume_20"),),
            AnomalyType.VOLUME_SHOCK,
        ),
        (
            VolumeAccelerationDetector(),
            (feature("volume_acceleration_3", 2.0),),
            (baseline("volume_acceleration_3"),),
            AnomalyType.VOLUME_ACCELERATION,
        ),
        (
            PriceAccelerationDetector(),
            (
                feature("absolute_price_acceleration", 0.03),
                feature("price_acceleration", -0.03),
                feature("price_change_1", -0.02),
            ),
            (baseline("absolute_price_acceleration", 0.01),),
            AnomalyType.PRICE_ACCELERATION,
        ),
        (
            VolatilityExpansionDetector(),
            (feature("realized_volatility_5", 0.03),),
            (baseline("realized_volatility_5", 0.01),),
            AnomalyType.VOLATILITY_EXPANSION,
        ),
        (
            OpenInterestAnomalyDetector(),
            (
                feature("open_interest_change", 0.05, normalized=3.0),
                feature("price_change_1", -0.01),
            ),
            (),
            AnomalyType.OI_SHOCK,
        ),
        (
            FundingAnomalyDetector(),
            (feature("funding_rate", 0.001, normalized=3.0),),
            (),
            AnomalyType.FUNDING_ANOMALY,
        ),
    ],
)
def test_independent_detector_scenarios(
    detector: object,
    features: tuple[FeatureObservation, ...],
    baselines: tuple[BaselineSnapshot, ...],
    expected: AnomalyType,
) -> None:
    observation = detector.detect(detector_input(features, baselines))  # type: ignore[attr-defined]

    assert observation is not None
    assert observation.anomaly_type is expected
    assert 0 < observation.severity <= 100
    assert observation.available_at <= observation.detected_at


def test_oi_modes_are_observations_not_trade_directions() -> None:
    detector = OpenInterestAnomalyDetector()
    regimes = []
    for price, oi in ((0.01, 0.05), (-0.01, 0.05), (0.01, -0.05), (-0.01, -0.05)):
        result = detector.detect(
            detector_input(
                (
                    feature("open_interest_change", oi, normalized=3.0),
                    feature("price_change_1", price),
                )
            )
        )
        assert result is not None
        regimes.append(result.reasons[0])

    assert any("PRICE_UP+OI_UP" in item for item in regimes)
    assert any("PRICE_DOWN+OI_UP" in item for item in regimes)
    assert any("PRICE_UP+OI_DOWN" in item for item in regimes)
    assert any("PRICE_DOWN+OI_DOWN" in item for item in regimes)
    assert not any("LONG" in item or "SHORT" in item for item in regimes)


def test_oi_zero_variance_history_can_use_absolute_historical_ratio() -> None:
    result = OpenInterestAnomalyDetector().detect(
        detector_input(
            (
                feature("open_interest_change", 0.05),
                feature("price_change_1", 0.01),
            ),
            (baseline("open_interest_change", 0.01),),
        )
    )

    assert result is not None
    assert "magnitude=5.000" in result.reasons[0]


def test_missing_or_cold_start_baseline_produces_no_fake_detection() -> None:
    cold = BaselineSnapshot(
        "ABCUSDT", "normalized_range_5", NOW, 4, 20, True, None, None, None, None
    )

    assert CompressionDetector().detect(
        detector_input((feature("normalized_range_5", 0.1),), (cold,))
    ) is None
    assert OpenInterestAnomalyDetector().detect(detector_input(())) is None
