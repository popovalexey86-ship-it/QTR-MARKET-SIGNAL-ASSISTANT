from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from market_signal_assistant.watchdog.baselines import BaselineSnapshot
from market_signal_assistant.watchdog.models import (
    AnomalyObservation,
    AnomalyType,
    FeatureObservation,
)


@dataclass(frozen=True, slots=True)
class DetectorInput:
    symbol: str
    detected_at: datetime
    features: tuple[FeatureObservation, ...]
    baselines: tuple[BaselineSnapshot, ...]
    missing_data: tuple[str, ...] = ()
    interval: str = "unknown"

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.interval.strip():
            raise ValueError("Detector identity cannot be empty.")
        if self.detected_at.tzinfo is None or self.detected_at.utcoffset() is None:
            raise ValueError("Detector time must be timezone-aware.")
        detected_at = self.detected_at.astimezone(UTC)
        if any(item.available_at > detected_at for item in self.features):
            raise ValueError("Detector input contains a future feature.")
        if any(item.as_of > detected_at for item in self.baselines):
            raise ValueError("Detector input contains a future baseline.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "detected_at", detected_at)


class AnomalyDetector(Protocol):
    """Pure explainable detector contract; implementations must not do I/O."""

    @property
    def name(self) -> str: ...

    def detect(self, data: DetectorInput) -> AnomalyObservation | None: ...


@dataclass(frozen=True, slots=True)
class DetectorResult:
    observations: tuple[AnomalyObservation, ...]
    missing_data: tuple[str, ...]


class DetectorPipeline:
    def __init__(self, detectors: tuple[AnomalyDetector, ...]) -> None:
        names = tuple(item.name.strip() for item in detectors)
        if any(not name for name in names) or len(set(names)) != len(names):
            raise ValueError("Detector names must be unique and non-empty.")
        self._detectors = detectors

    def evaluate(self, data: DetectorInput) -> DetectorResult:
        observations = tuple(
            observation
            for detector in self._detectors
            if (observation := detector.detect(data)) is not None
        )
        missing = tuple(
            dict.fromkeys(
                (
                    *data.missing_data,
                    *(x for item in observations for x in item.missing_data),
                )
            )
        )
        return DetectorResult(observations, missing)


@dataclass(frozen=True, slots=True)
class DetectorThresholds:
    compression_ratio: float = 0.75
    range_width_ratio: float = 0.80
    range_touch_density: float = 0.40
    relative_volume_ratio: float = 1.50
    volume_acceleration_ratio: float = 1.40
    price_acceleration_ratio: float = 2.00
    volatility_expansion_ratio: float = 1.50
    oi_z_score: float = 2.00
    oi_absolute_change: float = 0.02
    funding_z_score: float = 2.50

    def __post_init__(self) -> None:
        values = tuple(getattr(self, name) for name in self.__dataclass_fields__)
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError("Detector thresholds must be finite and positive.")


class CompressionDetector:
    name = "compression"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        feature = _feature(data, "normalized_range_5")
        ratio = _ratio_to_baseline(data, feature)
        if (
            feature is None
            or ratio is None
            or ratio > self._thresholds.compression_ratio
        ):
            return None
        severity = _inverse_severity(
            ratio,
            threshold=self._thresholds.compression_ratio,
            floor=0.25,
        )
        return _observation(
            data,
            AnomalyType.COMPRESSION,
            severity,
            (feature,),
            f"normalized range is {ratio:.3f}x its own rolling baseline",
        )


class RangeBuildupDetector:
    name = "range_buildup"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        width = _feature(data, "range_width_10")
        touches = _feature(data, "range_touch_density_10")
        ratio = _ratio_to_baseline(data, width)
        if (
            width is None
            or touches is None
            or ratio is None
            or ratio > self._thresholds.range_width_ratio
            or touches.value < self._thresholds.range_touch_density
        ):
            return None
        width_score = _inverse_severity(
            ratio,
            threshold=self._thresholds.range_width_ratio,
            floor=0.30,
        )
        touch_score = _severity(
            touches.value,
            threshold=self._thresholds.range_touch_density,
            ceiling=1.0,
        )
        return _observation(
            data,
            AnomalyType.RANGE_BUILDUP,
            (width_score + touch_score) / 2.0,
            (width, touches),
            f"range width is {ratio:.3f}x baseline with "
            f"touch density {touches.value:.3f}",
        )


class VolumeShockDetector:
    name = "volume_shock"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        feature = _feature(data, "relative_volume_20")
        ratio = _ratio_to_baseline(data, feature)
        if (
            feature is None
            or ratio is None
            or ratio < self._thresholds.relative_volume_ratio
        ):
            return None
        return _observation(
            data,
            AnomalyType.VOLUME_SHOCK,
            _severity(
                ratio,
                threshold=self._thresholds.relative_volume_ratio,
                ceiling=4.0,
            ),
            (feature,),
            f"relative volume is {ratio:.3f}x its own rolling baseline",
        )


class VolumeAccelerationDetector:
    name = "volume_acceleration"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        feature = _feature(data, "volume_acceleration_3")
        ratio = _ratio_to_baseline(data, feature)
        if (
            feature is None
            or ratio is None
            or ratio < self._thresholds.volume_acceleration_ratio
        ):
            return None
        return _observation(
            data,
            AnomalyType.VOLUME_ACCELERATION,
            _severity(
                ratio,
                threshold=self._thresholds.volume_acceleration_ratio,
                ceiling=3.5,
            ),
            (feature,),
            f"three-bar volume pace is {ratio:.3f}x its rolling baseline",
        )


class PriceAccelerationDetector:
    name = "price_acceleration"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        absolute = _feature(data, "absolute_price_acceleration")
        signed = _feature(data, "price_acceleration")
        price_change = _feature(data, "price_change_1")
        ratio = _ratio_to_baseline(data, absolute)
        if (
            absolute is None
            or signed is None
            or price_change is None
            or ratio is None
            or ratio < self._thresholds.price_acceleration_ratio
        ):
            return None
        observed_sign = "positive" if price_change.value >= 0 else "negative"
        return _observation(
            data,
            AnomalyType.PRICE_ACCELERATION,
            _severity(
                ratio,
                threshold=self._thresholds.price_acceleration_ratio,
                ceiling=5.0,
            ),
            (absolute, signed, price_change),
            f"absolute price acceleration is {ratio:.3f}x baseline; "
            f"observed price change is {observed_sign}",
        )


class VolatilityExpansionDetector:
    name = "volatility_expansion"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        feature = _feature(data, "realized_volatility_5")
        ratio = _ratio_to_baseline(data, feature)
        if (
            feature is None
            or ratio is None
            or ratio < self._thresholds.volatility_expansion_ratio
        ):
            return None
        return _observation(
            data,
            AnomalyType.VOLATILITY_EXPANSION,
            _severity(
                ratio,
                threshold=self._thresholds.volatility_expansion_ratio,
                ceiling=4.0,
            ),
            (feature,),
            f"realized volatility is {ratio:.3f}x its rolling baseline",
        )


class OpenInterestAnomalyDetector:
    name = "open_interest_anomaly"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        oi = _feature(data, "open_interest_change")
        price = _feature(data, "price_change_1")
        if oi is None or price is None:
            return None
        z_score = abs(oi.normalized_value) if oi.normalized_value is not None else None
        historical_ratio = _absolute_ratio_to_baseline(data, oi)
        magnitude = z_score if z_score is not None else historical_ratio
        threshold = self._thresholds.oi_z_score if z_score is not None else 3.0
        if (
            magnitude is None
            or magnitude < threshold
            or abs(oi.value) < self._thresholds.oi_absolute_change
        ):
            return None
        price_regime = "PRICE_UP" if price.value >= 0 else "PRICE_DOWN"
        oi_regime = "OI_UP" if oi.value >= 0 else "OI_DOWN"
        return _observation(
            data,
            AnomalyType.OI_SHOCK,
            _severity(
                magnitude,
                threshold=threshold,
                ceiling=5.0,
            ),
            (oi, price),
            f"observed regime {price_regime}+{oi_regime}; "
            f"OI anomaly magnitude={magnitude:.3f}",
        )


class FundingAnomalyDetector:
    name = "funding_anomaly"

    def __init__(self, thresholds: DetectorThresholds | None = None) -> None:
        self._thresholds = thresholds or DetectorThresholds()

    def detect(self, data: DetectorInput) -> AnomalyObservation | None:
        feature = _feature(data, "funding_rate")
        if feature is None or feature.normalized_value is None:
            return None
        magnitude = abs(feature.normalized_value)
        if magnitude < self._thresholds.funding_z_score:
            return None
        severity = min(
            55.0,
            _severity(
                magnitude,
                threshold=self._thresholds.funding_z_score,
                ceiling=6.0,
            ),
        )
        sign = "positive" if feature.value >= 0 else "negative"
        return _observation(
            data,
            AnomalyType.FUNDING_ANOMALY,
            severity,
            (feature,),
            f"funding is unusually {sign}; z={feature.normalized_value:.3f}",
        )


def default_detectors(
    thresholds: DetectorThresholds | None = None,
) -> tuple[AnomalyDetector, ...]:
    shared = thresholds or DetectorThresholds()
    return (
        CompressionDetector(shared),
        RangeBuildupDetector(shared),
        VolumeShockDetector(shared),
        VolumeAccelerationDetector(shared),
        PriceAccelerationDetector(shared),
        VolatilityExpansionDetector(shared),
        OpenInterestAnomalyDetector(shared),
        FundingAnomalyDetector(shared),
    )


def _feature(data: DetectorInput, name: str) -> FeatureObservation | None:
    return next((item for item in data.features if item.name == name), None)


def _ratio_to_baseline(
    data: DetectorInput,
    feature: FeatureObservation | None,
) -> float | None:
    if feature is None:
        return None
    baseline = next(
        (item for item in data.baselines if item.feature == feature.name), None
    )
    if baseline is None or baseline.cold_start or baseline.mean is None:
        return None
    if baseline.mean <= 1e-12:
        return None
    return feature.value / baseline.mean


def _absolute_ratio_to_baseline(
    data: DetectorInput,
    feature: FeatureObservation,
) -> float | None:
    baseline = next(
        (item for item in data.baselines if item.feature == feature.name), None
    )
    if baseline is None or baseline.cold_start or baseline.mean is None:
        return None
    if abs(baseline.mean) <= 1e-12:
        return None
    return abs(feature.value) / abs(baseline.mean)


def _observation(
    data: DetectorInput,
    anomaly_type: AnomalyType,
    severity: float,
    features: tuple[FeatureObservation, ...],
    reason: str,
) -> AnomalyObservation:
    return AnomalyObservation(
        anomaly_type=anomaly_type,
        severity=max(0.0, min(100.0, severity)),
        window=data.interval,
        detected_at=data.detected_at,
        available_at=max(item.available_at for item in features),
        features=features,
        reasons=(reason,),
    )


def _severity(value: float, *, threshold: float, ceiling: float) -> float:
    if ceiling <= threshold:
        raise ValueError("Severity ceiling must exceed threshold.")
    scaled = (value - threshold) / (ceiling - threshold)
    return 30.0 + 70.0 * max(0.0, min(1.0, scaled))


def _inverse_severity(value: float, *, threshold: float, floor: float) -> float:
    if threshold <= floor:
        raise ValueError("Inverse severity threshold must exceed floor.")
    scaled = (threshold - value) / (threshold - floor)
    return 30.0 + 70.0 * max(0.0, min(1.0, scaled))
