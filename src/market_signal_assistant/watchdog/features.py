from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from statistics import fmean, pstdev

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.models import Candle, MarketSeries
from market_signal_assistant.watchdog.adapters import DerivativesSnapshotAdapter
from market_signal_assistant.watchdog.baselines import (
    BaselineObservation,
    BaselineSnapshot,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.models import FeatureObservation


@dataclass(frozen=True, slots=True)
class WatchdogFeatureSnapshot:
    symbol: str
    interval: str
    observed_at: datetime
    available_at: datetime
    detected_at: datetime
    completed_candle_count: int
    features: tuple[FeatureObservation, ...]
    baselines: tuple[BaselineSnapshot, ...]
    missing_data: tuple[str, ...]
    stale_data: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.interval.strip():
            raise ValueError("Feature snapshot identity is required.")
        observed_at = _utc(self.observed_at)
        available_at = _utc(self.available_at)
        detected_at = _utc(self.detected_at)
        if observed_at > available_at or available_at > detected_at:
            raise ValueError("Feature snapshot violates PIT chronology.")
        if self.completed_candle_count < 0:
            raise ValueError("Completed candle count cannot be negative.")
        if any(item.available_at > detected_at for item in self.features):
            raise ValueError("Feature snapshot contains future data.")
        if any(item.as_of > detected_at for item in self.baselines):
            raise ValueError("Feature snapshot contains a future baseline.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "detected_at", detected_at)

    @property
    def baseline_sample_counts(self) -> tuple[tuple[str, int], ...]:
        return tuple((item.feature, item.sample_count) for item in self.baselines)


class WatchdogFeatureBuilder:
    """Build PIT features from completed candles and optional derivatives."""

    def __init__(
        self,
        baselines: RollingBaselineEngine,
        *,
        derivatives_adapter: DerivativesSnapshotAdapter | None = None,
    ) -> None:
        self._baselines = baselines
        self._derivatives_adapter = derivatives_adapter or DerivativesSnapshotAdapter()

    def build(
        self,
        series: MarketSeries,
        *,
        detected_at: datetime,
        derivatives: DerivativesSnapshot | None = None,
    ) -> WatchdogFeatureSnapshot:
        decision_time = _utc(detected_at)
        interval = _interval_duration(series.interval)
        completed = tuple(
            candle
            for candle in series.candles
            if candle.timestamp + interval <= decision_time
        )
        if not completed:
            return WatchdogFeatureSnapshot(
                symbol=series.instrument.symbol,
                interval=series.interval,
                observed_at=decision_time,
                available_at=decision_time,
                detected_at=decision_time,
                completed_candle_count=0,
                features=(),
                baselines=(),
                missing_data=("ohlcv:completed_candles",),
                stale_data=(),
            )

        available_at = completed[-1].timestamp + interval
        raw, missing = _ohlcv_features(completed)
        derivative_result = self._derivatives_adapter.adapt(
            derivatives,
            symbol=series.instrument.symbol,
            detected_at=decision_time,
        )
        missing.extend(derivative_result.missing_data)

        observations: list[FeatureObservation] = []
        baseline_snapshots: list[BaselineSnapshot] = []
        raw_features = tuple(
            FeatureObservation(
                name=name,
                value=value,
                observed_at=completed[-1].timestamp,
                available_at=available_at,
                unit=unit,
            )
            for name, value, unit in raw
        )
        for raw_feature in (*raw_features, *derivative_result.features):
            name = raw_feature.name
            value = raw_feature.value
            baseline = self._baselines.snapshot(
                series.instrument.symbol,
                name,
                detected_at=decision_time,
                scope=series.interval,
            )
            baseline_snapshots.append(baseline)
            normalized = _z_score(value, baseline)
            observations.append(
                FeatureObservation(
                    name=name,
                    value=value,
                    observed_at=raw_feature.observed_at,
                    available_at=raw_feature.available_at,
                    unit=raw_feature.unit,
                    baseline_value=baseline.mean,
                    normalized_value=normalized,
                )
            )
            if baseline.cold_start:
                missing.append(f"baseline:{name}")

        snapshot_available_at = max(
            (item.available_at for item in observations), default=available_at
        )
        return WatchdogFeatureSnapshot(
            symbol=series.instrument.symbol,
            interval=series.interval,
            observed_at=completed[-1].timestamp,
            available_at=snapshot_available_at,
            detected_at=decision_time,
            completed_candle_count=len(completed),
            features=tuple(observations),
            baselines=tuple(baseline_snapshots),
            missing_data=tuple(dict.fromkeys(missing)),
            stale_data=tuple(dict.fromkeys(derivative_result.stale_data)),
        )

    def commit(self, snapshot: WatchdogFeatureSnapshot) -> None:
        """Add the evaluated snapshot only after the decision was made."""
        observations = tuple(
            BaselineObservation(
                symbol=snapshot.symbol,
                feature=item.name,
                value=item.value,
                observed_at=item.observed_at,
                available_at=item.available_at,
                scope=snapshot.interval,
            )
            for item in snapshot.features
        )
        if observations:
            self._baselines.observe_many(
                observations,
                detected_at=snapshot.detected_at,
            )


def _ohlcv_features(
    candles: tuple[Candle, ...],
) -> tuple[tuple[tuple[str, float, str], ...], list[str]]:
    values: list[tuple[str, float, str]] = []
    missing: list[str] = []
    if len(candles) >= 5:
        ranges = tuple((item.high - item.low) / item.close for item in candles[-5:])
        values.append(("normalized_range_5", fmean(ranges), "ratio"))
    else:
        missing.append("ohlcv:normalized_range_5")

    if len(candles) >= 10:
        recent = candles[-10:]
        upper = max(item.high for item in recent)
        lower = min(item.low for item in recent)
        width = upper - lower
        values.append(("range_width_10", width / recent[-1].close, "ratio"))
        if width > 0:
            tolerance = width * 0.10
            touches = sum(
                1
                for item in recent
                if item.high >= upper - tolerance or item.low <= lower + tolerance
            )
            values.append(("range_touch_density_10", touches / len(recent), "ratio"))
    else:
        missing.extend(("ohlcv:range_width_10", "ohlcv:range_touch_density_10"))

    if len(candles) >= 21:
        previous_volume = fmean(item.volume for item in candles[-21:-1])
        if previous_volume > 0:
            values.append(
                ("relative_volume_20", candles[-1].volume / previous_volume, "ratio")
            )
        else:
            missing.append("ohlcv:relative_volume_20")
    else:
        missing.append("ohlcv:relative_volume_20")

    if len(candles) >= 13:
        previous = fmean(item.volume for item in candles[-13:-3])
        current = fmean(item.volume for item in candles[-3:])
        if previous > 0:
            values.append(("volume_acceleration_3", current / previous, "ratio"))
        else:
            missing.append("ohlcv:volume_acceleration_3")
    else:
        missing.append("ohlcv:volume_acceleration_3")

    returns = tuple(
        current.close / previous.close - 1.0
        for previous, current in zip(candles, candles[1:], strict=False)
    )
    if returns:
        values.append(("price_change_1", returns[-1], "ratio"))
    else:
        missing.append("ohlcv:price_change_1")
    if len(returns) >= 2:
        acceleration = returns[-1] - returns[-2]
        values.extend(
            (
                ("price_acceleration", acceleration, "ratio"),
                ("absolute_price_acceleration", abs(acceleration), "ratio"),
            )
        )
    else:
        missing.append("ohlcv:price_acceleration")
    if len(returns) >= 5:
        values.append(("realized_volatility_5", pstdev(returns[-5:]), "ratio"))
    else:
        missing.append("ohlcv:realized_volatility_5")
    return tuple(values), missing


def _z_score(value: float, baseline: BaselineSnapshot) -> float | None:
    deviation = baseline.standard_deviation
    if baseline.cold_start or baseline.mean is None or deviation is None:
        return None
    if deviation <= 1e-12:
        return 0.0 if math.isclose(value, baseline.mean) else None
    return (value - baseline.mean) / deviation


def _interval_duration(value: str) -> timedelta:
    normalized = value.strip().lower()
    if normalized.isdigit():
        return timedelta(minutes=int(normalized))
    suffixes = {
        "m": timedelta(minutes=1),
        "h": timedelta(hours=1),
        "d": timedelta(days=1),
    }
    suffix = normalized[-1:] if normalized else ""
    amount = normalized[:-1]
    if suffix not in suffixes or not amount.isdigit() or int(amount) <= 0:
        raise ValueError(f"Unsupported market interval: {value!r}.")
    return suffixes[suffix] * int(amount)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Watchdog time must be timezone-aware.")
    return value.astimezone(UTC)
