from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from market_signal_assistant.watchdog.models import (
    AnomalyContribution,
    AnomalyObservation,
    AnomalyType,
)


@dataclass(frozen=True, slots=True)
class SequenceStep:
    anomaly_type: AnomalyType
    detected_at: datetime

    def __post_init__(self) -> None:
        if self.detected_at.tzinfo is None or self.detected_at.utcoffset() is None:
            raise ValueError("Sequence timestamp must be timezone-aware.")
        object.__setattr__(self, "detected_at", self.detected_at.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class SequenceContext:
    steps: tuple[SequenceStep, ...] = ()

    def __post_init__(self) -> None:
        if any(
            current.detected_at < previous.detected_at
            for previous, current in zip(self.steps, self.steps[1:], strict=False)
        ):
            raise ValueError("Sequence steps must be chronological.")


@dataclass(frozen=True, slots=True)
class AggregationResult:
    anomaly_score: float
    contributors: tuple[AnomalyContribution, ...]
    sequence_context: SequenceContext
    reasons: tuple[str, ...]
    missing_data: tuple[str, ...]


class ExplainableAnomalyAggregator:
    """Weighted transparent score with explicit correlation attenuation."""

    _WEIGHTS = {
        AnomalyType.COMPRESSION: 18.0,
        AnomalyType.RANGE_BUILDUP: 16.0,
        AnomalyType.VOLUME_SHOCK: 18.0,
        AnomalyType.VOLUME_ACCELERATION: 24.0,
        AnomalyType.PRICE_ACCELERATION: 18.0,
        AnomalyType.VOLATILITY_EXPANSION: 21.0,
        AnomalyType.OI_SHOCK: 15.0,
        AnomalyType.FUNDING_ANOMALY: 8.0,
    }
    _GROUPS = {
        AnomalyType.COMPRESSION: "range_structure",
        AnomalyType.RANGE_BUILDUP: "range_structure",
        AnomalyType.VOLUME_SHOCK: "volume_activity",
        AnomalyType.VOLUME_ACCELERATION: "volume_activity",
        AnomalyType.PRICE_ACCELERATION: "price_volatility",
        AnomalyType.VOLATILITY_EXPANSION: "price_volatility",
        AnomalyType.OI_SHOCK: "open_interest",
        AnomalyType.FUNDING_ANOMALY: "funding",
    }
    _SECONDARY_FACTORS = {
        "range_structure": 0.35,
        "volume_activity": 0.25,
        "price_volatility": 0.50,
        "open_interest": 1.0,
        "funding": 1.0,
    }

    def __init__(
        self,
        *,
        sequence_horizon: timedelta = timedelta(minutes=60),
        maximum_sequence_steps: int = 32,
    ) -> None:
        if sequence_horizon <= timedelta(0) or maximum_sequence_steps <= 0:
            raise ValueError("Sequence retention must be positive.")
        self._sequence_horizon = sequence_horizon
        self._maximum_sequence_steps = maximum_sequence_steps

    def aggregate(
        self,
        observations: tuple[AnomalyObservation, ...],
        *,
        detected_at: datetime,
        sequence_context: SequenceContext | None = None,
        missing_data: tuple[str, ...] = (),
    ) -> AggregationResult:
        decision_time = _utc(detected_at)
        if any(item.detected_at > decision_time for item in observations):
            raise ValueError("Aggregator received a future anomaly.")
        context = sequence_context or SequenceContext()
        retained = tuple(
            item
            for item in context.steps
            if decision_time - self._sequence_horizon
            <= item.detected_at
            <= decision_time
        )
        strongest = _strongest_by_type(observations)
        grouped: dict[str, list[tuple[AnomalyObservation, float]]] = {}
        for item in strongest:
            weight = self._WEIGHTS.get(item.anomaly_type)
            group = self._GROUPS.get(item.anomaly_type)
            if weight is None or group is None:
                continue
            grouped.setdefault(group, []).append((item, weight * item.severity / 100.0))

        contributions: list[AnomalyContribution] = []
        for group, values in grouped.items():
            ordered = sorted(values, key=lambda value: value[1], reverse=True)
            for index, (item, base) in enumerate(ordered):
                factor = 1.0 if index == 0 else self._SECONDARY_FACTORS[group]
                adjusted = base * factor
                contributions.append(
                    AnomalyContribution(
                        anomaly_type=item.anomaly_type,
                        base_points=base,
                        adjusted_points=adjusted,
                        correlation_group=group,
                        reason=(
                            f"severity={item.severity:.3f}; weight="
                            f"{self._WEIGHTS[item.anomaly_type]:.3f}; "
                            f"correlation_factor={factor:.3f}"
                        ),
                    )
                )

        sequence = self._sequence_contribution(
            retained,
            tuple(item.anomaly_type for item in strongest),
            decision_time,
        )
        if sequence is not None:
            contributions.append(sequence)
        score = min(100.0, sum(item.adjusted_points for item in contributions))
        advanced = self._advance(retained, strongest)
        reasons = tuple(
            f"{item.anomaly_type.value} +{item.adjusted_points:.3f}"
            for item in contributions
        ) or ("no_supported_anomaly",)
        return AggregationResult(
            anomaly_score=score,
            contributors=tuple(contributions),
            sequence_context=advanced,
            reasons=reasons,
            missing_data=tuple(dict.fromkeys(missing_data)),
        )

    def _sequence_contribution(
        self,
        previous: tuple[SequenceStep, ...],
        current: tuple[AnomalyType, ...],
        detected_at: datetime,
    ) -> AnomalyContribution | None:
        del detected_at
        compression_times = tuple(
            item.detected_at
            for item in previous
            if item.anomaly_type is AnomalyType.COMPRESSION
        )
        volume_times = tuple(
            item.detected_at
            for item in previous
            if item.anomaly_type
            in {AnomalyType.VOLUME_ACCELERATION, AnomalyType.VOLUME_SHOCK}
        )
        if AnomalyType.VOLATILITY_EXPANSION in current and any(
            compression < volume
            for compression in compression_times
            for volume in volume_times
        ):
            return AnomalyContribution(
                anomaly_type=AnomalyType.MULTI_FACTOR_ANOMALY,
                base_points=12.0,
                adjusted_points=12.0,
                correlation_group="causal_sequence",
                reason="COMPRESSION->VOLUME->VOLATILITY_EXPANSION",
            )
        if AnomalyType.VOLATILITY_EXPANSION in current and any(
            item.anomaly_type is AnomalyType.RANGE_BUILDUP for item in previous
        ):
            return AnomalyContribution(
                anomaly_type=AnomalyType.MULTI_FACTOR_ANOMALY,
                base_points=8.0,
                adjusted_points=8.0,
                correlation_group="causal_sequence",
                reason="RANGE_BUILDUP->VOLATILITY_EXPANSION",
            )
        return None

    def _advance(
        self,
        previous: tuple[SequenceStep, ...],
        observations: tuple[AnomalyObservation, ...],
    ) -> SequenceContext:
        additions = tuple(
            SequenceStep(item.anomaly_type, item.detected_at)
            for item in observations
            if item.anomaly_type
            in {
                AnomalyType.COMPRESSION,
                AnomalyType.RANGE_BUILDUP,
                AnomalyType.VOLUME_SHOCK,
                AnomalyType.VOLUME_ACCELERATION,
                AnomalyType.VOLATILITY_EXPANSION,
            }
        )
        return SequenceContext((*previous, *additions)[-self._maximum_sequence_steps :])


def _strongest_by_type(
    observations: tuple[AnomalyObservation, ...],
) -> tuple[AnomalyObservation, ...]:
    selected: dict[AnomalyType, AnomalyObservation] = {}
    for item in observations:
        existing = selected.get(item.anomaly_type)
        if existing is None or item.severity > existing.severity:
            selected[item.anomaly_type] = item
    return tuple(selected[key] for key in sorted(selected, key=lambda item: item.value))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Aggregation time must be timezone-aware.")
    return value.astimezone(UTC)
