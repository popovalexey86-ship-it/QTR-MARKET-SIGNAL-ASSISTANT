from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from market_signal_assistant.derivatives.models import DerivativesSnapshot
from market_signal_assistant.models import MarketSeries
from market_signal_assistant.watchdog.aggregation import (
    AggregationResult,
    ExplainableAnomalyAggregator,
)
from market_signal_assistant.watchdog.detectors import (
    DetectorInput,
    DetectorPipeline,
)
from market_signal_assistant.watchdog.features import (
    WatchdogFeatureBuilder,
    WatchdogFeatureSnapshot,
)
from market_signal_assistant.watchdog.models import (
    AnomalyObservation,
    WatchdogCandidate,
    WatchdogEvent,
    watchdog_event_id,
)
from market_signal_assistant.watchdog.state_machine import (
    StateEvaluation,
    StateTransition,
    WatchdogStateMachine,
    WatchdogSymbolState,
)
from market_signal_assistant.watchdog.state_store import (
    WatchdogRuntimeState,
    WatchdogStateRepository,
)


@dataclass(frozen=True, slots=True)
class WatchdogDetectionResult:
    snapshot: WatchdogFeatureSnapshot
    aggregation: AggregationResult
    state: WatchdogSymbolState
    transition: StateTransition | None
    event: WatchdogEvent | None
    candidate: WatchdogCandidate | None


class WatchdogDetectionEngine:
    """Offline-capable PIT pipeline; all external observations are injected."""

    def __init__(
        self,
        feature_builder: WatchdogFeatureBuilder,
        detectors: DetectorPipeline,
        aggregator: ExplainableAnomalyAggregator,
        state_machine: WatchdogStateMachine,
        states: WatchdogStateRepository,
    ) -> None:
        self._feature_builder = feature_builder
        self._detectors = detectors
        self._aggregator = aggregator
        self._state_machine = state_machine
        self._states = states

    def evaluate(
        self,
        series: MarketSeries,
        *,
        detected_at: datetime,
        derivatives: DerivativesSnapshot | None = None,
    ) -> WatchdogDetectionResult:
        snapshot = self._feature_builder.build(
            series,
            detected_at=detected_at,
            derivatives=derivatives,
        )
        runtime = self._states.get(snapshot.symbol, detected_at=snapshot.detected_at)
        detector_result = self._detectors.evaluate(
            DetectorInput(
                symbol=snapshot.symbol,
                detected_at=snapshot.detected_at,
                features=snapshot.features,
                baselines=snapshot.baselines,
                missing_data=(
                    *snapshot.missing_data,
                    *(f"stale:{item}" for item in snapshot.stale_data),
                ),
                interval=snapshot.interval,
            )
        )
        aggregation = self._aggregator.aggregate(
            detector_result.observations,
            detected_at=snapshot.detected_at,
            sequence_context=runtime.sequence_context,
            missing_data=detector_result.missing_data,
        )
        state_result = self._state_machine.evaluate(
            runtime.symbol_state,
            StateEvaluation(
                anomaly_score=aggregation.anomaly_score,
                available_at=snapshot.available_at,
                detected_at=snapshot.detected_at,
                reasons=aggregation.reasons,
            ),
        )
        event = self._event(
            snapshot,
            detector_result.observations,
            aggregation,
            runtime.symbol_state,
            state_result.state,
        )
        candidate = WatchdogCandidate.from_event(event) if event is not None else None
        # Validate and persist PIT history before advancing the state machine.
        # A rejected observation must never leave partially advanced symbol state.
        self._feature_builder.commit(snapshot)
        self._states.save(
            WatchdogRuntimeState(
                state_result.state,
                aggregation.sequence_context,
            )
        )
        return WatchdogDetectionResult(
            snapshot=snapshot,
            aggregation=aggregation,
            state=state_result.state,
            transition=state_result.transition,
            event=event,
            candidate=candidate,
        )

    @staticmethod
    def _event(
        snapshot: WatchdogFeatureSnapshot,
        observations: tuple[AnomalyObservation, ...],
        aggregation: AggregationResult,
        previous: WatchdogSymbolState,
        current: WatchdogSymbolState,
    ) -> WatchdogEvent | None:
        if not observations:
            return None
        return WatchdogEvent(
            event_id=watchdog_event_id(snapshot.symbol, snapshot.detected_at),
            symbol=snapshot.symbol,
            event_time=snapshot.observed_at,
            available_at=snapshot.available_at,
            detected_at=snapshot.detected_at,
            previous_state=previous.state,
            state=current.state,
            anomaly_score=aggregation.anomaly_score,
            anomalies=observations,
            features=snapshot.features,
            reasons=aggregation.reasons,
            missing_data=aggregation.missing_data,
            contributors=aggregation.contributors,
            config_version="phase2-engineering-defaults-v1",
        )
