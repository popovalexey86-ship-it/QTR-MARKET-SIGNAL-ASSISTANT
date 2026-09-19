from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from market_signal_assistant.watchdog.models import WatchdogState


@dataclass(frozen=True, slots=True)
class StateMachineConfig:
    watch_enter: float = 30.0
    watch_exit: float = 20.0
    in_play_enter: float = 55.0
    in_play_exit: float = 45.0
    high_attention_enter: float = 75.0
    high_attention_exit: float = 65.0
    escalation_confirmations: int = 2
    deescalation_confirmations: int = 2
    cooldown: timedelta = timedelta(minutes=30)

    def __post_init__(self) -> None:
        values = (
            self.watch_exit,
            self.watch_enter,
            self.in_play_exit,
            self.in_play_enter,
            self.high_attention_exit,
            self.high_attention_enter,
        )
        if any(not math.isfinite(value) or not 0 <= value <= 100 for value in values):
            raise ValueError("State thresholds must be finite scores.")
        if not (
            self.watch_exit < self.watch_enter
            < self.in_play_enter
            < self.high_attention_enter
        ):
            raise ValueError("Escalation thresholds are inconsistent.")
        if not self.watch_exit < self.in_play_exit < self.in_play_enter:
            raise ValueError("IN PLAY hysteresis is inconsistent.")
        if not self.in_play_exit < self.high_attention_exit < self.high_attention_enter:
            raise ValueError("HIGH ATTENTION hysteresis is inconsistent.")
        if self.escalation_confirmations <= 0 or self.deescalation_confirmations <= 0:
            raise ValueError("State confirmations must be positive.")
        if self.cooldown <= timedelta(0):
            raise ValueError("State cooldown must be positive.")


@dataclass(frozen=True, slots=True)
class StateEvaluation:
    anomaly_score: float
    available_at: datetime
    detected_at: datetime
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not math.isfinite(self.anomaly_score) or not 0 <= self.anomaly_score <= 100:
            raise ValueError("Anomaly score must be between 0 and 100.")
        if not self.reasons:
            raise ValueError("State evaluation requires reasons.")
        available_at = _utc(self.available_at)
        detected_at = _utc(self.detected_at)
        if available_at > detected_at:
            raise ValueError("State evaluation violates PIT chronology.")
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "detected_at", detected_at)


@dataclass(frozen=True, slots=True)
class WatchdogSymbolState:
    symbol: str
    state: WatchdogState
    changed_at: datetime
    last_detected_at: datetime
    last_score: float
    consecutive_escalations: int = 0
    consecutive_deescalations: int = 0
    cooldown_until: datetime | None = None
    previous_state: WatchdogState = WatchdogState.NORMAL
    last_transition_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("State symbol cannot be empty.")
        changed_at = _utc(self.changed_at)
        last_detected_at = _utc(self.last_detected_at)
        if changed_at > last_detected_at:
            raise ValueError("State change cannot follow last detection.")
        if not math.isfinite(self.last_score) or not 0 <= self.last_score <= 100:
            raise ValueError("State score must be between 0 and 100.")
        if self.consecutive_escalations < 0 or self.consecutive_deescalations < 0:
            raise ValueError("State counters cannot be negative.")
        cooldown_until = self.cooldown_until
        if cooldown_until is not None:
            cooldown_until = _utc(cooldown_until)
        if self.state is WatchdogState.COOLDOWN and cooldown_until is None:
            raise ValueError("COOLDOWN state requires an expiration time.")
        if self.state is not WatchdogState.COOLDOWN and cooldown_until is not None:
            raise ValueError("Only COOLDOWN state can have an expiration time.")
        last_transition_at = self.last_transition_at
        if last_transition_at is not None:
            last_transition_at = _utc(last_transition_at)
            if last_transition_at > last_detected_at:
                raise ValueError("Last transition cannot follow last detection.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "changed_at", changed_at)
        object.__setattr__(self, "last_detected_at", last_detected_at)
        object.__setattr__(self, "cooldown_until", cooldown_until)
        object.__setattr__(self, "last_transition_at", last_transition_at)

    @classmethod
    def initial(
        cls,
        symbol: str,
        *,
        detected_at: datetime,
    ) -> WatchdogSymbolState:
        now = _utc(detected_at)
        return cls(symbol, WatchdogState.NORMAL, now, now, 0.0)


@dataclass(frozen=True, slots=True)
class StateTransition:
    symbol: str
    previous_state: WatchdogState
    state: WatchdogState
    available_at: datetime
    detected_at: datetime
    anomaly_score: float
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.reasons:
            raise ValueError("State transition requires identity and reasons.")
        available_at = _utc(self.available_at)
        detected_at = _utc(self.detected_at)
        if available_at > detected_at:
            raise ValueError("State transition violates PIT chronology.")
        if not math.isfinite(self.anomaly_score) or not 0 <= self.anomaly_score <= 100:
            raise ValueError("Transition anomaly score must be between 0 and 100.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "detected_at", detected_at)


@dataclass(frozen=True, slots=True)
class StateMachineResult:
    state: WatchdogSymbolState
    transition: StateTransition | None


class WatchdogStateMachine:
    """Deterministic hysteretic lifecycle independent from trading direction."""

    def __init__(self, config: StateMachineConfig | None = None) -> None:
        self._config = config or StateMachineConfig()

    def evaluate(
        self,
        current: WatchdogSymbolState,
        evaluation: StateEvaluation,
    ) -> StateMachineResult:
        if evaluation.detected_at < current.last_detected_at:
            raise ValueError("State evaluations must be chronological.")
        if current.state is WatchdogState.COOLDOWN:
            return self._evaluate_cooldown(current, evaluation)
        upward = self._upward_threshold(current.state)
        downward = self._downward_threshold(current.state)
        up_count = (
            current.consecutive_escalations + 1
            if upward is not None and evaluation.anomaly_score >= upward
            else 0
        )
        down_count = (
            current.consecutive_deescalations + 1
            if downward is not None and evaluation.anomaly_score < downward
            else 0
        )
        next_state: WatchdogState = current.state
        if up_count >= self._config.escalation_confirmations:
            next_state = _escalated(current.state)
        elif down_count >= self._config.deescalation_confirmations:
            next_state = _deescalated(current.state)

        if next_state is current.state:
            updated = replace(
                current,
                last_detected_at=evaluation.detected_at,
                last_score=evaluation.anomaly_score,
                consecutive_escalations=up_count,
                consecutive_deescalations=down_count,
            )
            return StateMachineResult(updated, None)
        cooldown_until = (
            evaluation.detected_at + self._config.cooldown
            if next_state is WatchdogState.COOLDOWN
            else None
        )
        updated = WatchdogSymbolState(
            symbol=current.symbol,
            state=next_state,
            changed_at=evaluation.detected_at,
            last_detected_at=evaluation.detected_at,
            last_score=evaluation.anomaly_score,
            cooldown_until=cooldown_until,
            previous_state=current.state,
            last_transition_at=evaluation.detected_at,
        )
        return StateMachineResult(
            updated,
            StateTransition(
                symbol=current.symbol,
                previous_state=current.state,
                state=next_state,
                available_at=evaluation.available_at,
                detected_at=evaluation.detected_at,
                anomaly_score=evaluation.anomaly_score,
                reasons=evaluation.reasons,
            ),
        )

    def _evaluate_cooldown(
        self,
        current: WatchdogSymbolState,
        evaluation: StateEvaluation,
    ) -> StateMachineResult:
        if current.cooldown_until is None:
            raise ValueError("COOLDOWN state requires an expiration time.")
        if evaluation.detected_at < current.cooldown_until:
            return StateMachineResult(
                replace(
                    current,
                    last_detected_at=evaluation.detected_at,
                    last_score=evaluation.anomaly_score,
                ),
                None,
            )
        next_state = (
            WatchdogState.WATCH
            if evaluation.anomaly_score >= self._config.watch_enter
            else WatchdogState.NORMAL
        )
        updated = WatchdogSymbolState(
            symbol=current.symbol,
            state=next_state,
            changed_at=evaluation.detected_at,
            last_detected_at=evaluation.detected_at,
            last_score=evaluation.anomaly_score,
            previous_state=current.state,
            last_transition_at=evaluation.detected_at,
        )
        reason = (
            "cooldown_reentry"
            if next_state is WatchdogState.WATCH
            else "cooldown_expired"
        )
        return StateMachineResult(
            updated,
            StateTransition(
                symbol=current.symbol,
                previous_state=WatchdogState.COOLDOWN,
                state=next_state,
                available_at=evaluation.available_at,
                detected_at=evaluation.detected_at,
                anomaly_score=evaluation.anomaly_score,
                reasons=(reason, *evaluation.reasons),
            ),
        )

    def _upward_threshold(self, state: WatchdogState) -> float | None:
        return {
            WatchdogState.NORMAL: self._config.watch_enter,
            WatchdogState.WATCH: self._config.in_play_enter,
            WatchdogState.IN_PLAY: self._config.high_attention_enter,
            WatchdogState.HIGH_ATTENTION: None,
            WatchdogState.COOLDOWN: None,
        }[state]

    def _downward_threshold(self, state: WatchdogState) -> float | None:
        return {
            WatchdogState.NORMAL: None,
            WatchdogState.WATCH: self._config.watch_exit,
            WatchdogState.IN_PLAY: self._config.in_play_exit,
            WatchdogState.HIGH_ATTENTION: self._config.high_attention_exit,
            WatchdogState.COOLDOWN: None,
        }[state]


def _escalated(state: WatchdogState) -> WatchdogState:
    return {
        WatchdogState.NORMAL: WatchdogState.WATCH,
        WatchdogState.WATCH: WatchdogState.IN_PLAY,
        WatchdogState.IN_PLAY: WatchdogState.HIGH_ATTENTION,
        WatchdogState.HIGH_ATTENTION: WatchdogState.HIGH_ATTENTION,
        WatchdogState.COOLDOWN: WatchdogState.COOLDOWN,
    }[state]


def _deescalated(state: WatchdogState) -> WatchdogState:
    return {
        WatchdogState.NORMAL: WatchdogState.NORMAL,
        WatchdogState.WATCH: WatchdogState.COOLDOWN,
        WatchdogState.IN_PLAY: WatchdogState.WATCH,
        WatchdogState.HIGH_ATTENTION: WatchdogState.IN_PLAY,
        WatchdogState.COOLDOWN: WatchdogState.COOLDOWN,
    }[state]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("State time must be timezone-aware.")
    return value.astimezone(UTC)
