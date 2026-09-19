from datetime import UTC, datetime, timedelta

from market_signal_assistant.watchdog.models import WatchdogState
from market_signal_assistant.watchdog.state_machine import (
    StateEvaluation,
    WatchdogStateMachine,
    WatchdogSymbolState,
)

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


def evaluation(score: float, minute: int) -> StateEvaluation:
    detected_at = NOW + timedelta(minutes=minute)
    return StateEvaluation(score, detected_at, detected_at, (f"score={score}",))


def test_deterministic_escalation_hysteresis_and_cooldown() -> None:
    machine = WatchdogStateMachine()
    state = WatchdogSymbolState.initial("ABCUSDT", detected_at=NOW)
    transitions: list[tuple[WatchdogState, WatchdogState]] = []

    for minute, score in enumerate((35, 35, 60, 60, 80, 80), start=1):
        result = machine.evaluate(state, evaluation(score, minute))
        state = result.state
        if result.transition is not None:
            transitions.append(
                (result.transition.previous_state, result.transition.state)
            )

    assert transitions == [
        (WatchdogState.NORMAL, WatchdogState.WATCH),
        (WatchdogState.WATCH, WatchdogState.IN_PLAY),
        (WatchdogState.IN_PLAY, WatchdogState.HIGH_ATTENTION),
    ]

    for minute, score in ((7, 64), (8, 64), (9, 44), (10, 44)):
        state = machine.evaluate(state, evaluation(score, minute)).state
    assert state.state is WatchdogState.WATCH

    state = machine.evaluate(state, evaluation(19, 11)).state
    result = machine.evaluate(state, evaluation(19, 12))
    assert result.state.state is WatchdogState.COOLDOWN
    assert result.state.cooldown_until == NOW + timedelta(minutes=42)

    held = machine.evaluate(result.state, evaluation(90, 20))
    assert held.state.state is WatchdogState.COOLDOWN
    reentered = machine.evaluate(held.state, evaluation(90, 42))
    assert reentered.state.state is WatchdogState.WATCH


def test_mid_band_scores_do_not_churn_state() -> None:
    machine = WatchdogStateMachine()
    state = WatchdogSymbolState.initial("ABCUSDT", detected_at=NOW)
    state = machine.evaluate(state, evaluation(35, 1)).state
    state = machine.evaluate(state, evaluation(35, 2)).state

    for minute, score in ((3, 25), (4, 29), (5, 21), (6, 30)):
        result = machine.evaluate(state, evaluation(score, minute))
        state = result.state
        assert result.transition is None

    assert state.state is WatchdogState.WATCH
