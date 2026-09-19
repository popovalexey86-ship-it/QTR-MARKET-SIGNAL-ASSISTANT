from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.watchdog.aggregation import SequenceContext, SequenceStep
from market_signal_assistant.watchdog.models import AnomalyType, WatchdogState
from market_signal_assistant.watchdog.state_machine import WatchdogSymbolState
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
    WatchdogRuntimeState,
    WatchdogStateRepository,
    WatchdogStateStoreError,
)

NOW = datetime(2026, 9, 18, 8, tzinfo=UTC)


@pytest.mark.parametrize("state", [WatchdogState.WATCH, WatchdogState.IN_PLAY])
def test_restart_preserves_active_state_counters_and_sequence(
    tmp_path: Path, state: WatchdogState
) -> None:
    path = tmp_path / "watchdog-state.json"
    repository = WatchdogStateRepository(JsonWatchdogStateStore(path))
    expected = WatchdogRuntimeState(
        WatchdogSymbolState(
            symbol="ABCUSDT",
            state=state,
            previous_state=WatchdogState.NORMAL,
            changed_at=NOW - timedelta(minutes=10),
            last_detected_at=NOW,
            last_transition_at=NOW - timedelta(minutes=10),
            last_score=64.0,
            consecutive_escalations=1,
            consecutive_deescalations=0,
        ),
        SequenceContext(
            (SequenceStep(AnomalyType.COMPRESSION, NOW - timedelta(minutes=5)),)
        ),
    )
    repository.save(expected)

    restarted = WatchdogStateRepository(JsonWatchdogStateStore(path))
    recovered = restarted.get("abcusdt", detected_at=NOW + timedelta(minutes=1))

    assert recovered == expected
    assert recovered.symbol_state.state is state
    assert recovered.symbol_state.state is not WatchdogState.NORMAL
    assert not tuple(tmp_path.glob("*.tmp"))


def test_cooldown_and_last_transition_survive_restart(tmp_path: Path) -> None:
    path = tmp_path / "watchdog-state.json"
    state = WatchdogSymbolState(
        symbol="ABCUSDT",
        state=WatchdogState.COOLDOWN,
        previous_state=WatchdogState.WATCH,
        changed_at=NOW,
        last_detected_at=NOW,
        last_transition_at=NOW,
        last_score=10.0,
        cooldown_until=NOW + timedelta(minutes=30),
    )
    WatchdogStateRepository(JsonWatchdogStateStore(path)).save(
        WatchdogRuntimeState(state)
    )

    recovered = WatchdogStateRepository(JsonWatchdogStateStore(path)).get(
        "ABCUSDT", detected_at=NOW
    )

    assert recovered.symbol_state.cooldown_until == NOW + timedelta(minutes=30)
    assert recovered.symbol_state.previous_state is WatchdogState.WATCH
    assert recovered.symbol_state.last_transition_at == NOW


def test_corrupt_state_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "watchdog-state.json"
    path.write_text("not-json", encoding="utf-8")

    with pytest.raises(WatchdogStateStoreError, match="invalid"):
        WatchdogStateRepository(JsonWatchdogStateStore(path))
