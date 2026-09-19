from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.watchdog.aggregation import SequenceContext, SequenceStep
from market_signal_assistant.watchdog.models import AnomalyType, WatchdogState
from market_signal_assistant.watchdog.state_machine import WatchdogSymbolState

STATE_SCHEMA_VERSION = 1


class WatchdogStateStoreError(RuntimeError):
    """Durable Watchdog runtime state cannot be read or written safely."""


@dataclass(frozen=True, slots=True)
class WatchdogRuntimeState:
    symbol_state: WatchdogSymbolState
    sequence_context: SequenceContext = SequenceContext()


class JsonWatchdogStateStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> tuple[WatchdogRuntimeState, ...]:
        if not self._path.exists():
            return ()
        try:
            payload: Any = json.loads(self._path.read_text(encoding="utf-8"))
            if (
                not isinstance(payload, dict)
                or payload.get("version") != STATE_SCHEMA_VERSION
                or not isinstance(payload.get("symbols"), list)
            ):
                raise ValueError
            return tuple(_runtime_from_json(item) for item in payload["symbols"])
        except (
            OSError,
            TypeError,
            ValueError,
            KeyError,
            json.JSONDecodeError,
        ) as error:
            raise WatchdogStateStoreError(
                "Watchdog runtime state is invalid."
            ) from error

    def save(self, states: tuple[WatchdogRuntimeState, ...]) -> None:
        payload = {
            "version": STATE_SCHEMA_VERSION,
            "symbols": [_runtime_to_json(item) for item in states],
        }
        temporary: Path | None = None
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._path)
        except OSError as error:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise WatchdogStateStoreError(
                "Watchdog runtime state cannot be saved."
            ) from error


class WatchdogStateRepository:
    """In-memory index backed by an atomic whole-state snapshot."""

    def __init__(self, store: JsonWatchdogStateStore) -> None:
        self._store = store
        loaded = store.load()
        self._states = {item.symbol_state.symbol: item for item in loaded}
        if len(self._states) != len(loaded):
            raise WatchdogStateStoreError("Watchdog runtime state has duplicates.")

    def get(self, symbol: str, *, detected_at: datetime) -> WatchdogRuntimeState:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("Watchdog state symbol cannot be empty.")
        existing = self._states.get(normalized)
        if existing is not None:
            return existing
        return WatchdogRuntimeState(
            WatchdogSymbolState.initial(normalized, detected_at=detected_at)
        )

    def save(self, state: WatchdogRuntimeState) -> None:
        updated = {**self._states, state.symbol_state.symbol: state}
        ordered = tuple(updated[key] for key in sorted(updated))
        self._store.save(ordered)
        self._states = updated


def _runtime_to_json(item: WatchdogRuntimeState) -> dict[str, object]:
    state = item.symbol_state
    return {
        "symbol": state.symbol,
        "state": state.state.value,
        "previous_state": state.previous_state.value,
        "changed_at": state.changed_at.isoformat(),
        "last_detected_at": state.last_detected_at.isoformat(),
        "last_transition_at": (
            state.last_transition_at.isoformat()
            if state.last_transition_at is not None
            else None
        ),
        "last_score": state.last_score,
        "consecutive_escalations": state.consecutive_escalations,
        "consecutive_deescalations": state.consecutive_deescalations,
        "cooldown_until": (
            state.cooldown_until.isoformat()
            if state.cooldown_until is not None
            else None
        ),
        "sequence": [
            {
                "anomaly_type": step.anomaly_type.value,
                "detected_at": step.detected_at.isoformat(),
            }
            for step in item.sequence_context.steps
        ],
    }


def _runtime_from_json(value: object) -> WatchdogRuntimeState:
    if not isinstance(value, dict):
        raise ValueError
    cooldown = value.get("cooldown_until")
    transition = value.get("last_transition_at")
    sequence = value.get("sequence")
    if not isinstance(sequence, list):
        raise ValueError
    state = WatchdogSymbolState(
        symbol=str(value["symbol"]),
        state=WatchdogState(str(value["state"])),
        changed_at=datetime.fromisoformat(str(value["changed_at"])),
        last_detected_at=datetime.fromisoformat(str(value["last_detected_at"])),
        last_score=float(value["last_score"]),
        consecutive_escalations=int(value["consecutive_escalations"]),
        consecutive_deescalations=int(value["consecutive_deescalations"]),
        cooldown_until=(
            datetime.fromisoformat(str(cooldown)) if cooldown is not None else None
        ),
        previous_state=WatchdogState(str(value["previous_state"])),
        last_transition_at=(
            datetime.fromisoformat(str(transition)) if transition is not None else None
        ),
    )
    steps = tuple(_step_from_json(item) for item in sequence)
    return WatchdogRuntimeState(state, SequenceContext(steps))


def _step_from_json(value: object) -> SequenceStep:
    if not isinstance(value, dict):
        raise ValueError
    return SequenceStep(
        AnomalyType(str(value["anomaly_type"])),
        datetime.fromisoformat(str(value["detected_at"])),
    )
