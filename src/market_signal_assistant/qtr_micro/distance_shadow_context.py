"""Strictly read-only, one-shot fallback. No recovery or execution services."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.qtr_micro.distance_shadow import FrozenRuntimeContext
from market_signal_assistant.qtr_micro.models import InstrumentRules, PreflightResult
from market_signal_assistant.qtr_micro.state import _state_from_json
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate


class ContextFallbackFailed(RuntimeError):
    def __init__(self, stage: str, error: Exception) -> None:
        self.stage = stage
        self.detail = type(error).__name__  # exception messages may contain secrets
        super().__init__(f"{stage}:{self.detail}")


@dataclass(frozen=True, slots=True)
class ReadOnlyShadowContextProvider:
    state_path: Path
    equity_reader: Callable[[], float]
    rules_reader: Callable[[str], InstrumentRules]
    preflight_reader: Callable[[], PreflightResult]
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)

    def __call__(self, candidate: QtrSetupCandidate) -> FrozenRuntimeContext:
        stage = "state_snapshot"
        try:
            # Deliberately not JsonQtrMicroStateStore.load(): it can rename state.
            state = _state_from_json(json.loads(self.state_path.read_bytes()))
            stage = "preflight_snapshot"
            preflight = self.preflight_reader()
            if preflight.mode != "demo" or preflight.host != "api-demo.bybit.com":
                raise ValueError("Shadow requires factual Demo preflight.")
            stage = "equity"
            equity = self.equity_reader()
            stage = "instrument_rules"
            rules = self.rules_reader(candidate.result.symbol)
            if rules.symbol != candidate.result.symbol:
                raise ValueError("Symbol rules mismatch.")
            stage = "freeze_context"
            return FrozenRuntimeContext.capture(
                now=self.clock(),
                equity=equity,
                rules=rules,
                state=state,
                preflight=preflight,
            )
        except Exception as error:
            raise ContextFallbackFailed(stage, error) from None
