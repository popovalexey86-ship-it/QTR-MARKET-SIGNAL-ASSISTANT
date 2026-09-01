from __future__ import annotations

from typing import Protocol

from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.audit import JsonlSetupAuditStore
from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupAnalysisResult,
)
from market_signal_assistant.setup_engine.settings import SetupEngineSettings


class SetupAuditStore(Protocol):
    def append(
        self,
        data: SetupAnalysisInput,
        result: SetupAnalysisResult,
    ) -> None: ...


class SetupEngine:
    """Opt-in orchestration around the pure setup classifier."""

    def __init__(
        self,
        settings: SetupEngineSettings | None = None,
        audit_store: SetupAuditStore | None = None,
    ) -> None:
        self._settings = settings or SetupEngineSettings.from_environment()
        self._audit_store = audit_store
        if self._settings.enabled and self._audit_store is None:
            self._audit_store = JsonlSetupAuditStore(self._settings.audit_path)

    def analyze(self, data: SetupAnalysisInput) -> SetupAnalysisResult:
        result = analyze_setup(data)
        if self._settings.enabled:
            if self._audit_store is None:
                raise RuntimeError("Setup Engine audit store не настроен.")
            self._audit_store.append(data, result)
        return result
