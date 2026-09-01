from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from market_signal_assistant.setup_engine.audit import (
    DEFAULT_SETUP_ENGINE_AUDIT_PATH,
)


@dataclass(frozen=True, slots=True)
class SetupEngineSettings:
    enabled: bool = False
    audit_path: Path = DEFAULT_SETUP_ENGINE_AUDIT_PATH

    @classmethod
    def from_environment(cls) -> SetupEngineSettings:
        raw = os.getenv("QTR_SETUP_ENGINE_ENABLED", "false").strip().lower()
        if raw not in {"true", "false"}:
            raise ValueError(
                "QTR_SETUP_ENGINE_ENABLED должна быть равна true или false."
            )
        return cls(enabled=raw == "true")
