from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal


class OperationalEventType(StrEnum):
    RUNTIME_START = "RUNTIME_START"
    RUNTIME_STOP = "RUNTIME_STOP"
    UNIVERSE_REFRESH = "UNIVERSE_REFRESH"
    PROVIDER_FAILURE = "PROVIDER_FAILURE"
    RATE_LIMIT = "RATE_LIMIT"
    DEGRADED_MODE = "DEGRADED_MODE"
    RECOVERY = "RECOVERY"
    FATAL_ERROR = "FATAL_ERROR"


@dataclass(frozen=True, slots=True)
class OperationalEvent:
    event_type: OperationalEventType
    occurred_at: datetime
    details: tuple[tuple[str, str], ...]
    symbol: str | None = None

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("Operational event time must be timezone-aware.")
        if not self.details:
            raise ValueError("Operational event requires details.")
        object.__setattr__(self, "occurred_at", self.occurred_at.astimezone(UTC))
        if self.symbol is not None:
            object.__setattr__(self, "symbol", self.symbol.strip().upper())

    @property
    def audit_id(self) -> str:
        payload = json.dumps(
            [
                self.event_type.value,
                self.occurred_at.isoformat(),
                self.symbol,
                self.details,
            ],
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class OperationalAuditJournal:
    def __init__(self, path: Path) -> None:
        self._journal = ImmutableJsonlJournal(path, id_field="audit_id")

    def append(self, event: OperationalEvent) -> bool:
        return self._journal.append(
            event.audit_id,
            {
                "audit_id": event.audit_id,
                "event_type": event.event_type.value,
                "occurred_at": event.occurred_at.isoformat(),
                "symbol": event.symbol,
                "details": dict(event.details),
            },
        )

    def records(self) -> tuple[dict[str, object], ...]:
        return self._journal.records()
