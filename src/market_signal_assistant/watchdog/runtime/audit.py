from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Any

from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal


class OperationalEventType(StrEnum):
    RUNTIME_START = "RUNTIME_START"
    RUNTIME_STOP = "RUNTIME_STOP"
    UNIVERSE_REFRESH = "UNIVERSE_REFRESH"
    PROVIDER_FAILURE = "PROVIDER_FAILURE"
    RATE_LIMIT = "RATE_LIMIT"
    THROTTLE_WAIT = "THROTTLE_WAIT"
    SYMBOL_FAILURE = "SYMBOL_FAILURE"
    DEGRADED_MODE = "DEGRADED_MODE"
    RECOVERY = "RECOVERY"
    FATAL_ERROR = "FATAL_ERROR"
    CURSOR_REALIGNMENT = "CURSOR_REALIGNMENT"
    SCHEDULER_TIMEOUT = "SCHEDULER_TIMEOUT"
    SCHEDULER_RECONCILIATION = "SCHEDULER_RECONCILIATION"


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
        self._rollup_path = path.with_name(f"{path.stem}-rollups.json")
        self._rollup_lock = Lock()

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

    def append_rollup(self, event: OperationalEvent, *, signature: str) -> bool:
        """Record every occurrence cumulatively but journal a signature once."""
        normalized = signature.strip()
        if not normalized:
            raise ValueError("Operational rollup signature cannot be empty.")
        with self._rollup_lock:
            rollups = self._load_rollups()
            existing = rollups.get(normalized)
            first = existing is None
            symbols: set[str] = set()
            count = 0
            first_seen = event.occurred_at.isoformat()
            if existing is not None:
                raw_count = existing.get("count")
                if not isinstance(raw_count, int):
                    raise ValueError("Operational rollup count is invalid.")
                count = raw_count
                first_seen = str(existing["first_seen"])
                raw_symbols = existing.get("affected_symbols", [])
                if isinstance(raw_symbols, list):
                    symbols.update(str(item) for item in raw_symbols)
            if event.symbol is not None:
                symbols.add(event.symbol)
            rollups[normalized] = {
                "signature": normalized,
                "event_type": event.event_type.value,
                "count": count + 1,
                "first_seen": first_seen,
                "last_seen": event.occurred_at.isoformat(),
                "affected_symbols": sorted(symbols),
                "latest_details": dict(event.details),
            }
            self._save_rollups(rollups)
        return self.append(event) if first else False

    def rollups(self) -> tuple[dict[str, object], ...]:
        with self._rollup_lock:
            return tuple(
                dict(value)
                for _, value in sorted(self._load_rollups().items())
            )

    @property
    def retained_index_entries(self) -> int:
        return self._journal.retained_index_entries

    def _load_rollups(self) -> dict[str, dict[str, object]]:
        if not self._rollup_path.exists():
            return {}
        value: Any = json.loads(self._rollup_path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("Operational rollup state is invalid.")
        return {
            str(key): dict(item)
            for key, item in value.items()
            if isinstance(item, dict)
        }

    def _save_rollups(self, rollups: dict[str, dict[str, object]]) -> None:
        temporary: Path | None = None
        try:
            self._rollup_path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._rollup_path.parent,
                prefix=f".{self._rollup_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(rollups, stream, sort_keys=True, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._rollup_path)
        except OSError:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise
