from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal


@dataclass(frozen=True, slots=True)
class UniverseTransition:
    symbol: str
    previous_eligible: bool | None
    new_eligible: bool
    previous_tier: str | None
    new_tier: str | None
    previous_state: str | None
    new_state: str | None
    rejection_reasons: tuple[str, ...]
    observed_at: datetime
    available_at: datetime
    recorded_at: datetime

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("Universe transition requires a symbol.")
        if self.new_eligible != (self.new_tier is not None):
            raise ValueError("Eligible universe transitions require a tier.")
        times = (self.observed_at, self.available_at, self.recorded_at)
        if any(value.tzinfo is None or value.utcoffset() is None for value in times):
            raise ValueError("Universe transition timestamps must be timezone-aware.")
        observed, available, recorded = (item.astimezone(UTC) for item in times)
        if not observed <= available <= recorded:
            raise ValueError("Universe transition timestamps are not PIT ordered.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "recorded_at", recorded)

    @property
    def transition_id(self) -> str:
        identity = (
            self.symbol,
            self.previous_eligible,
            self.new_eligible,
            self.previous_tier,
            self.new_tier,
            self.observed_at.isoformat(),
        )
        return hashlib.sha256(repr(identity).encode("utf-8")).hexdigest()

    def payload(self) -> dict[str, Any]:
        return {
            "transition_id": self.transition_id,
            "symbol": self.symbol,
            "previous_eligible": self.previous_eligible,
            "new_eligible": self.new_eligible,
            "previous_tier": self.previous_tier,
            "new_tier": self.new_tier,
            "previous_state": self.previous_state,
            "new_state": self.new_state,
            "rejection_reasons": list(self.rejection_reasons),
            "observed_at": self.observed_at.isoformat(),
            "available_at": self.available_at.isoformat(),
            "recorded_at": self.recorded_at.isoformat(),
        }


class UniverseTransitionJournal:
    """Append-only causal evidence; replay retains only current membership."""

    def __init__(self, path: Path) -> None:
        self._journal = ImmutableJsonlJournal(path, id_field="transition_id")
        self._path = path

    def append(self, transition: UniverseTransition) -> bool:
        return self._journal.append(transition.transition_id, transition.payload())

    def current(self) -> dict[str, tuple[str, str | None]]:
        current: dict[str, tuple[str, str | None]] = {}
        if not self._path.exists():
            return current
        with self._path.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.endswith("\n"):
                    continue
                value: Any = json.loads(line)
                symbol = str(value["symbol"])
                if value["new_eligible"]:
                    current[symbol] = (str(value["new_tier"]), value["new_state"])
                else:
                    current.pop(symbol, None)
        return current

    def last_removals(self, symbols: set[str]) -> dict[str, datetime]:
        removed: dict[str, datetime] = {}
        if not self._path.exists():
            return removed
        with self._path.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.endswith("\n"):
                    continue
                value: Any = json.loads(line)
                symbol = str(value["symbol"])
                if symbol not in symbols:
                    continue
                if value["new_eligible"]:
                    removed.pop(symbol, None)
                else:
                    removed[symbol] = datetime.fromisoformat(value["observed_at"])
        return removed

    @property
    def retained_index_entries(self) -> int:
        return self._journal.retained_index_entries
