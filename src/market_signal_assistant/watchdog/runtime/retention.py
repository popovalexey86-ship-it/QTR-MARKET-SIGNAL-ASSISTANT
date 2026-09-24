from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_signal_assistant.watchdog.baselines import RollingBaselineEngine
from market_signal_assistant.watchdog.runtime.schedule import JsonBucketCursorStore
from market_signal_assistant.watchdog.runtime.universe_evidence import (
    UniverseTransitionJournal,
)
from market_signal_assistant.watchdog.state_store import (
    WatchdogRuntimeState,
    WatchdogStateRepository,
    _runtime_from_json,
    _runtime_to_json,
)


@dataclass(frozen=True, slots=True)
class InactiveTombstone:
    symbol: str
    evicted_at: datetime
    chronology_floor: datetime | None
    state: WatchdogRuntimeState | None
    cursors: dict[str, datetime]


class InactiveSymbolArchive:
    """Disk-only tombstones for PIT-safe restoration of light symbol state."""

    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS inactive ("
                "symbol TEXT PRIMARY KEY, evicted_at TEXT NOT NULL, "
                "chronology_floor TEXT, state_json TEXT, cursors_json TEXT NOT NULL)"
            )

    def get(self, symbol: str) -> InactiveTombstone | None:
        normalized = symbol.strip().upper()
        with sqlite3.connect(self._path) as connection:
            row = connection.execute(
                "SELECT evicted_at, chronology_floor, state_json, cursors_json "
                "FROM inactive WHERE symbol = ?",
                (normalized,),
            ).fetchone()
        if row is None:
            return None
        state = _runtime_from_json(json.loads(row[2])) if row[2] else None
        cursors = {
            interval: datetime.fromisoformat(boundary)
            for interval, boundary in json.loads(row[3]).items()
        }
        return InactiveTombstone(
            normalized,
            datetime.fromisoformat(row[0]),
            datetime.fromisoformat(row[1]) if row[1] else None,
            state,
            cursors,
        )

    def save(
        self,
        symbol: str,
        *,
        evicted_at: datetime,
        state: WatchdogRuntimeState | None,
        cursors: dict[str, datetime],
    ) -> None:
        normalized = symbol.strip().upper()
        floor = state.symbol_state.last_detected_at if state else None
        payload = json.dumps(_runtime_to_json(state)) if state else None
        cursor_payload = json.dumps(
            {key: value.isoformat() for key, value in cursors.items()},
            sort_keys=True,
        )
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO inactive VALUES (?, ?, ?, ?, ?)",
                (
                    normalized,
                    evicted_at.astimezone(UTC).isoformat(),
                    floor.isoformat() if floor else None,
                    payload,
                    cursor_payload,
                ),
            )

    def delete(self, symbol: str) -> None:
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                "DELETE FROM inactive WHERE symbol = ?", (symbol.strip().upper(),)
            )

    @property
    def record_count(self) -> int:
        with sqlite3.connect(self._path) as connection:
            return int(
                connection.execute("SELECT COUNT(*) FROM inactive").fetchone()[0]
            )


class InactiveRetention:
    """Bound live history by current universe plus an explicit inactive window."""

    def __init__(
        self,
        baselines: RollingBaselineEngine,
        states: WatchdogStateRepository,
        cursors: JsonBucketCursorStore,
        archive: InactiveSymbolArchive,
        transitions: UniverseTransitionJournal,
        *,
        window: timedelta = timedelta(hours=24),
    ) -> None:
        if window <= timedelta(0):
            raise ValueError("Inactive retention window must be positive.")
        self._baselines = baselines
        self._states = states
        self._cursors = cursors
        self._archive = archive
        self._window = window
        resident = baselines.symbols | states.symbols | cursors.symbols
        self._inactive_since = transitions.last_removals(set(resident))

    def removed(self, symbol: str, observed_at: datetime) -> None:
        self._inactive_since[symbol.strip().upper()] = observed_at.astimezone(UTC)

    def activated(self, symbol: str) -> bool:
        """Restore chronology but never resurrect evicted baseline observations."""
        normalized = symbol.strip().upper()
        tombstone = self._archive.get(normalized)
        was_inactive = normalized in self._inactive_since or tombstone is not None
        # Re-entry is always a new baseline maturation epoch, including when
        # the inactivity window has not elapsed yet.
        removed_observations = (
            self._baselines.evict_symbol(normalized) if was_inactive else 0
        )
        if tombstone is not None:
            # Archive-first eviction is restart-safe: a crash after writing a
            # tombstone cannot accidentally restore the old baseline.
            current = self._states.persisted(normalized)
            if tombstone.state is not None and (
                current is None
                or current.symbol_state.last_detected_at
                < tombstone.state.symbol_state.last_detected_at
            ):
                self._states.save(tombstone.state)
            for interval, boundary in tombstone.cursors.items():
                existing = self._cursors.get(normalized, interval)
                if existing is None or existing < boundary:
                    self._cursors.save(normalized, interval, boundary)
            self._archive.delete(normalized)
        self._inactive_since.pop(normalized, None)
        return tombstone is not None or removed_observations > 0

    def expire(self, now: datetime, active: set[str]) -> tuple[str, ...]:
        as_of = now.astimezone(UTC)
        resident = (
            self._baselines.symbols | self._states.symbols | self._cursors.symbols
        )
        evicted: list[str] = []
        for symbol in sorted(resident - active):
            since = self._inactive_since.setdefault(symbol, as_of)
            if as_of - since < self._window:
                continue
            if self._archive.get(symbol) is None:
                self._archive.save(
                    symbol,
                    evicted_at=as_of,
                    state=self._states.persisted(symbol),
                    cursors=self._cursors.for_symbol(symbol),
                )
            evicted.append(symbol)
        if evicted:
            selected = set(evicted)
            self._baselines.evict_symbols(selected)
            self._states.evict_symbols(selected)
            self._cursors.evict_symbols(selected)
            for symbol in evicted:
                self._inactive_since.pop(symbol, None)
        return tuple(evicted)

    @property
    def retained_inactive_count(self) -> int:
        return len(self._inactive_since)
