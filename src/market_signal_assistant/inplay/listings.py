from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from market_signal_assistant.inplay.models import ListingStatus


class ListingStateError(RuntimeError):
    """Controlled local listing-state failure."""


@dataclass(frozen=True, slots=True)
class ListingRecord:
    first_seen: datetime
    discovered_after_baseline: bool


@dataclass(frozen=True, slots=True)
class ListingSnapshot:
    initialized_at: datetime | None
    active_symbols: tuple[str, ...]
    records: Mapping[str, ListingRecord]


class JsonListingStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> ListingSnapshot:
        if not self._path.exists():
            return ListingSnapshot(None, (), {})
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            initialized_at = _datetime(raw["initialized_at"])
            active_symbols = tuple(str(item) for item in raw["active_symbols"])
            records = {
                str(symbol): ListingRecord(
                    first_seen=_datetime(value["first_seen"]),
                    discovered_after_baseline=bool(
                        value["discovered_after_baseline"]
                    ),
                )
                for symbol, value in raw["records"].items()
            }
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            raise ListingStateError("Local listing state is invalid.") from None
        return ListingSnapshot(initialized_at, active_symbols, records)

    def save(self, snapshot: ListingSnapshot) -> None:
        if snapshot.initialized_at is None:
            raise ListingStateError("Initialized listing state requires a timestamp.")
        payload = {
            "version": 1,
            "initialized_at": snapshot.initialized_at.isoformat(),
            "active_symbols": list(snapshot.active_symbols),
            "records": {
                symbol: {
                    "first_seen": record.first_seen.isoformat(),
                    "discovered_after_baseline": record.discovered_after_baseline,
                }
                for symbol, record in sorted(snapshot.records.items())
            },
        }
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(self._path)
        except OSError:
            raise ListingStateError("Local listing state cannot be saved.") from None


class ListingTracker:
    def __init__(self, store: JsonListingStore) -> None:
        self._store = store

    def observe(
        self,
        symbols: tuple[str, ...],
        observed_at: datetime,
    ) -> tuple[ListingStatus, ...]:
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("Listing observation time must be timezone-aware.")
        now = observed_at.astimezone(UTC)
        current = tuple(dict.fromkeys(symbol.strip().upper() for symbol in symbols))
        snapshot = self._store.load()
        records = dict(snapshot.records)
        baseline = snapshot.initialized_at is None
        for symbol in current:
            if symbol and symbol not in records:
                records[symbol] = ListingRecord(
                    first_seen=now,
                    discovered_after_baseline=not baseline,
                )
        updated = ListingSnapshot(
            initialized_at=snapshot.initialized_at or now,
            active_symbols=current,
            records=records,
        )
        self._store.save(updated)
        return tuple(_status(symbol, records[symbol], now) for symbol in current)


def _status(
    symbol: str,
    record: ListingRecord,
    observed_at: datetime,
) -> ListingStatus:
    age = max(timedelta(), observed_at - record.first_seen)
    is_recent = record.discovered_after_baseline and age < timedelta(days=7)
    return ListingStatus(
        symbol=symbol,
        first_seen=record.first_seen,
        is_new_listing=is_recent,
        listing_bonus=_listing_bonus(age) if record.discovered_after_baseline else 0.0,
    )


def _listing_bonus(age: timedelta) -> float:
    if age < timedelta(hours=24):
        return 10.0
    if age < timedelta(hours=72):
        return 6.0
    if age < timedelta(days=7):
        return 3.0
    return 0.0


def _datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected ISO timestamp.")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Expected timezone-aware ISO timestamp.")
    return parsed.astimezone(UTC)
