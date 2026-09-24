from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class BucketCursorError(RuntimeError):
    """Completed-bucket cursor cannot be recovered safely."""


@dataclass(frozen=True, slots=True)
class CompletedBucketScheduler:
    maximum_catchup_buckets: int = 64

    def due(
        self,
        *,
        now: datetime,
        interval: str,
        last_completed: datetime | None,
    ) -> tuple[datetime, ...]:
        return self.plan(
            now=now, interval=interval, last_completed=last_completed
        ).buckets

    def plan(
        self,
        *,
        now: datetime,
        interval: str,
        last_completed: datetime | None,
    ) -> BucketPlan:
        current = completed_boundary(now, interval)
        if last_completed is None:
            return BucketPlan((current,))
        previous = _utc(last_completed)
        if previous >= current:
            return BucketPlan(())
        duration = interval_duration(interval)
        due: list[datetime] = []
        cursor = previous + duration
        while cursor <= current:
            due.append(cursor)
            cursor += duration
        omitted = max(0, len(due) - self.maximum_catchup_buckets)
        return BucketPlan(tuple(due[-self.maximum_catchup_buckets :]), omitted)


@dataclass(frozen=True, slots=True)
class BucketPlan:
    buckets: tuple[datetime, ...]
    omitted_bucket_count: int = 0


class JsonBucketCursorStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._cursors = self._load()

    def get(self, symbol: str, interval: str) -> datetime | None:
        return self._cursors.get((symbol.strip().upper(), interval))

    @property
    def retained_count(self) -> int:
        return len(self._cursors)

    @property
    def symbols(self) -> frozenset[str]:
        return frozenset(symbol for symbol, _ in self._cursors)

    def for_symbol(self, symbol: str) -> dict[str, datetime]:
        normalized = symbol.strip().upper()
        return {
            interval: boundary
            for (item_symbol, interval), boundary in self._cursors.items()
            if item_symbol == normalized
        }

    def evict_symbol(self, symbol: str) -> int:
        return self.evict_symbols({symbol})

    def evict_symbols(self, symbols: set[str]) -> int:
        normalized = {symbol.strip().upper() for symbol in symbols}
        remaining = {
            key: value
            for key, value in self._cursors.items()
            if key[0] not in normalized
        }
        removed = len(self._cursors) - len(remaining)
        if removed:
            self._persist(remaining)
        return removed

    def save(self, symbol: str, interval: str, boundary: datetime) -> None:
        key = (symbol.strip().upper(), interval)
        value = _utc(boundary)
        existing = self._cursors.get(key)
        if existing is not None and value < existing:
            raise BucketCursorError("Bucket cursor cannot move backwards.")
        self._persist({**self._cursors, key: value})

    def _persist(self, updated: dict[tuple[str, str], datetime]) -> None:
        payload = {
            "version": 1,
            "cursors": [
                {
                    "symbol": item[0],
                    "interval": item[1],
                    "completed_at": updated[item].isoformat(),
                }
                for item in sorted(updated)
            ],
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
            raise BucketCursorError("Bucket cursor cannot be saved.") from error
        self._cursors = updated

    def _load(self) -> dict[tuple[str, str], datetime]:
        if not self._path.exists():
            return {}
        try:
            payload: Any = json.loads(self._path.read_text(encoding="utf-8"))
            if payload.get("version") != 1 or not isinstance(payload["cursors"], list):
                raise ValueError
            result: dict[tuple[str, str], datetime] = {}
            for item in payload["cursors"]:
                key = (str(item["symbol"]).upper(), str(item["interval"]))
                if key in result:
                    raise ValueError
                result[key] = _utc(datetime.fromisoformat(str(item["completed_at"])))
            return result
        except (
            OSError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise BucketCursorError("Bucket cursor is invalid.") from error


def interval_duration(interval: str) -> timedelta:
    return {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
    }[interval]


def completed_boundary(now: datetime, interval: str) -> datetime:
    value = _utc(now)
    seconds = int(interval_duration(interval).total_seconds())
    epoch = int(value.timestamp())
    return datetime.fromtimestamp(epoch - epoch % seconds, tz=UTC)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Bucket time must be timezone-aware.")
    return value.astimezone(UTC)
