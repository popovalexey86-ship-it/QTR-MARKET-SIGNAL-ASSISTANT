from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.qtr_signal_outcome.models import (
    Direction,
    SignalSnapshot,
)

_RECENT_SIGNAL_ID_CAPACITY = 100_000


@dataclass(frozen=True, slots=True)
class SignalSourceStats:
    lines_read: int = 0
    delivered_records: int = 0
    filtered_records: int = 0
    invalid_source_records: int = 0
    duplicate_records: int = 0


class SignalSourceReader:
    """Stream delivered Setup Pilot events without materializing the JSONL."""

    def __init__(self, path: Path) -> None:
        self._path = path.resolve()
        self._stats = SignalSourceStats()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def stats(self) -> SignalSourceStats:
        return self._stats

    def iter_signals(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> Iterator[SignalSnapshot]:
        start = _optional_utc(since)
        end = _optional_utc(until)
        seen: dict[str, None] = {}
        lines = delivered = filtered = invalid = duplicates = 0
        if not self._path.exists():
            self._stats = SignalSourceStats()
            return
        with self._path.open("r", encoding="utf-8") as stream:
            for raw_line in stream:
                lines += 1
                if not raw_line.strip():
                    continue
                try:
                    raw = json.loads(raw_line)
                    if not isinstance(raw, Mapping):
                        raise ValueError
                except (json.JSONDecodeError, ValueError):
                    invalid += 1
                    continue
                if not _was_delivered(raw):
                    filtered += 1
                    continue
                try:
                    signal = _signal(raw)
                except (KeyError, TypeError, ValueError):
                    invalid += 1
                    continue
                if start is not None and signal.signal_timestamp < start:
                    filtered += 1
                    continue
                if end is not None and signal.signal_timestamp >= end:
                    filtered += 1
                    continue
                if signal.signal_id in seen:
                    duplicates += 1
                    continue
                seen[signal.signal_id] = None
                if len(seen) > _RECENT_SIGNAL_ID_CAPACITY:
                    del seen[next(iter(seen))]
                delivered += 1
                yield signal
        self._stats = SignalSourceStats(lines, delivered, filtered, invalid, duplicates)


def _was_delivered(raw: Mapping[str, Any]) -> bool:
    return (
        raw.get("decision") == "send"
        and raw.get("sent") is True
        and raw.get("delivery_committed") is True
    )


def _signal(raw: Mapping[str, Any]) -> SignalSnapshot:
    context = raw["price_context"]
    if not isinstance(context, Mapping):
        raise ValueError("Missing price context.")
    timestamp = _time(raw["timestamp"])
    symbol = _text(raw["symbol"]).upper()
    direction = _direction(raw["direction"])
    setup_type = _text(raw["type"]).upper()
    fingerprint = _text(raw.get("semantic_fingerprint", ""))
    price = _positive(context.get("market_price"))
    atr = _positive(context.get("atr"))
    signal_id = _signal_id(timestamp, symbol, direction, setup_type, fingerprint, price)
    return SignalSnapshot(
        signal_id=signal_id,
        symbol=symbol,
        direction=direction,
        setup_type=setup_type,
        signal_timestamp=timestamp,
        source_observed_at=_time(context["observed_at"]),
        semantic_fingerprint=fingerprint,
        signal_price=price,
        trigger_price=_optional_positive(context.get("trigger_price")),
        invalidation_price=_optional_positive(context.get("invalidation_price")),
        atr=atr,
        setup_confidence=_optional_number(context.get("setup_confidence")),
        telegram_quality_score=_optional_number(raw.get("telegram_quality_score")),
        quality_components=_quality_components(
            raw.get(
                "quality_components",
                raw.get("telegram_quality_components"),
            )
        ),
        volume_confirmation=_optional_bool(context.get("volume_confirmation")),
        volatility_confirmation=_optional_bool(context.get("volatility_confirmation")),
        liquidity_ok=_optional_bool(context.get("liquidity_ok")),
        confirmations=_strings(context.get("confirmations")),
        warnings=_strings(context.get("warnings")),
    )


def _signal_id(
    timestamp: datetime,
    symbol: str,
    direction: Direction,
    setup_type: str,
    fingerprint: str,
    price: float,
) -> str:
    payload = {
        "timestamp": timestamp.isoformat(),
        "symbol": symbol,
        "direction": direction.value,
        "setup_type": setup_type,
        "semantic_fingerprint": fingerprint,
        "signal_price": price,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _direction(value: Any) -> Direction:
    normalized = _text(value).upper()
    if normalized in {"UP", "LONG"}:
        return Direction.LONG
    if normalized in {"DOWN", "SHORT"}:
        return Direction.SHORT
    raise ValueError("Signal direction must be LONG or SHORT.")


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Invalid timestamp.")
    return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware.")
    return value.astimezone(UTC)


def _optional_utc(value: datetime | None) -> datetime | None:
    return _utc(value) if value is not None else None


def _text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Expected non-empty text.")
    return value.strip()


def _positive(value: Any) -> float:
    number = _number(value)
    if number <= 0:
        raise ValueError("Expected positive number.")
    return number


def _optional_positive(value: Any) -> float | None:
    return None if value is None else _positive(value)


def _number(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("Boolean is not numeric.")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Expected finite number.")
    return number


def _optional_number(value: Any) -> float | None:
    return None if value is None else _number(value)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError("Expected boolean.")
    return value


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("Expected string list.")
    return tuple(value)


def _quality_components(value: Any) -> tuple[tuple[str, float], ...]:
    if value is None:
        return ()
    if not isinstance(value, Mapping):
        raise ValueError("Invalid quality components.")
    return tuple(sorted((_text(key), _number(score)) for key, score in value.items()))
