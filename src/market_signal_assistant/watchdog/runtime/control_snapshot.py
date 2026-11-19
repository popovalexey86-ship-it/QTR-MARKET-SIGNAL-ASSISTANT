"""Bounded, atomic read model for the optional Telegram control panel."""

from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

from market_signal_assistant.watchdog.engine import WatchdogDetectionResult
from market_signal_assistant.watchdog.universe import UniverseEntry, UniverseSnapshot

SCHEMA_VERSION = 1
MAX_SYMBOLS = 2048
MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024
FEATURES = (
    "price_change_1",
    "volume_acceleration_3",
    "open_interest_change",
    "funding_rate",
)


class ControlSnapshotError(ValueError):
    """The panel read model is absent or cannot be trusted."""


class ControlSnapshotPublisher:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = Lock()
        self._rows: dict[str, dict[str, object]] = {}

    def capture(
        self,
        entry: UniverseEntry,
        result: WatchdogDetectionResult,
        *,
        close: float | None,
    ) -> None:
        symbol = entry.instrument.symbol
        features = {
            item.name: item.value
            for item in result.snapshot.features
            if item.name in FEATURES and math.isfinite(item.value)
        }
        event = result.event
        row: dict[str, object] = {
            "symbol": symbol,
            "tier": entry.tier.value,
            "state": result.state.state.value,
            "score": result.aggregation.anomaly_score,
            "anomaly_types": [
                item.value
                for item in dict.fromkeys(
                    anomaly.anomaly_type for anomaly in event.anomalies
                )
            ]
            if event is not None
            else [],
            "event_id": event.event_id if event is not None else None,
            "detected_at": result.snapshot.detected_at.isoformat(),
            "observed_at": result.snapshot.observed_at.isoformat(),
            "available_at": result.snapshot.available_at.isoformat(),
            "interval": result.snapshot.interval,
            "baseline_ready": entry.baseline_samples >= 20,
            "baseline_samples": entry.baseline_samples,
            "close": close,
            "features": features,
        }
        with self._lock:
            self._rows.pop(symbol, None)
            self._rows[symbol] = row
            if len(self._rows) > MAX_SYMBOLS:
                # The newest evaluated symbols survive; total retained history
                # cannot grow with lifetime or an unexpectedly large catalog.
                oldest = next(iter(self._rows))
                del self._rows[oldest]

    def publish(self, universe: UniverseSnapshot, *, now: datetime) -> None:
        eligible = sorted(item.instrument.symbol for item in universe.eligible)
        eligible_set = set(eligible)
        with self._lock:
            self._rows = {
                symbol: row
                for symbol, row in self._rows.items()
                if symbol in eligible_set
            }
            rows = [self._rows[symbol] for symbol in sorted(self._rows)]
        rejected = [
            {"symbol": symbol, "reasons": list(reasons)}
            for symbol, reasons in universe.rejected[:MAX_SYMBOLS]
        ]
        payload = {
            "version": SCHEMA_VERSION,
            "published_at": now.astimezone(UTC).isoformat(),
            "universe_observed_at": universe.observed_at.isoformat(),
            "eligible_count": len(eligible),
            "eligible_symbols": eligible[:MAX_SYMBOLS],
            "eligible_truncated": max(0, len(eligible) - MAX_SYMBOLS),
            "rejected": rejected,
            "rejected_truncated": max(0, len(universe.rejected) - MAX_SYMBOLS),
            "symbols": rows,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        if len(encoded) > MAX_SNAPSHOT_BYTES:
            raise ControlSnapshotError("Control snapshot exceeds byte limit")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=self.path.parent, prefix=".control-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @property
    def retained_count(self) -> int:
        with self._lock:
            return len(self._rows)


def load_control_snapshot(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if path.stat().st_size > MAX_SNAPSHOT_BYTES:
        raise ControlSnapshotError("Control snapshot exceeds byte limit")
    try:
        with path.open("rb") as stream:
            raw = stream.read(MAX_SNAPSHOT_BYTES + 1)
        if len(raw) > MAX_SNAPSHOT_BYTES:
            raise ValueError
        payload = json.loads(raw)
        if (
            not isinstance(payload, dict)
            or payload.get("version") != SCHEMA_VERSION
            or not isinstance(payload.get("symbols"), list)
            or len(payload["symbols"]) > MAX_SYMBOLS
            or not isinstance(payload.get("eligible_symbols"), list)
            or len(payload["eligible_symbols"]) > MAX_SYMBOLS
            or not isinstance(payload.get("rejected"), list)
            or len(payload["rejected"]) > MAX_SYMBOLS
            or not isinstance(payload.get("eligible_count"), int)
            or payload["eligible_count"] < 0
        ):
            raise ValueError
        for symbol in payload["eligible_symbols"]:
            if not isinstance(symbol, str) or len(symbol) > 32:
                raise ValueError
        for item in payload["rejected"]:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("symbol"), str)
                or not isinstance(item.get("reasons"), list)
            ):
                raise ValueError
        for row in payload["symbols"]:
            if (
                not isinstance(row, dict)
                or not isinstance(row.get("symbol"), str)
                or len(row["symbol"]) > 32
                or not isinstance(row.get("score"), (int, float))
                or isinstance(row["score"], bool)
                or not math.isfinite(row["score"])
                or not isinstance(row.get("anomaly_types"), list)
                or len(row["anomaly_types"]) > 16
                or not isinstance(row.get("features"), dict)
                or len(row["features"]) > len(FEATURES)
            ):
                raise ValueError
            for key in ("detected_at", "observed_at", "available_at"):
                stamp = datetime.fromisoformat(row[key])
                if stamp.tzinfo is None or stamp.utcoffset() is None:
                    raise ValueError
        published = datetime.fromisoformat(payload["published_at"])
        if published.tzinfo is None or published.utcoffset() is None:
            raise ValueError
        return payload
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise ControlSnapshotError("Control snapshot is invalid") from None
