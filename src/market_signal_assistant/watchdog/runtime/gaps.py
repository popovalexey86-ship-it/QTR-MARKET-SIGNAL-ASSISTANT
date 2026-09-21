from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.watchdog.journal import ImmutableJsonlJournal


@dataclass(frozen=True, slots=True)
class SchedulingGap:
    symbol: str
    interval: str
    first_missing_boundary: datetime
    last_missing_boundary: datetime
    missing_bucket_count: int
    recorded_at: datetime
    reason: str = "catchup_cap_exceeded"
    safe_backfill: str = "OHLCV_BASELINE_ONLY_AT_RECOVERY_AVAILABILITY"

    def __post_init__(self) -> None:
        times = (
            self.first_missing_boundary,
            self.last_missing_boundary,
            self.recorded_at,
        )
        if any(value.tzinfo is None or value.utcoffset() is None for value in times):
            raise ValueError("Gap timestamps must be timezone-aware.")
        if self.first_missing_boundary > self.last_missing_boundary:
            raise ValueError("Gap boundaries are inconsistent.")
        if self.missing_bucket_count <= 0:
            raise ValueError("Gap must contain at least one bucket.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        for name in (
            "first_missing_boundary",
            "last_missing_boundary",
            "recorded_at",
        ):
            object.__setattr__(self, name, getattr(self, name).astimezone(UTC))

    @property
    def gap_id(self) -> str:
        identity = "|".join(
            (
                self.symbol,
                self.interval,
                self.first_missing_boundary.isoformat(),
                self.last_missing_boundary.isoformat(),
                self.reason,
            )
        )
        return hashlib.sha256(identity.encode()).hexdigest()


class GapLedger:
    def __init__(self, path: Path) -> None:
        self._journal = ImmutableJsonlJournal(path, id_field="gap_id")

    def append(self, gap: SchedulingGap) -> bool:
        return self._journal.append(
            gap.gap_id,
            {
                "gap_id": gap.gap_id,
                "symbol": gap.symbol,
                "interval": gap.interval,
                "first_missing_boundary": gap.first_missing_boundary.isoformat(),
                "last_missing_boundary": gap.last_missing_boundary.isoformat(),
                "missing_bucket_count": gap.missing_bucket_count,
                "recorded_at": gap.recorded_at.isoformat(),
                "reason": gap.reason,
                "safe_backfill": gap.safe_backfill,
            },
        )

    def records(self) -> tuple[dict[str, object], ...]:
        return self._journal.records()

    @property
    def record_count(self) -> int:
        return self._journal.recovery.record_count
