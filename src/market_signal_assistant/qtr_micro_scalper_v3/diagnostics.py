from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from types import MappingProxyType

from market_signal_assistant.qtr_micro_scalper_v3.models import (
    ImpulseDirection,
    V3EntryDecision,
)

DEFAULT_DECISION_DIAGNOSTICS_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_micro_scalper_v3_decisions.jsonl"
)
_MAX_REASON_KEYS = 64


@dataclass(frozen=True, slots=True)
class DecisionDiagnosticSummary:
    window_started_at: datetime | None
    window_ended_at: datetime | None
    snapshots_evaluated: int
    accepted: int
    rejected: int
    long_evaluated: int
    short_evaluated: int
    blocking_reasons: Mapping[str, int]
    spread_min_bps: float | None
    spread_max_bps: float | None
    spread_mean_bps: float | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "blocking_reasons",
            MappingProxyType(dict(self.blocking_reasons)),
        )


class DecisionDiagnostics:
    """Bounded in-memory counters persisted as one compact summary per window."""

    def __init__(
        self,
        path: Path = DEFAULT_DECISION_DIAGNOSTICS_PATH,
        *,
        interval: timedelta = timedelta(seconds=60),
    ) -> None:
        if interval.total_seconds() <= 0:
            raise ValueError("V3 diagnostic interval must be positive.")
        self._path = path.resolve()
        self._interval = interval
        self._lock = Lock()
        self._write_failures = 0
        self._window_started_at: datetime | None = None
        self._window_ended_at: datetime | None = None
        self._evaluated = 0
        self._accepted = 0
        self._rejected = 0
        self._long = 0
        self._short = 0
        self._reasons: dict[str, int] = {}
        self._spread_count = 0
        self._spread_sum = 0.0
        self._spread_min: float | None = None
        self._spread_max: float | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def write_failures(self) -> int:
        return self._write_failures

    def observe(self, decision: V3EntryDecision) -> None:
        with self._lock:
            if (
                self._window_started_at is not None
                and decision.evaluated_at >= self._window_started_at + self._interval
            ):
                self._persist_locked()
                self._reset_locked()
            if self._window_started_at is None:
                self._window_started_at = decision.evaluated_at
            self._window_ended_at = decision.evaluated_at
            self._evaluated += 1
            if decision.accepted:
                self._accepted += 1
            else:
                self._rejected += 1
            if decision.direction is ImpulseDirection.LONG:
                self._long += 1
            elif decision.direction is ImpulseDirection.SHORT:
                self._short += 1
            for reason in decision.blocking_reasons:
                key = reason
                if key not in self._reasons and len(self._reasons) >= _MAX_REASON_KEYS:
                    key = "other"
                self._reasons[key] = self._reasons.get(key, 0) + 1
            spread = decision.snapshot.spread_bps
            self._spread_count += 1
            self._spread_sum += spread
            self._spread_min = (
                spread if self._spread_min is None else min(self._spread_min, spread)
            )
            self._spread_max = (
                spread if self._spread_max is None else max(self._spread_max, spread)
            )

    def snapshot(self) -> DecisionDiagnosticSummary:
        with self._lock:
            return self._summary_locked()

    def flush(self) -> bool:
        with self._lock:
            if self._evaluated == 0:
                return False
            written = self._persist_locked()
            if written:
                self._reset_locked()
            return written

    def _persist_locked(self) -> bool:
        summary = self._summary_locked()
        payload = {
            "window_started_at": _iso(summary.window_started_at),
            "window_ended_at": _iso(summary.window_ended_at),
            "snapshots_evaluated": summary.snapshots_evaluated,
            "accepted": summary.accepted,
            "rejected": summary.rejected,
            "long_evaluated": summary.long_evaluated,
            "short_evaluated": summary.short_evaluated,
            "blocking_reasons": dict(summary.blocking_reasons),
            "spread_min_bps": summary.spread_min_bps,
            "spread_max_bps": summary.spread_max_bps,
            "spread_mean_bps": summary.spread_mean_bps,
        }
        line = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(line)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError:
            self._write_failures += 1
            return False
        return True

    def _summary_locked(self) -> DecisionDiagnosticSummary:
        mean = (
            self._spread_sum / self._spread_count
            if self._spread_count
            else None
        )
        return DecisionDiagnosticSummary(
            window_started_at=self._window_started_at,
            window_ended_at=self._window_ended_at,
            snapshots_evaluated=self._evaluated,
            accepted=self._accepted,
            rejected=self._rejected,
            long_evaluated=self._long,
            short_evaluated=self._short,
            blocking_reasons=self._reasons,
            spread_min_bps=self._spread_min,
            spread_max_bps=self._spread_max,
            spread_mean_bps=mean,
        )

    def _reset_locked(self) -> None:
        self._window_started_at = None
        self._window_ended_at = None
        self._evaluated = 0
        self._accepted = 0
        self._rejected = 0
        self._long = 0
        self._short = 0
        self._reasons = {}
        self._spread_count = 0
        self._spread_sum = 0.0
        self._spread_min = None
        self._spread_max = None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
