"""Rebuildable streaming trade index; JSONL alone is the durable authority.

Python retention is O(distinct trades), not O(historical updates). Exact
record-ID membership lives in a temporary SQLite B-tree with a bounded page
cache. No persistent checkpoint is trusted, and no source bytes are repaired.
"""

from __future__ import annotations

import os
import sqlite3
import weakref
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock

from market_signal_assistant.qtr_micro_scalper.shadow_decision import ShadowTradeStage
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowJournalRecovery,
    ShadowTradeRecord,
    deserialize_shadow_trade_record,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEventType,
)

_WARNING_LIMIT = 100
_ANCHOR_BYTES = 4096
_STAGE_ORDER = {
    ShadowTradeStage.WAITING_ENTRY: 0,
    ShadowTradeStage.OPEN: 1,
    ShadowTradeStage.TP1_HIT: 2,
    ShadowTradeStage.CLOSED: 3,
    ShadowTradeStage.EXPIRED: 3,
}


def record_order(record: ShadowTradeRecord) -> tuple[datetime, int, str]:
    return record.recorded_at, _STAGE_ORDER[record.stage], record.record_id


@dataclass(frozen=True, slots=True)
class ShadowTradeState:
    first: ShadowTradeRecord
    latest: ShadowTradeRecord
    last_appended: ShadowTradeRecord
    entered: bool
    tp1_stage_seen: bool
    event_types: frozenset[ShadowRuntimeEventType]
    stop_reason_seen: bool

    @classmethod
    def consume(
        cls, record: ShadowTradeRecord, previous: ShadowTradeState | None
    ) -> ShadowTradeState:
        entered = record.entry_time is not None or record.stage in {
            ShadowTradeStage.OPEN,
            ShadowTradeStage.TP1_HIT,
            ShadowTradeStage.CLOSED,
        }
        stop_reason = any(
            "stop" in text.casefold() or "стоп" in text.casefold()
            for text in (*record.reasons, *record.warnings)
        )
        events = frozenset(event.event_type for event in record.events)
        return cls(
            first=(
                record
                if previous is None
                else min(previous.first, record, key=record_order)
            ),
            latest=(
                record
                if previous is None
                else max(previous.latest, record, key=record_order)
            ),
            last_appended=record,
            entered=entered or (previous is not None and previous.entered),
            tp1_stage_seen=(
                record.stage is ShadowTradeStage.TP1_HIT
                or (previous is not None and previous.tp1_stage_seen)
            ),
            event_types=events if previous is None else previous.event_types | events,
            stop_reason_seen=stop_reason
            or (previous is not None and previous.stop_reason_seen),
        )


@dataclass(frozen=True, slots=True)
class ShadowTradeIndexSnapshot:
    record_count: int
    states: tuple[ShadowTradeState, ...]
    warnings: tuple[str, ...]

    @property
    def active_records(self) -> tuple[ShadowTradeRecord, ...]:
        return tuple(
            state.last_appended
            for state in self.states
            if state.last_appended.stage
            not in {ShadowTradeStage.CLOSED, ShadowTradeStage.EXPIRED}
        )


@dataclass(frozen=True, slots=True)
class ShadowTradeIndexMetrics:
    bootstrap_scans: int
    incremental_reads: int
    bytes_read: int
    records_processed: int
    malformed_lines: int
    resets_rotations: int
    file_offset: int
    retained_trades: int
    sqlite_cache_kib: int = 2048


class ShadowTradeJournalIndex:
    """One streaming bootstrap, then complete-line tails; exact disk-backed IDs.

    The private temporary database is deleted when closed. Losing it cannot
    lose source history: every new instance rebuilds from JSONL. SQLite page
    spilling is enabled and mmap disabled; Python never owns an all-ID set.
    """

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self._lock = Lock()
        self._seen = sqlite3.connect("", check_same_thread=False)
        self._seen.execute("PRAGMA cache_size=-2048")
        self._seen.execute("PRAGMA cache_spill=ON")
        self._seen.execute("PRAGMA mmap_size=0")
        self._seen.execute("PRAGMA temp_store=FILE")
        self._seen.execute("PRAGMA user_version=1")
        self._seen.execute("CREATE TABLE seen (id BLOB PRIMARY KEY) WITHOUT ROWID")
        self._close = weakref.finalize(self, self._seen.close)
        self._states: dict[str, ShadowTradeState] = {}
        self._record_count = 0
        self._offset = 0
        self._line_number = 0
        self._signature: tuple[int, int, int, int, int] | None = None
        self._prefix = b""
        self._boundary = b""
        self._corrupted: list[int] = []
        self._warnings: list[str] = []
        self._bootstrap_scans = self._incremental_reads = self._bytes_read = 0
        self._records_processed = self._malformed_lines = self._resets = 0
        self._failed = False

    def close(self) -> None:
        with self._lock:
            self._close()
            self._failed = True

    def _check(self) -> None:
        if self._failed:
            raise RuntimeError("Shadow trade index unavailable; rebuild from JSONL.")

    @property
    def record_count(self) -> int:
        with self._lock:
            self._check()
            return self._record_count

    def contains(self, record_id: str) -> bool:
        with self._lock:
            self._check()
            return (
                self._seen.execute(
                    "SELECT 1 FROM seen WHERE id=?", (bytes.fromhex(record_id),)
                ).fetchone()
                is not None
            )

    def latest(self, trade_id: str) -> ShadowTradeRecord | None:
        with self._lock:
            self._check()
            state = self._states.get(trade_id)
            return state.last_appended if state is not None else None

    @property
    def recovery(self) -> ShadowJournalRecovery:
        with self._lock:
            self._check()
            return ShadowJournalRecovery(
                (), tuple(self._corrupted), tuple(self._warnings)
            )

    def snapshot(self) -> ShadowTradeIndexSnapshot:
        with self._lock:
            self._check()
            return ShadowTradeIndexSnapshot(
                self._record_count,
                tuple(self._states[key] for key in sorted(self._states)),
                tuple(self._warnings),
            )

    def metrics(self) -> ShadowTradeIndexMetrics:
        with self._lock:
            return ShadowTradeIndexMetrics(
                self._bootstrap_scans,
                self._incremental_reads,
                self._bytes_read,
                self._records_processed,
                self._malformed_lines,
                self._resets,
                self._offset,
                len(self._states),
            )

    def refresh(self) -> int:
        with self._lock:
            self._check()
            try:
                return self._refresh_locked()
            except Exception:
                self._failed = True  # never serve a partially consumed failed index
                raise

    def _refresh_locked(self) -> int:
        try:
            stream = self.path.open("rb")
        except FileNotFoundError:
            if self._signature is not None:
                self._reset()
            return 0
        with stream:
            stat = os.fstat(stream.fileno())
            signature = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            )
            if signature == self._signature:
                return 0
            if self._signature is not None:
                replaced = signature[:2] != self._signature[:2]
                truncated = stat.st_size < self._offset
                # Bounded anchors also detect copy-truncate/regrowth between polls.
                stream.seek(0)
                prefix_matches = stream.read(len(self._prefix)) == self._prefix
                stream.seek(max(0, self._offset - len(self._boundary)))
                boundary_matches = stream.read(len(self._boundary)) == self._boundary
                rewritten = (
                    stat.st_size == self._signature[2]
                    and signature[3:] != self._signature[3:]
                )
                if (
                    replaced
                    or truncated
                    or not prefix_matches
                    or not boundary_matches
                    or rewritten
                ):
                    self._reset()
            if self._signature is None:
                self._bootstrap_scans += 1
            elif stat.st_size > self._offset:
                self._incremental_reads += 1
            self._signature = signature
            accepted = 0
            stream.seek(self._offset)
            while stream.tell() < stat.st_size:
                start = stream.tell()
                raw = stream.readline(stat.st_size - start)
                self._bytes_read += len(raw)
                if not raw.endswith(b"\n"):
                    break  # leave partial tail unconsumed until newline arrives
                self._line_number += 1
                if raw.strip():
                    try:
                        record = deserialize_shadow_trade_record(raw.decode("utf8"))
                    except (UnicodeError, ValueError) as error:
                        self._malformed_lines += 1
                        if len(self._warnings) < _WARNING_LIMIT:
                            self._corrupted.append(self._line_number)
                            self._warnings.append(
                                f"Skipped corrupted shadow journal line "
                                f"{self._line_number}: {error}"
                            )
                    else:
                        inserted = self._seen.execute(
                            "INSERT OR IGNORE INTO seen VALUES (?)",
                            (bytes.fromhex(record.record_id),),
                        ).rowcount
                        if inserted:
                            self._states[record.trade_id] = ShadowTradeState.consume(
                                record, self._states.get(record.trade_id)
                            )
                            self._record_count += 1
                            self._records_processed += 1
                            accepted += 1
                self._offset = stream.tell()
            self._seen.commit()
            stream.seek(0)
            self._prefix = stream.read(min(_ANCHOR_BYTES, self._offset))
            stream.seek(max(0, self._offset - _ANCHOR_BYTES))
            self._boundary = stream.read(min(_ANCHOR_BYTES, self._offset))
            return accepted

    def _reset(self) -> None:
        self._seen.execute("DELETE FROM seen")
        self._seen.commit()
        self._states.clear()
        self._record_count = self._offset = self._line_number = 0
        self._signature = None
        self._prefix = self._boundary = b""
        self._corrupted.clear()
        self._warnings.clear()
        self._resets += 1
