from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Protocol

from market_signal_assistant.qtr_entry_readiness.models import (
    EntryReadinessRunTelemetry,
)

ENTRY_READINESS_RUN_SCHEMA_VERSION = 1
DEFAULT_ENTRY_READINESS_RUN_AUDIT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_entry_readiness_shadow_runs.jsonl"
)
_LOGGER = logging.getLogger(__name__)


class EntryReadinessRunAuditWriter(Protocol):
    def append(self, record: EntryReadinessRunTelemetry) -> None: ...


class JsonlEntryReadinessRunAuditStore:
    """Append-only aggregate telemetry isolated from candidate observations."""

    def __init__(
        self, path: Path = DEFAULT_ENTRY_READINESS_RUN_AUDIT_PATH
    ) -> None:
        self._path = path.resolve()
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, record: EntryReadinessRunTelemetry) -> None:
        line = json.dumps(
            _run_record_to_json(record),
            ensure_ascii=False,
            sort_keys=True,
        )
        with self._lock:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            _ensure_line_boundary(self._path)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(line)
                stream.write("\n")


def append_run_safely(
    store: EntryReadinessRunAuditWriter,
    record: EntryReadinessRunTelemetry,
) -> bool:
    try:
        store.append(record)
    except Exception as error:
        _LOGGER.warning(
            "QTR Entry Readiness run audit append failed (%s).",
            type(error).__name__,
        )
        return False
    return True


def _ensure_line_boundary(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        return
    with path.open("rb") as stream:
        stream.seek(-1, 2)
        last = stream.read(1)
    if last != b"\n":
        with path.open("ab") as stream:
            stream.write(b"\n")


def _run_record_to_json(record: EntryReadinessRunTelemetry) -> dict[str, Any]:
    return {
        "record_type": "ENTRY_READINESS_RUN",
        "schema_version": ENTRY_READINESS_RUN_SCHEMA_VERSION,
        "recorded_at": record.recorded_at.isoformat(),
        "run_id": record.run_id,
        "status": record.status.value,
        "candidates_received": record.candidates_received,
        "candidates_evaluated": record.candidates_evaluated,
        "candidates_suppressed": record.candidates_suppressed,
        "prices_received": record.prices_received,
        "prices_missing": record.prices_missing,
        "batch_price_latency_ms": record.batch_price_latency_ms,
        "total_run_latency_ms": record.total_run_latency_ms,
        "error_type": record.error_type,
    }
