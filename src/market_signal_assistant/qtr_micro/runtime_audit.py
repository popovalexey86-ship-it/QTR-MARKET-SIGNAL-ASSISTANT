from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

DEFAULT_QTR_MICRO_RUNTIME_AUDIT_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_micro_runtime_audit.jsonl"
)
_LOGGER = logging.getLogger(__name__)


class QtrMicroRuntimeEvent(StrEnum):
    ENTRY_REVALIDATION_STARTED = "ENTRY_REVALIDATION_STARTED"
    ENTRY_REVALIDATION_PASSED = "ENTRY_REVALIDATION_PASSED"
    ENTRY_REVALIDATION_REJECTED = "ENTRY_REVALIDATION_REJECTED"
    FRESH_PRICE_LOADED = "FRESH_PRICE_LOADED"
    SIZE_RECALCULATED = "SIZE_RECALCULATED"
    PREPARED = "PREPARED"
    LEVERAGE_READY = "LEVERAGE_READY"
    ENTRY_SUBMIT_ATTEMPT = "ENTRY_SUBMIT_ATTEMPT"
    ENTRY_ACK = "ENTRY_ACK"
    ENTRY_REJECTED = "ENTRY_REJECTED"
    ENTRY_CONFIRMATION_WAIT = "ENTRY_CONFIRMATION_WAIT"
    ENTRY_CONFIRMATION_POLL = "ENTRY_CONFIRMATION_POLL"
    ENTRY_CONFIRMATION_TIMEOUT = "ENTRY_CONFIRMATION_TIMEOUT"
    ENTRY_FILLED = "ENTRY_FILLED"
    ACTUAL_RISK_CHECK = "ACTUAL_RISK_CHECK"
    ACTUAL_RISK_RESIZE = "ACTUAL_RISK_RESIZE"
    ACTUAL_RISK_EXCEEDED = "ACTUAL_RISK_EXCEEDED"
    PROTECTION_ATTEMPT = "PROTECTION_ATTEMPT"
    PROTECTION_FAILED = "PROTECTION_FAILED"
    ENTRY_PROTECTED = "ENTRY_PROTECTED"
    JOURNAL_WRITE = "JOURNAL_WRITE"
    JOURNAL_ALREADY_PRESENT = "JOURNAL_ALREADY_PRESENT"
    ENTRY_ABORTED = "ENTRY_ABORTED"


@dataclass(frozen=True, slots=True)
class QtrMicroRuntimeAuditRecord:
    occurred_at: datetime
    event: QtrMicroRuntimeEvent
    trade_id: str
    symbol: str
    stage: str
    reason: str | None = None
    detail: str | None = None
    ret_code: int | None = None


class JsonlQtrMicroRuntimeAudit:
    """Best-effort transition audit without API secrets or raw payloads."""

    def __init__(
        self,
        path: Path = DEFAULT_QTR_MICRO_RUNTIME_AUDIT_PATH,
    ) -> None:
        self._path = path.resolve()
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, record: QtrMicroRuntimeAuditRecord) -> None:
        timestamp = record.occurred_at
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("Runtime audit timestamp must be timezone-aware.")
        payload = asdict(record)
        payload["occurred_at"] = timestamp.astimezone(UTC).isoformat()
        payload["event"] = record.event.value
        try:
            with self._lock:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as stream:
                    stream.write(
                        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
                    )
        except OSError:
            _LOGGER.warning("Не удалось записать QTR Micro runtime audit.")
