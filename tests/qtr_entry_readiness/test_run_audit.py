from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.qtr_entry_readiness.models import (
    EntryReadinessRunStatus,
    EntryReadinessRunTelemetry,
)
from market_signal_assistant.qtr_entry_readiness.run_audit import (
    JsonlEntryReadinessRunAuditStore,
    append_run_safely,
)


def test_run_audit_is_append_only_and_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "runs.jsonl"
    store = JsonlEntryReadinessRunAuditStore(path)
    record = EntryReadinessRunTelemetry(
        recorded_at=datetime(2026, 9, 15, tzinfo=UTC),
        run_id="run-1",
        status=EntryReadinessRunStatus.COMPLETED,
        candidates_received=2,
        candidates_evaluated=2,
        candidates_suppressed=1,
        prices_received=1,
        prices_missing=1,
        batch_price_latency_ms=12.5,
        total_run_latency_ms=15.0,
    )

    assert append_run_safely(store, record) is True
    assert append_run_safely(store, record) is True

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert lines[0] == lines[1]
    payload = json.loads(lines[0])
    assert payload["record_type"] == "ENTRY_READINESS_RUN"
    assert payload["status"] == "COMPLETED"
    assert payload["candidates_evaluated"] / payload["candidates_received"] == 1


def test_run_audit_failure_is_contained() -> None:
    class FailedStore:
        def append(self, record: EntryReadinessRunTelemetry) -> None:
            del record
            raise OSError("disk unavailable secret-token")

    record = EntryReadinessRunTelemetry(
        recorded_at=datetime(2026, 9, 15, tzinfo=UTC),
        run_id="run-2",
        status=EntryReadinessRunStatus.SKIPPED_BUSY,
        candidates_received=1,
        candidates_evaluated=0,
        candidates_suppressed=0,
        prices_received=0,
        prices_missing=0,
        batch_price_latency_ms=None,
        total_run_latency_ms=None,
    )

    assert append_run_safely(FailedStore(), record) is False
