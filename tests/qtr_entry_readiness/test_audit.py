from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from market_signal_assistant.qtr_entry_readiness.audit import (
    ENTRY_READINESS_SCHEMA_VERSION,
    JsonlEntryReadinessAuditStore,
)
from market_signal_assistant.qtr_entry_readiness.models import UserReadiness

from .test_engine import NOW, candidate, evaluate


def test_audit_is_append_only_and_json_serializable(tmp_path: Path) -> None:
    path = tmp_path / "entry-readiness.jsonl"
    store = JsonlEntryReadinessAuditStore(path)
    waiting = evaluate(candidate(), 100.6)
    ready = evaluate(candidate(), 100.2)

    store.append((waiting,))
    store.append((ready,))

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["schema_version"] == ENTRY_READINESS_SCHEMA_VERSION
    assert rows[0]["user_readiness"] == "WAIT"
    assert rows[1]["user_readiness"] == "NOW"
    assert rows[1]["quality_components"]["structure"] == 20.0
    assert rows[1]["entry_zone_low"] == 100.0
    assert rows[1]["distance_bucket"] == "0.00-0.15_ATR"


def test_audit_payload_contains_no_secret_fields(tmp_path: Path) -> None:
    path = tmp_path / "entry-readiness.jsonl"
    JsonlEntryReadinessAuditStore(path).append((evaluate(candidate(), 100.2),))

    text = path.read_text(encoding="utf-8").lower()

    assert "api_key" not in text
    assert "api_secret" not in text
    assert "token" not in text
    assert "password" not in text


def test_evaluation_id_is_deterministic() -> None:
    first = evaluate(candidate(), 100.2)
    second = evaluate(candidate(), 100.2)

    assert first.evaluation_id == second.evaluation_id


def test_suppressed_record_serializes_null_user_status(tmp_path: Path) -> None:
    path = tmp_path / "entry-readiness.jsonl"
    suppressed = evaluate(candidate(), None)
    JsonlEntryReadinessAuditStore(path).append((suppressed,))

    row = json.loads(path.read_text(encoding="utf-8"))
    assert row["user_readiness"] is None
    assert row["internal_disposition"] == "SUPPRESSED"
    assert row["internal_reason"] == "FRESH_PRICE_MISSING"


def test_transition_fields_are_serialized(tmp_path: Path) -> None:
    path = tmp_path / "entry-readiness.jsonl"
    record = replace(
        evaluate(candidate(), 100.2),
        previous_user_readiness=UserReadiness.WAIT,
        transition="WAIT_TO_NOW",
        first_wait_at=NOW,
        first_now_at=NOW,
        wait_to_now_seconds=0.0,
    )
    JsonlEntryReadinessAuditStore(path).append((record,))

    row = json.loads(path.read_text(encoding="utf-8"))
    assert row["previous_user_readiness"] == "WAIT"
    assert row["transition"] == "WAIT_TO_NOW"
    assert row["wait_to_now_seconds"] == 0.0
