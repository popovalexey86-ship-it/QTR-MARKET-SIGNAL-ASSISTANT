from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.qtr_entry_readiness.models import (
    ENTRY_READINESS_SCHEMA_VERSION as ENTRY_READINESS_SCHEMA_VERSION,
)
from market_signal_assistant.qtr_entry_readiness.models import (
    EntryReadinessEvaluation,
)

DEFAULT_ENTRY_READINESS_AUDIT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_entry_readiness_shadow_audit.jsonl"
)
_LOGGER = logging.getLogger(__name__)


class JsonlEntryReadinessAuditStore:
    """Durable append-only shadow journal; it never reads or rewrites history."""

    def __init__(self, path: Path = DEFAULT_ENTRY_READINESS_AUDIT_PATH) -> None:
        self._path = path.resolve()
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, records: tuple[EntryReadinessEvaluation, ...]) -> None:
        if not records:
            return
        lines = tuple(
            json.dumps(_record_to_json(record), ensure_ascii=False, sort_keys=True)
            for record in records
        )
        with self._lock:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            _ensure_line_boundary(self._path)
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write("\n".join(lines))
                stream.write("\n")


def append_safely(
    store: JsonlEntryReadinessAuditStore,
    records: tuple[EntryReadinessEvaluation, ...],
) -> None:
    try:
        store.append(records)
    except (OSError, TypeError, ValueError):
        _LOGGER.warning("QTR Entry Readiness shadow audit append failed.")


def _ensure_line_boundary(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        return
    with path.open("rb") as stream:
        stream.seek(-1, 2)
        last = stream.read(1)
    if last != b"\n":
        with path.open("ab") as stream:
            stream.write(b"\n")


def _time(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _record_to_json(record: EntryReadinessEvaluation) -> dict[str, Any]:
    return {
        "schema_version": record.schema_version,
        "recorded_at": record.recorded_at.isoformat(),
        "evaluation_id": record.evaluation_id,
        "candidate_id": record.candidate_id,
        "setup_episode_id": record.setup_episode_id,
        "setup_episode_key": record.setup_episode_key,
        "symbol": record.symbol,
        "direction": record.direction,
        "setup_type": record.setup_type,
        "quality_score": record.quality_score,
        "quality_components": dict(record.quality_components),
        "user_readiness": (
            record.user_readiness.value if record.user_readiness is not None else None
        ),
        "wait_reason": record.wait_reason.value if record.wait_reason else None,
        "internal_disposition": record.internal_disposition.value,
        "internal_reason": (
            record.internal_reason.value if record.internal_reason else None
        ),
        "signal_time": _time(record.signal_time),
        "confirmation_time": record.confirmation_time.isoformat(),
        "evaluation_time": record.evaluation_time.isoformat(),
        "fresh_price_time": _time(record.fresh_price_time),
        "signal_age_seconds": record.signal_age_seconds,
        "confirmation_age_seconds": record.confirmation_age_seconds,
        "signal_price": record.signal_price,
        "fresh_price": record.fresh_price,
        "trigger": record.trigger,
        "atr": record.atr,
        "entry_zone_low": record.entry_zone_low,
        "entry_zone_high": record.entry_zone_high,
        "structural_invalidation": record.structural_invalidation,
        "protective_level": record.protective_level,
        "signal_distance_atr": record.signal_distance_atr,
        "fresh_distance_atr": record.fresh_distance_atr,
        "distance_bucket": (
            record.distance_bucket.value if record.distance_bucket else None
        ),
        "risk_distance": record.risk_distance,
        "risk_distance_atr": record.risk_distance_atr,
        "risk_bucket": record.risk_bucket.value if record.risk_bucket else None,
        "age_bucket": record.age_bucket.value,
        "structure_ok": record.structure_ok,
        "confirmation_ok": record.confirmation_ok,
        "retest_held": record.retest_held,
        "breakout_confirmed": record.breakout_confirmed,
        "volume_confirmed": record.volume_confirmed,
        "correct_side": record.correct_side,
        "spread_ok": record.spread_ok,
        "liquidity_ok": record.liquidity_ok,
        "current_failure": record.current_failure,
        "late": record.late,
        "context_ids": list(record.context_ids),
        "previous_user_readiness": (
            record.previous_user_readiness.value
            if record.previous_user_readiness is not None
            else None
        ),
        "transition": record.transition,
        "first_wait_at": _time(record.first_wait_at),
        "first_now_at": _time(record.first_now_at),
        "wait_to_now_seconds": record.wait_to_now_seconds,
    }
