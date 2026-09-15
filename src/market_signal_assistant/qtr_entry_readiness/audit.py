from __future__ import annotations

import json
import logging
import threading
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_signal_assistant.qtr_entry_readiness.models import (
    ENTRY_READINESS_SCHEMA_VERSION as ENTRY_READINESS_SCHEMA_VERSION,
)
from market_signal_assistant.qtr_entry_readiness.models import (
    EntryReadinessEpisodeState,
    EntryReadinessEvaluation,
    UserReadiness,
)

DEFAULT_ENTRY_READINESS_AUDIT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_entry_readiness_shadow_audit.jsonl"
)
_LOGGER = logging.getLogger(__name__)


class JsonlEntryReadinessAuditStore:
    """Append-only journal with streaming, bounded transition recovery."""

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

    def recover_episode_states(
        self, *, capacity: int
    ) -> tuple[EntryReadinessEpisodeState, ...]:
        """Stream bounded transition state without materializing JSONL history."""
        if capacity <= 0:
            raise ValueError("Entry-readiness state capacity must be positive.")
        recovered: OrderedDict[str, EntryReadinessEpisodeState] = OrderedDict()
        try:
            with self._path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    if not line.endswith("\n"):
                        continue
                    state = _episode_state_from_line(line, recovered)
                    if state is None:
                        continue
                    recovered[state.setup_episode_key] = state
                    recovered.move_to_end(state.setup_episode_key)
                    while len(recovered) > capacity:
                        recovered.popitem(last=False)
        except FileNotFoundError:
            return ()
        except OSError:
            _LOGGER.warning("QTR Entry Readiness shadow audit recovery failed.")
            return ()
        return tuple(recovered.values())


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
        "first_confirmation_observed_at": _time(
            record.first_confirmation_observed_at
        ),
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
        "age_bucket": record.age_bucket.value if record.age_bucket else None,
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


def _episode_state_from_line(
    line: str,
    recovered: OrderedDict[str, EntryReadinessEpisodeState],
) -> EntryReadinessEpisodeState | None:
    try:
        raw = json.loads(line)
        if not isinstance(raw, dict):
            return None
        key = str(raw["setup_episode_key"]).strip()
        if not key:
            return None
        previous = recovered.get(key)
        readiness = _readiness(raw.get("user_readiness"))
        latest = readiness if readiness is not None else (
            previous.latest_readiness if previous is not None else None
        )
        first_wait = _first_time(
            raw,
            "first_wait_at",
            previous.first_wait_at if previous is not None else None,
        )
        first_now = _first_time(
            raw,
            "first_now_at",
            previous.first_now_at if previous is not None else None,
        )
        confirmation = _first_time(
            raw,
            "first_confirmation_observed_at",
            (
                previous.first_confirmation_observed_at
                if previous is not None
                else None
            ),
        )
        if latest is None and confirmation is None:
            return None
        return EntryReadinessEpisodeState(
            setup_episode_key=key,
            latest_readiness=latest,
            first_wait_at=first_wait,
            first_now_at=first_now,
            first_confirmation_observed_at=confirmation,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _readiness(value: object) -> UserReadiness | None:
    try:
        return UserReadiness(str(value)) if value is not None else None
    except ValueError:
        return None


def _first_time(
    raw: dict[str, Any],
    key: str,
    previous: datetime | None,
) -> datetime | None:
    if previous is not None:
        return previous
    value = raw.get(key)
    if not isinstance(value, str):
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)
