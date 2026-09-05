from __future__ import annotations

import hashlib
import json
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any

from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine.models import (
    SetupDirection,
    SetupState,
    SetupType,
    TradeEligibility,
)

DEFAULT_QTR_SETUP_NOTIFICATION_STATE_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_setup_notifications.json"
)
STATE_VERSION = 1
QTR_TELEGRAM_QUALITY_IMPROVEMENT = 10.0
QTR_SETUP_NOTIFICATION_STATE_CAPACITY = 10_000
_LOGGER = logging.getLogger(__name__)


class QtrNotificationReason(StrEnum):
    NEW_EPISODE = "new_episode"
    DIRECTION_CHANGED = "direction_changed"
    TYPE_CHANGED = "type_changed"
    QUALITY_IMPROVED = "quality_improved"
    DUPLICATE = "duplicate"
    STATUS_SUPPRESSED = "status_suppressed"
    QUALITY_BELOW_THRESHOLD = "quality_below_threshold"
    DISTANCE_EXCEEDED = "distance_exceeded"
    TECHNICAL_DATA_UNAVAILABLE = "technical_data_unavailable"


@dataclass(frozen=True, slots=True)
class QtrTelegramFilterPolicy:
    minimum_quality: float = 90.0
    maximum_distance_atr: float = 1.2

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.minimum_quality)
            or not 0 <= self.minimum_quality <= 100
        ):
            raise ValueError("QTR Telegram minimum quality must be within 0..100.")
        if (
            not math.isfinite(self.maximum_distance_atr)
            or self.maximum_distance_atr <= 0
        ):
            raise ValueError("QTR Telegram maximum ATR distance must be positive.")


@dataclass(frozen=True, slots=True)
class QtrTelegramFilterMetrics:
    candidates_seen: int
    telegram_quality_passed: int
    suppressed_status: int
    suppressed_quality: int
    suppressed_distance: int
    suppressed_duplicate: int


@dataclass(frozen=True, slots=True)
class QtrSetupEvent:
    candidate: QtrSetupCandidate
    direction: SetupDirection
    setup_type: SetupType
    state: SetupState
    trade_eligibility: TradeEligibility
    current_failure: bool
    retest_held: bool
    visible_confirmations: tuple[str, ...]
    warnings: tuple[str, ...]
    quality_components: Mapping[str, float]
    quality_score: float
    semantic_fingerprint: str

    @property
    def record_key(self) -> str:
        return f"{self.candidate.result.symbol}::{self.candidate.episode_id}"


@dataclass(frozen=True, slots=True)
class QtrSetupNotificationRecord:
    symbol: str
    episode_id: str
    last_semantic_fingerprint: str
    last_state: SetupState
    last_direction: SetupDirection
    last_setup_type: SetupType
    first_sent_at: datetime
    last_sent_at: datetime
    send_count: int
    cancellation_sent: bool
    last_quality_score: float | None


@dataclass(frozen=True, slots=True)
class QtrSetupNotificationState:
    updated_at: datetime | None
    records: Mapping[str, QtrSetupNotificationRecord]


@dataclass(frozen=True, slots=True)
class QtrSetupDecision:
    event: QtrSetupEvent | None
    candidate: QtrSetupCandidate
    should_notify: bool
    reason: QtrNotificationReason

    @property
    def decision_id(self) -> str:
        if self.event is not None:
            return self.event.record_key
        return f"{self.candidate.result.symbol}::{self.candidate.episode_id}"


@dataclass(frozen=True, slots=True)
class QtrSetupNotificationPlan:
    observed_at: datetime
    decisions: tuple[QtrSetupDecision, ...]
    original_records: Mapping[str, QtrSetupNotificationRecord]


class QtrSetupStateError(RuntimeError):
    pass


class JsonQtrSetupNotificationStore:
    def __init__(self, path: Path = DEFAULT_QTR_SETUP_NOTIFICATION_STATE_PATH) -> None:
        self._path = path.resolve()

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> QtrSetupNotificationState:
        if not self._path.exists():
            return QtrSetupNotificationState(None, {})
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return _state_from_json(raw)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            _LOGGER.warning(
                "Состояние QTR Setup Pilot повреждено; используется пустое состояние."
            )
            self._backup_corrupt()
            return QtrSetupNotificationState(None, {})

    def save(self, state: QtrSetupNotificationState) -> None:
        payload = {
            "version": STATE_VERSION,
            "updated_at": _time_json(state.updated_at),
            "records": {
                key: _record_to_json(record)
                for key, record in sorted(state.records.items())
            },
        }
        temporary = self._path.with_suffix(self._path.suffix + ".tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            temporary.replace(self._path)
        except OSError:
            raise QtrSetupStateError(
                "Не удалось сохранить состояние QTR Setup Pilot."
            ) from None

    def _backup_corrupt(self) -> None:
        try:
            backup = self._path.with_suffix(self._path.suffix + ".corrupt")
            self._path.replace(backup)
        except OSError:
            _LOGGER.warning(
                "Не удалось создать резервную копию повреждённого QTR state."
            )


class QtrSetupNotificationService:
    def __init__(
        self,
        store: JsonQtrSetupNotificationStore,
        policy: QtrTelegramFilterPolicy | None = None,
    ) -> None:
        self._store = store
        self._policy = policy or QtrTelegramFilterPolicy()
        self._candidates_seen = 0
        self._telegram_quality_passed = 0
        self._suppressed_status = 0
        self._suppressed_quality = 0
        self._suppressed_distance = 0
        self._suppressed_duplicate = 0

    @property
    def metrics(self) -> QtrTelegramFilterMetrics:
        return QtrTelegramFilterMetrics(
            candidates_seen=self._candidates_seen,
            telegram_quality_passed=self._telegram_quality_passed,
            suppressed_status=self._suppressed_status,
            suppressed_quality=self._suppressed_quality,
            suppressed_distance=self._suppressed_distance,
            suppressed_duplicate=self._suppressed_duplicate,
        )

    def prepare(
        self,
        candidates: tuple[QtrSetupCandidate, ...],
        observed_at: datetime,
    ) -> QtrSetupNotificationPlan:
        now = _utc(observed_at)
        state = self._store.load()
        decisions = tuple(
            _decision(candidate, state.records, self._policy)
            for candidate in candidates
        )
        self._candidates_seen += len(decisions)
        for decision in decisions:
            if decision.should_notify:
                self._telegram_quality_passed += 1
            elif decision.reason is QtrNotificationReason.STATUS_SUPPRESSED:
                self._suppressed_status += 1
            elif decision.reason is QtrNotificationReason.DISTANCE_EXCEEDED:
                self._suppressed_distance += 1
            elif decision.reason is QtrNotificationReason.DUPLICATE:
                self._suppressed_duplicate += 1
            else:
                self._suppressed_quality += 1
        return QtrSetupNotificationPlan(now, decisions, dict(state.records))

    def commit(
        self,
        plan: QtrSetupNotificationPlan,
        delivered_decision_ids: frozenset[str],
    ) -> None:
        allowed = {
            decision.decision_id: decision
            for decision in plan.decisions
            if decision.should_notify and decision.event is not None
        }
        if delivered_decision_ids.difference(allowed):
            raise ValueError("Cannot commit QTR decisions absent from the plan.")
        records = dict(plan.original_records)
        for decision_id in delivered_decision_ids:
            decision = allowed[decision_id]
            event = decision.event
            assert event is not None
            previous = records.get(event.record_key)
            first_sent = (
                previous.first_sent_at if previous is not None else plan.observed_at
            )
            count = (previous.send_count if previous is not None else 0) + 1
            records[event.record_key] = QtrSetupNotificationRecord(
                symbol=event.candidate.result.symbol,
                episode_id=event.candidate.episode_id,
                last_semantic_fingerprint=event.semantic_fingerprint,
                last_state=event.state,
                last_direction=event.direction,
                last_setup_type=event.setup_type,
                first_sent_at=first_sent,
                last_sent_at=plan.observed_at,
                send_count=count,
                cancellation_sent=(
                    event.state is SetupState.CANCELLED
                    or bool(previous and previous.cancellation_sent)
                ),
                last_quality_score=event.quality_score,
            )
        if len(records) > QTR_SETUP_NOTIFICATION_STATE_CAPACITY:
            newest = sorted(
                records.items(),
                key=lambda item: item[1].last_sent_at,
                reverse=True,
            )[:QTR_SETUP_NOTIFICATION_STATE_CAPACITY]
            records = dict(newest)
        self._store.save(QtrSetupNotificationState(plan.observed_at, records))


def event_from_candidate(
    candidate: QtrSetupCandidate,
    *,
    maximum_distance_atr: float = 1.2,
) -> QtrSetupEvent:
    result = candidate.result
    confirmations = _visible_confirmations(candidate)
    warnings = _visible_warnings(candidate)
    quality_components = qtr_telegram_quality_components(
        candidate,
        maximum_distance_atr=maximum_distance_atr,
    )
    quality_score = sum(quality_components.values())
    fingerprint = _fingerprint(
        result.symbol,
        result.direction,
        result.setup_type,
        result.setup_state,
        result.trade_eligibility,
        result.current_breakout_failure,
        result.retest_held,
        confirmations,
        warnings,
    )
    return QtrSetupEvent(
        candidate,
        result.direction,
        result.setup_type,
        result.setup_state,
        result.trade_eligibility,
        result.current_breakout_failure,
        result.retest_held,
        confirmations,
        warnings,
        quality_components,
        quality_score,
        fingerprint,
    )


def _decision(
    candidate: QtrSetupCandidate,
    records: Mapping[str, QtrSetupNotificationRecord],
    policy: QtrTelegramFilterPolicy,
) -> QtrSetupDecision:
    result = candidate.result
    if result.technical_gap or result.missing_data:
        return QtrSetupDecision(
            None, candidate, False, QtrNotificationReason.TECHNICAL_DATA_UNAVAILABLE
        )
    event = event_from_candidate(
        candidate,
        maximum_distance_atr=policy.maximum_distance_atr,
    )
    previous = records.get(event.record_key)
    if (
        event.state is not SetupState.READY_TO_CONSIDER
        or event.trade_eligibility is not TradeEligibility.READY_TO_CONSIDER
        or event.direction is SetupDirection.NEUTRAL
        or event.setup_type is SetupType.NO_TRADE
    ):
        return QtrSetupDecision(
            event,
            candidate,
            False,
            QtrNotificationReason.STATUS_SUPPRESSED,
        )
    distance = result.distance_to_trigger_atr
    if distance is None or abs(distance) > policy.maximum_distance_atr:
        return QtrSetupDecision(
            event,
            candidate,
            False,
            QtrNotificationReason.DISTANCE_EXCEEDED,
        )
    retest_missing = event.setup_type is SetupType.RETEST and not event.retest_held
    if (
        not result.structure_confirmation
        or candidate.source_input.correct_side_of_level is not True
        or retest_missing
        or not result.liquidity_ok
        or not result.spread_ok
        or not result.freshness_confirmation
        or result.current_breakout_failure
        or event.quality_score < policy.minimum_quality
    ):
        return QtrSetupDecision(
            event,
            candidate,
            False,
            QtrNotificationReason.QUALITY_BELOW_THRESHOLD,
        )
    if previous is None:
        return QtrSetupDecision(
            event, candidate, True, QtrNotificationReason.NEW_EPISODE
        )
    if event.direction is not previous.last_direction:
        return QtrSetupDecision(
            event, candidate, True, QtrNotificationReason.DIRECTION_CHANGED
        )
    if event.setup_type is not previous.last_setup_type:
        return QtrSetupDecision(
            event, candidate, True, QtrNotificationReason.TYPE_CHANGED
        )
    previous_quality = previous.last_quality_score
    if (
        previous_quality is not None
        and event.quality_score
        >= previous_quality + QTR_TELEGRAM_QUALITY_IMPROVEMENT
    ):
        return QtrSetupDecision(
            event,
            candidate,
            True,
            QtrNotificationReason.QUALITY_IMPROVED,
        )
    return QtrSetupDecision(
        event,
        candidate,
        False,
        QtrNotificationReason.DUPLICATE,
    )


def qtr_telegram_quality_score(
    candidate: QtrSetupCandidate,
    *,
    maximum_distance_atr: float = 1.2,
) -> float:
    """Return a deterministic Telegram rank, never a win probability."""
    return sum(
        qtr_telegram_quality_components(
            candidate,
            maximum_distance_atr=maximum_distance_atr,
        ).values()
    )


def qtr_telegram_quality_components(
    candidate: QtrSetupCandidate,
    *,
    maximum_distance_atr: float = 1.2,
) -> Mapping[str, float]:
    """Expose the existing score inputs without changing score semantics."""
    result = candidate.result
    source = candidate.source_input
    setup_confirmation = (
        result.retest_held
        if result.setup_type is SetupType.RETEST
        else result.structure_confirmation
    )
    distance_ok = (
        result.distance_to_trigger_atr is not None
        and abs(result.distance_to_trigger_atr) <= maximum_distance_atr
    )
    components = {
        "structure": 20.0 if result.structure_confirmation else 0.0,
        "correct_side": 15.0 if source.correct_side_of_level is True else 0.0,
        "setup_or_retest": 15.0 if setup_confirmation else 0.0,
        "volume": 15.0 if result.volume_confirmation else 0.0,
        "volatility": 10.0 if result.volatility_confirmation else 0.0,
        "liquidity_and_spread": (
            15.0 if result.liquidity_ok and result.spread_ok else 0.0
        ),
        "freshness": 5.0 if result.freshness_confirmation else 0.0,
        "distance": 5.0 if distance_ok else 0.0,
    }
    return MappingProxyType(components)


def _visible_confirmations(candidate: QtrSetupCandidate) -> tuple[str, ...]:
    source = candidate.source_input
    result = candidate.result
    values: list[str] = []
    for enabled, text in (
        (result.structure_confirmation, "Структура подтверждена"),
        (source.correct_side_of_level is True, "Цена удерживает нужную сторону уровня"),
        (result.retest_held, "Ретест уровня удержан"),
        (result.volume_confirmation, "Объём подтверждает движение"),
        (result.volatility_confirmation, "Волатильность подтверждает импульс"),
        (result.liquidity_ok and result.spread_ok, "Ликвидность и спред допустимы"),
        (result.freshness_confirmation, "Конструкция остаётся свежей"),
    ):
        if enabled and text not in values:
            values.append(text)
    return tuple(values[:6])


def _visible_warnings(candidate: QtrSetupCandidate) -> tuple[str, ...]:
    result = candidate.result
    values: list[str] = []
    for enabled, text in (
        (result.current_breakout_failure, "Текущий пробой потерял силу"),
        (result.is_late, "Цена ушла слишком далеко от рабочей области"),
        (not result.spread_ok, "Спред не соответствует условиям готовности"),
        (not result.liquidity_ok, "Ликвидность не подтверждена"),
    ):
        if enabled:
            values.append(text)
    return tuple(values)


def _fingerprint(
    symbol: str,
    direction: SetupDirection,
    setup_type: SetupType,
    state: SetupState,
    eligibility: TradeEligibility,
    current_failure: bool,
    retest_held: bool,
    confirmations: tuple[str, ...],
    warnings: tuple[str, ...],
) -> str:
    payload = {
        "symbol": symbol.strip().upper(),
        "direction": direction.value,
        "setup_type": setup_type.value,
        "state": state.value,
        "trade_eligibility": eligibility.value,
        "current_failure": current_failure,
        "retest_held": retest_held,
        "visible_confirmations": sorted(confirmations),
        "warnings": sorted(warnings),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _record_to_json(record: QtrSetupNotificationRecord) -> dict[str, Any]:
    return {
        "symbol": record.symbol,
        "episode_id": record.episode_id,
        "last_semantic_fingerprint": record.last_semantic_fingerprint,
        "last_state": record.last_state.value,
        "last_direction": record.last_direction.value,
        "last_setup_type": record.last_setup_type.value,
        "first_sent_at": record.first_sent_at.isoformat(),
        "last_sent_at": record.last_sent_at.isoformat(),
        "send_count": record.send_count,
        "cancellation_sent": record.cancellation_sent,
        "last_quality_score": record.last_quality_score,
    }


def _state_from_json(raw: Any) -> QtrSetupNotificationState:
    if not isinstance(raw, dict) or raw.get("version") != STATE_VERSION:
        raise ValueError("Unsupported QTR setup state.")
    raw_records = raw.get("records", {})
    if not isinstance(raw_records, dict):
        raise ValueError("Invalid QTR setup records.")
    records: dict[str, QtrSetupNotificationRecord] = {}
    for key, value in raw_records.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            raise ValueError("Invalid QTR setup record.")
        records[key] = QtrSetupNotificationRecord(
            symbol=str(value["symbol"]),
            episode_id=str(value["episode_id"]),
            last_semantic_fingerprint=str(value["last_semantic_fingerprint"]),
            last_state=SetupState(str(value["last_state"])),
            last_direction=SetupDirection(str(value["last_direction"])),
            last_setup_type=SetupType(str(value["last_setup_type"])),
            first_sent_at=_parse_time(value["first_sent_at"]),
            last_sent_at=_parse_time(value["last_sent_at"]),
            send_count=int(value["send_count"]),
            cancellation_sent=bool(value["cancellation_sent"]),
            last_quality_score=(
                float(value["last_quality_score"])
                if value.get("last_quality_score") is not None
                else None
            ),
        )
    updated = raw.get("updated_at")
    return QtrSetupNotificationState(
        _parse_time(updated) if updated is not None else None, records
    )


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Invalid timestamp.")
    return _utc(datetime.fromisoformat(value))


def _time_json(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("QTR setup timestamp must be timezone-aware.")
    return value.astimezone(UTC)
