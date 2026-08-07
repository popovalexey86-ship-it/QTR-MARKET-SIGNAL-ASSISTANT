from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from market_signal_assistant.inplay.models import InPlayDirection, InPlayResult
from market_signal_assistant.inplay.safety import (
    AutomaticDisplayStatus,
    AutomaticInPlaySemantics,
    AutomaticRiskClass,
    automatic_semantics,
    visible_confirmations,
)

DEFAULT_INPLAY_NOTIFICATION_STATE_PATH = (
    Path(__file__).resolve().parents[3] / "data" / "inplay_notifications.json"
)
NOTIFICATION_COOLDOWN = timedelta(hours=6)
MINIMUM_SEMANTIC_COOLDOWN = timedelta(minutes=60)
REAPPEARANCE_WINDOW = timedelta(minutes=60)
MINIMUM_SCORE_INCREASE = 10.0

_LOGGER = logging.getLogger(__name__)
_DIRECTIONAL = frozenset((InPlayDirection.LONG, InPlayDirection.SHORT))
_NUMBER = re.compile(r"[+-]?\d+(?:[.,]\d+)?")
_NON_WORD = re.compile(r"[^\w]+", flags=re.UNICODE)


class NotificationStateError(RuntimeError):
    """Controlled failure while persisting local notification state."""


class NotificationDecisionReason(Enum):
    FIRST_APPEARANCE = "first_appearance"
    NEW_LISTING = "new_listing"
    REAPPEARED = "reappeared"
    DIRECTION_CHANGED = "direction_changed"
    DIRECTION_CONFIRMED = "direction_confirmed"
    SCORE_INCREASED = "score_increased"
    IMPORTANT_CONFIRMATION = "important_confirmation"
    COOLDOWN_EXPIRED = "cooldown_expired"
    UNCHANGED = "unchanged"
    REAPPEARED_TOO_SOON = "reappeared_too_soon"
    DIRECTION_WEAKENED = "direction_weakened"
    MINIMUM_COOLDOWN = "minimum_cooldown"
    SAFETY_DOWNGRADE = "safety_downgrade"


@dataclass(frozen=True, slots=True)
class SentNotification:
    symbol: str
    internal_direction: InPlayDirection | None
    display_status: AutomaticDisplayStatus
    risk_class: AutomaticRiskClass
    user_action: str
    visible_confirmations: tuple[str, ...]
    semantic_fingerprint: str
    inplay_score: float
    sent_at: datetime
    is_new_listing: bool
    reasons_fingerprint: str
    important_confirmations: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("Notification symbol cannot be empty.")
        if not math.isfinite(self.inplay_score) or not 0 <= self.inplay_score <= 100:
            raise ValueError("Notification score must be between 0 and 100.")
        if self.sent_at.tzinfo is None or self.sent_at.utcoffset() is None:
            raise ValueError("Notification timestamp must be timezone-aware.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "sent_at", self.sent_at.astimezone(UTC))
        if not self.user_action.strip():
            raise ValueError("Notification user action cannot be empty.")
        if not self.semantic_fingerprint.strip():
            raise ValueError("Notification semantic fingerprint cannot be empty.")

    @property
    def direction(self) -> InPlayDirection | None:
        """Compatibility alias; new code uses ``internal_direction``."""
        return self.internal_direction


@dataclass(frozen=True, slots=True)
class NotificationRecord:
    symbol: str
    current_internal_direction: InPlayDirection | None
    current_display_status: AutomaticDisplayStatus
    last_notification: SentNotification
    new_listing_notified: bool
    absent_since: datetime | None = None

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol or symbol != self.last_notification.symbol:
            raise ValueError("Notification record symbol is invalid.")
        object.__setattr__(self, "symbol", symbol)
        if self.absent_since is not None:
            if (
                self.absent_since.tzinfo is None
                or self.absent_since.utcoffset() is None
            ):
                raise ValueError("Absence timestamp must be timezone-aware.")
            object.__setattr__(self, "absent_since", self.absent_since.astimezone(UTC))

    @property
    def current_direction(self) -> InPlayDirection | None:
        """Compatibility alias; new code uses ``current_internal_direction``."""
        return self.current_internal_direction


@dataclass(frozen=True, slots=True)
class NotificationState:
    updated_at: datetime | None
    records: Mapping[str, NotificationRecord]

    def __post_init__(self) -> None:
        if self.updated_at is not None:
            if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
                raise ValueError("State timestamp must be timezone-aware.")
            object.__setattr__(self, "updated_at", self.updated_at.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class NotificationDecision:
    result: InPlayResult
    should_notify: bool
    reason: NotificationDecisionReason


@dataclass(frozen=True, slots=True)
class NotificationPlan:
    observed_at: datetime
    decisions: tuple[NotificationDecision, ...]
    original_records: Mapping[str, NotificationRecord]
    passive_records: Mapping[str, NotificationRecord]


class JsonInPlayNotificationStore:
    """Atomic JSON persistence isolated from the listing-discovery snapshot."""

    def __init__(self, path: Path = DEFAULT_INPLAY_NOTIFICATION_STATE_PATH) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> NotificationState:
        if not self._path.exists():
            empty = NotificationState(None, {})
            self.save(empty)
            return empty
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return _state_from_json(raw)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            _LOGGER.warning(
                "Файл состояния IN PLAY-уведомлений повреждён; "
                "используется безопасное пустое состояние."
            )
            empty = NotificationState(None, {})
            try:
                self.save(empty)
            except NotificationStateError:
                _LOGGER.warning(
                    "Не удалось перезаписать повреждённый файл состояния "
                    "IN PLAY-уведомлений."
                )
            return empty

    def save(self, state: NotificationState) -> None:
        payload = {
            "version": 2,
            "updated_at": (
                state.updated_at.isoformat() if state.updated_at is not None else None
            ),
            "records": {
                symbol: _record_to_json(record)
                for symbol, record in sorted(state.records.items())
            },
        }
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(self._path)
        except OSError:
            raise NotificationStateError(
                "Local IN PLAY notification state cannot be saved."
            ) from None


class InPlayNotificationService:
    """Decide which current IN PLAY results merit an automatic notification.

    This service has no scheduler or Telegram dependency. Calling ``evaluate``
    is the explicit lifecycle boundary that reads and updates local state.
    """

    def __init__(self, store: JsonInPlayNotificationStore) -> None:
        self._store = store

    def evaluate(
        self,
        results: tuple[InPlayResult, ...],
        observed_at: datetime,
    ) -> tuple[NotificationDecision, ...]:
        plan = self.prepare(results, observed_at)
        notified_symbols = frozenset(
            decision.result.symbol.strip().upper()
            for decision in plan.decisions
            if decision.should_notify
        )
        self.commit(plan, notified_symbols)
        return plan.decisions

    def prepare(
        self,
        results: tuple[InPlayResult, ...],
        observed_at: datetime,
    ) -> NotificationPlan:
        """Build decisions without marking allowed notifications as sent."""
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("Notification observation time must be timezone-aware.")
        now = observed_at.astimezone(UTC)
        current = _unique_results(results)
        current_symbols = {item.symbol.strip().upper() for item in current}
        state = self._store.load()
        original_records = dict(state.records)
        passive_records = dict(state.records)

        for symbol, record in tuple(passive_records.items()):
            if symbol not in current_symbols and record.absent_since is None:
                passive_records[symbol] = replace(record, absent_since=now)

        decisions: list[NotificationDecision] = []
        for item in current:
            symbol = item.symbol.strip().upper()
            previous = original_records.get(symbol)
            should_notify, reason = _decision(item, previous, now)
            decisions.append(NotificationDecision(item, should_notify, reason))
            if not should_notify and previous is not None:
                passive_records[symbol] = _updated_record(
                    item,
                    previous,
                    now,
                    should_notify=False,
                )

        return NotificationPlan(
            observed_at=now,
            decisions=tuple(decisions),
            original_records=original_records,
            passive_records=passive_records,
        )

    def commit(
        self,
        plan: NotificationPlan,
        notified_symbols: frozenset[str],
    ) -> None:
        """Persist passive transitions and only acknowledged notifications."""
        normalized_symbols = frozenset(
            symbol.strip().upper() for symbol in notified_symbols
        )
        allowed = {
            decision.result.symbol.strip().upper(): decision
            for decision in plan.decisions
            if decision.should_notify
        }
        unknown = normalized_symbols.difference(allowed)
        if unknown:
            raise ValueError("Cannot commit notifications absent from the plan.")
        records = dict(plan.passive_records)
        for symbol in normalized_symbols:
            decision = allowed[symbol]
            records[symbol] = _updated_record(
                decision.result,
                plan.original_records.get(symbol),
                plan.observed_at,
                should_notify=True,
            )
        self._store.save(NotificationState(plan.observed_at, records))


def _decision(
    result: InPlayResult,
    record: NotificationRecord | None,
    now: datetime,
) -> tuple[bool, NotificationDecisionReason]:
    current = automatic_semantics(result)
    if record is None:
        reason = (
            NotificationDecisionReason.NEW_LISTING
            if result.is_new_listing
            else NotificationDecisionReason.FIRST_APPEARANCE
        )
        return True, reason

    last = record.last_notification
    same_semantics = _same_semantics(current, last)
    if (
        current.display_status is AutomaticDisplayStatus.DO_NOT_CHASE
        and same_semantics
    ):
        return False, NotificationDecisionReason.UNCHANGED

    if record.absent_since is not None:
        absence = max(timedelta(), now - record.absent_since)
        if absence >= REAPPEARANCE_WINDOW:
            return True, NotificationDecisionReason.REAPPEARED
        return False, NotificationDecisionReason.REAPPEARED_TOO_SOON

    elapsed = max(timedelta(), now - last.sent_at)
    transitioned_to_extreme = (
        current.display_status is AutomaticDisplayStatus.DO_NOT_CHASE
        and last.display_status is not AutomaticDisplayStatus.DO_NOT_CHASE
    )
    direction_changed = (
        record.current_internal_direction in _DIRECTIONAL
        and current.internal_direction in _DIRECTIONAL
        and record.current_internal_direction is not current.internal_direction
        and current.directional_entry_allowed
    )
    direction_confirmed = (
        record.current_internal_direction is InPlayDirection.WATCH
        and current.internal_direction in _DIRECTIONAL
        and current.directional_entry_allowed
    )
    new_listing = result.is_new_listing and not record.new_listing_notified

    if elapsed < MINIMUM_SEMANTIC_COOLDOWN:
        if transitioned_to_extreme:
            return True, NotificationDecisionReason.SAFETY_DOWNGRADE
        if direction_changed:
            return True, NotificationDecisionReason.DIRECTION_CHANGED
        if direction_confirmed:
            return True, NotificationDecisionReason.DIRECTION_CONFIRMED
        if new_listing:
            return True, NotificationDecisionReason.NEW_LISTING
        if (
            same_semantics
            and abs(result.inplay_score - last.inplay_score) < 1e-12
        ):
            return False, NotificationDecisionReason.UNCHANGED
        return False, NotificationDecisionReason.MINIMUM_COOLDOWN

    if transitioned_to_extreme:
        return True, NotificationDecisionReason.SAFETY_DOWNGRADE

    if current.internal_direction is InPlayDirection.WATCH and (
        record.current_internal_direction in _DIRECTIONAL
        or last.internal_direction in _DIRECTIONAL
    ):
        return False, NotificationDecisionReason.DIRECTION_WEAKENED

    if direction_confirmed:
        return True, NotificationDecisionReason.DIRECTION_CONFIRMED

    if direction_changed:
        return True, NotificationDecisionReason.DIRECTION_CHANGED

    if result.inplay_score - last.inplay_score >= MINIMUM_SCORE_INCREASE - 1e-12:
        return True, NotificationDecisionReason.SCORE_INCREASED

    confirmations = set(current.visible_confirmations)
    if confirmations.difference(last.visible_confirmations):
        return True, NotificationDecisionReason.IMPORTANT_CONFIRMATION

    if new_listing:
        return True, NotificationDecisionReason.NEW_LISTING

    if now - last.sent_at >= NOTIFICATION_COOLDOWN:
        return True, NotificationDecisionReason.COOLDOWN_EXPIRED

    return False, NotificationDecisionReason.UNCHANGED


def _same_semantics(
    current: AutomaticInPlaySemantics,
    last: SentNotification,
) -> bool:
    if last.internal_direction is not None:
        current_fingerprint = _semantic_fingerprint(
            symbol=current.result.symbol,
            internal_direction=current.internal_direction,
            display_status=current.display_status,
            risk_class=current.risk_class,
            user_action=current.user_action,
            visible_confirmations=current.visible_confirmations,
        )
        return current_fingerprint == last.semantic_fingerprint
    direction_matches = (
        last.internal_direction is None
        or current.internal_direction is last.internal_direction
    )
    return (
        direction_matches
        and current.display_status is last.display_status
        and current.risk_class is last.risk_class
        and current.user_action == last.user_action
        and current.visible_confirmations == last.visible_confirmations
    )


def _updated_record(
    result: InPlayResult,
    previous: NotificationRecord | None,
    now: datetime,
    *,
    should_notify: bool,
) -> NotificationRecord:
    if should_notify or previous is None:
        sent = _sent_notification(result, now)
        return NotificationRecord(
            symbol=result.symbol,
            current_internal_direction=sent.internal_direction,
            current_display_status=sent.display_status,
            last_notification=sent,
            new_listing_notified=(
                result.is_new_listing
                or (previous.new_listing_notified if previous is not None else False)
            ),
        )
    semantics = automatic_semantics(result)
    last_notification = previous.last_notification
    if last_notification.internal_direction is None:
        last_notification = _enrich_legacy_direction(
            last_notification,
            semantics.internal_direction,
        )
    return replace(
        previous,
        current_internal_direction=semantics.internal_direction,
        current_display_status=semantics.display_status,
        last_notification=last_notification,
        absent_since=None,
    )


def _sent_notification(result: InPlayResult, sent_at: datetime) -> SentNotification:
    semantics = automatic_semantics(result)
    return SentNotification(
        symbol=result.symbol,
        internal_direction=semantics.internal_direction,
        display_status=semantics.display_status,
        risk_class=semantics.risk_class,
        user_action=semantics.user_action,
        visible_confirmations=semantics.visible_confirmations,
        semantic_fingerprint=_semantic_fingerprint(
            symbol=result.symbol,
            internal_direction=semantics.internal_direction,
            display_status=semantics.display_status,
            risk_class=semantics.risk_class,
            user_action=semantics.user_action,
            visible_confirmations=semantics.visible_confirmations,
        ),
        inplay_score=result.inplay_score,
        sent_at=sent_at,
        is_new_listing=result.is_new_listing,
        reasons_fingerprint=_reasons_fingerprint(result.reasons),
        important_confirmations=semantics.visible_confirmations,
    )


def _enrich_legacy_direction(
    sent: SentNotification,
    internal_direction: InPlayDirection,
) -> SentNotification:
    return replace(
        sent,
        internal_direction=internal_direction,
        semantic_fingerprint=_semantic_fingerprint(
            symbol=sent.symbol,
            internal_direction=internal_direction,
            display_status=sent.display_status,
            risk_class=sent.risk_class,
            user_action=sent.user_action,
            visible_confirmations=sent.visible_confirmations,
        ),
    )


def _unique_results(results: tuple[InPlayResult, ...]) -> tuple[InPlayResult, ...]:
    selected: dict[str, InPlayResult] = {}
    for item in results:
        symbol = item.symbol.strip().upper()
        if symbol not in selected:
            selected[symbol] = item
    return tuple(selected.values())


def _reasons_fingerprint(reasons: tuple[str, ...]) -> str:
    normalized = sorted(
        {_normalize_reason(reason, hide_numbers=True) for reason in reasons}
    )
    content = "\n".join(item for item in normalized if item)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _semantic_fingerprint(
    *,
    symbol: str,
    internal_direction: InPlayDirection | None,
    display_status: AutomaticDisplayStatus,
    risk_class: AutomaticRiskClass,
    user_action: str,
    visible_confirmations: tuple[str, ...],
) -> str:
    payload = {
        "symbol": symbol.strip().upper(),
        "internal_direction": (
            internal_direction.value if internal_direction is not None else "UNKNOWN"
        ),
        "display_status": display_status.value,
        "risk_class": risk_class.value,
        "user_action": _normalize_reason(user_action, hide_numbers=True),
        "visible_confirmations": sorted(
            _normalize_reason(item, hide_numbers=True)
            for item in visible_confirmations
        ),
    }
    content = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _important_confirmations(reasons: tuple[str, ...]) -> tuple[str, ...]:
    return visible_confirmations(reasons)


def _normalize_reason(reason: str, *, hide_numbers: bool) -> str:
    normalized = unicodedata.normalize("NFKC", reason).casefold()
    if hide_numbers:
        normalized = _NUMBER.sub("#", normalized)
    return " ".join(_NON_WORD.sub(" ", normalized).split())


def _state_from_json(value: Any) -> NotificationState:
    if not isinstance(value, dict) or value.get("version") not in {1, 2}:
        raise ValueError("Unsupported notification-state payload.")
    version = value["version"]
    raw_records = value["records"]
    if not isinstance(raw_records, dict):
        raise ValueError("Notification records must be an object.")
    records: dict[str, NotificationRecord] = {}
    for key, raw_record in raw_records.items():
        if not isinstance(key, str) or not isinstance(raw_record, dict):
            raise ValueError("Invalid notification record.")
        record = _record_from_json(raw_record, version=version)
        if record.symbol != key.strip().upper():
            raise ValueError("Notification symbol does not match its key.")
        records[record.symbol] = record
    return NotificationState(_optional_datetime(value.get("updated_at")), records)


def _record_from_json(
    value: Mapping[str, Any],
    *,
    version: int,
) -> NotificationRecord:
    raw_notification = value["last_notification"]
    if not isinstance(raw_notification, dict):
        raise ValueError("Invalid sent notification.")
    legacy_direction = _optional_direction(raw_notification.get("direction"))
    raw_important = tuple(
        _string(item) for item in raw_notification["important_confirmations"]
    )
    default_display = _legacy_display_status(legacy_direction).value
    display_status = AutomaticDisplayStatus(
        _string(raw_notification.get("display_status", default_display))
    )
    visible = raw_notification.get("visible_confirmations")
    visible_values = (
        tuple(_string(item) for item in visible)
        if isinstance(visible, list)
        else visible_confirmations(raw_important)
    )
    internal_direction = _persisted_internal_direction(
        raw_notification,
        legacy_direction=legacy_direction,
        display_status=display_status,
        version=version,
    )
    risk_class = AutomaticRiskClass(
        _string(
            raw_notification.get(
                "risk_class",
                _legacy_risk_class(display_status).value,
            )
        )
    )
    user_action = _string(
        raw_notification.get(
            "user_action",
            _legacy_user_action(display_status),
        )
    )
    symbol = _string(raw_notification["symbol"])
    semantic_fingerprint = raw_notification.get("semantic_fingerprint")
    if semantic_fingerprint is None:
        semantic_fingerprint = _semantic_fingerprint(
            symbol=symbol,
            internal_direction=internal_direction,
            display_status=display_status,
            risk_class=risk_class,
            user_action=user_action,
            visible_confirmations=visible_values,
        )
    sent = SentNotification(
        symbol=symbol,
        internal_direction=internal_direction,
        display_status=display_status,
        risk_class=risk_class,
        user_action=user_action,
        visible_confirmations=visible_values,
        semantic_fingerprint=_string(semantic_fingerprint),
        inplay_score=_float(raw_notification["inplay_score"]),
        sent_at=_datetime(raw_notification["sent_at"]),
        is_new_listing=_boolean(raw_notification["is_new_listing"]),
        reasons_fingerprint=_string(
            raw_notification.get("reasons_fingerprint", "legacy")
        ),
        important_confirmations=visible_values,
    )
    current_internal_direction = _record_internal_direction(
        value,
        fallback=internal_direction,
        display_status=display_status,
        version=version,
    )
    return NotificationRecord(
        symbol=_string(value["symbol"]),
        current_internal_direction=current_internal_direction,
        current_display_status=AutomaticDisplayStatus(
            _string(value.get("current_display_status", display_status.value))
        ),
        last_notification=sent,
        new_listing_notified=_boolean(value["new_listing_notified"]),
        absent_since=_optional_datetime(value.get("absent_since")),
    )


def _record_to_json(record: NotificationRecord) -> dict[str, Any]:
    sent = record.last_notification
    return {
        "symbol": record.symbol,
        "current_internal_direction": (
            record.current_internal_direction.value
            if record.current_internal_direction is not None
            else None
        ),
        "current_display_status": record.current_display_status.value,
        "current_direction": (
            record.current_internal_direction.value
            if record.current_internal_direction is not None
            else None
        ),
        "new_listing_notified": record.new_listing_notified,
        "absent_since": (
            record.absent_since.isoformat() if record.absent_since is not None else None
        ),
        "last_notification": {
            "symbol": sent.symbol,
            "internal_direction": (
                sent.internal_direction.value
                if sent.internal_direction is not None
                else None
            ),
            "direction": (
                sent.internal_direction.value
                if sent.internal_direction is not None
                else None
            ),
            "display_status": sent.display_status.value,
            "risk_class": sent.risk_class.value,
            "user_action": sent.user_action,
            "visible_confirmations": list(sent.visible_confirmations),
            "semantic_fingerprint": sent.semantic_fingerprint,
            "inplay_score": sent.inplay_score,
            "reasons_fingerprint": sent.reasons_fingerprint,
            "important_confirmations": list(sent.important_confirmations),
            "sent_at": sent.sent_at.isoformat(),
            "is_new_listing": sent.is_new_listing,
        },
    }


def _legacy_display_status(
    direction: InPlayDirection | None,
) -> AutomaticDisplayStatus:
    if direction is InPlayDirection.LONG:
        return AutomaticDisplayStatus.LONG
    if direction is InPlayDirection.SHORT:
        return AutomaticDisplayStatus.SHORT
    return AutomaticDisplayStatus.WATCH


def _legacy_risk_class(
    status: AutomaticDisplayStatus,
) -> AutomaticRiskClass:
    if status is AutomaticDisplayStatus.DO_NOT_CHASE:
        return AutomaticRiskClass.EXTREME
    if status is AutomaticDisplayStatus.LATE_ENTRY:
        return AutomaticRiskClass.LATE_ENTRY
    return AutomaticRiskClass.NORMAL


def _persisted_internal_direction(
    value: Mapping[str, Any],
    *,
    legacy_direction: InPlayDirection | None,
    display_status: AutomaticDisplayStatus,
    version: int,
) -> InPlayDirection | None:
    if version == 2:
        return _optional_direction(value.get("internal_direction"))
    if (
        legacy_direction is InPlayDirection.WATCH
        and display_status
        in {AutomaticDisplayStatus.LATE_ENTRY, AutomaticDisplayStatus.DO_NOT_CHASE}
    ):
        return None
    return legacy_direction


def _record_internal_direction(
    value: Mapping[str, Any],
    *,
    fallback: InPlayDirection | None,
    display_status: AutomaticDisplayStatus,
    version: int,
) -> InPlayDirection | None:
    field = "current_internal_direction" if version == 2 else "current_direction"
    direction = _optional_direction(value.get(field))
    if (
        version == 1
        and direction is InPlayDirection.WATCH
        and display_status
        in {AutomaticDisplayStatus.LATE_ENTRY, AutomaticDisplayStatus.DO_NOT_CHASE}
    ):
        return None
    return direction if direction is not None else fallback


def _legacy_user_action(status: AutomaticDisplayStatus) -> str:
    if status in {AutomaticDisplayStatus.LONG, AutomaticDisplayStatus.SHORT}:
        return "Проверить уровень, стоп и соотношение риска к прибыли."
    return "Ждать пробой или дополнительное подтверждение."


def _datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected ISO timestamp.")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Expected timezone-aware ISO timestamp.")
    return parsed.astimezone(UTC)


def _optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    return _datetime(value)


def _string(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected string value.")
    return value


def _optional_direction(value: Any) -> InPlayDirection | None:
    if value is None:
        return None
    return InPlayDirection(_string(value))


def _float(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Expected numeric value.")
    return float(value)


def _boolean(value: Any) -> bool:
    if not isinstance(value, bool):
        raise ValueError("Expected boolean value.")
    return value
