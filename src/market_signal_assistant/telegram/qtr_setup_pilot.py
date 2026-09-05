from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol

from market_signal_assistant.qtr_setup_pilot.audit import (
    JsonlQtrSetupTelegramAuditStore,
    QtrSetupAuditOutcome,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.qtr_setup_pilot.notifications import (
    QtrSetupDecision,
    QtrSetupEvent,
    QtrSetupNotificationPlan,
    QtrSetupNotificationService,
)
from market_signal_assistant.setup_engine.models import (
    SetupDirection,
    SetupState,
    SetupType,
)

QTR_SETUP_MAXIMUM_EVENTS = 3
_LOGGER = logging.getLogger(__name__)


class QtrSetupScanner(Protocol):
    def scan(self) -> tuple[QtrSetupCandidate, ...]: ...


QtrSetupSender = Callable[[int, str], Awaitable[None]]
QtrSetupCandidateHandler = Callable[
    [tuple[QtrSetupCandidate, ...], QtrSetupSender], Awaitable[None]
]


class QtrSetupPilotNotifier:
    def __init__(
        self,
        *,
        scanner: QtrSetupScanner,
        notification_service: QtrSetupNotificationService,
        audit_store: JsonlQtrSetupTelegramAuditStore,
        allowed_chat_ids: frozenset[int],
        clock: Callable[[], datetime] | None = None,
        candidate_handler: QtrSetupCandidateHandler | None = None,
    ) -> None:
        self._scanner = scanner
        self._notifications = notification_service
        self._audit = audit_store
        self._allowed_chat_ids = allowed_chat_ids
        self._clock = clock or (lambda: datetime.now(UTC))
        self._candidate_handler = candidate_handler
        self._lock = asyncio.Lock()

    async def run_once(self, send: QtrSetupSender) -> bool:
        if not self._allowed_chat_ids:
            _LOGGER.warning("QTR Setup Pilot не запущен: нет разрешённых chat ID.")
            return False
        if self._lock.locked():
            _LOGGER.warning("Предыдущий QTR Setup scan ещё выполняется; scan пропущен.")
            return False
        now = self._clock()
        try:
            async with self._lock:
                candidates = await asyncio.to_thread(self._scanner.scan)
                plan = await asyncio.to_thread(
                    self._notifications.prepare, candidates, now
                )
                metrics = self._notifications.metrics
                _LOGGER.info(
                    "QTR Scanner Telegram filter: candidates_seen=%d, "
                    "telegram_quality_passed=%d, suppressed_status=%d, "
                    "suppressed_quality=%d, suppressed_distance=%d, "
                    "suppressed_duplicate=%d",
                    metrics.candidates_seen,
                    metrics.telegram_quality_passed,
                    metrics.suppressed_status,
                    metrics.suppressed_quality,
                    metrics.suppressed_distance,
                    metrics.suppressed_duplicate,
                )
                selected = select_qtr_setup_decisions(plan)
                sent_ids: set[str] = set()
                try:
                    for decision in selected:
                        assert decision.event is not None
                        text = format_qtr_setup_event(decision.event)
                        for chat_id in sorted(self._allowed_chat_ids):
                            await send(chat_id, text)
                        sent_ids.add(decision.decision_id)
                except Exception as error:
                    _LOGGER.warning(
                        "Telegram не доставил QTR Setup уведомление (%s).",
                        type(error).__name__,
                    )
                    self._write_audit(plan, frozenset(sent_ids), frozenset(), now)
                    return False
                delivered = frozenset(sent_ids)
                try:
                    await asyncio.to_thread(
                        self._notifications.commit, plan, delivered
                    )
                except Exception as error:
                    _LOGGER.warning(
                        "QTR Setup отправлен, но state не сохранён (%s).",
                        type(error).__name__,
                    )
                    self._write_audit(plan, delivered, frozenset(), now)
                    return False
                self._write_audit(plan, delivered, delivered, now)
                if self._candidate_handler is not None:
                    try:
                        await self._candidate_handler(candidates, send)
                    except Exception as error:
                        _LOGGER.warning(
                            "QTR Micro candidate handler завершился ошибкой (%s).",
                            type(error).__name__,
                        )
                return bool(delivered)
        except Exception as error:
            _LOGGER.warning(
                "QTR Setup Pilot scan завершился ошибкой (%s).",
                type(error).__name__,
            )
            return False

    def _write_audit(
        self,
        plan: QtrSetupNotificationPlan,
        sent: frozenset[str],
        committed: frozenset[str],
        now: datetime,
    ) -> None:
        outcomes = tuple(
            QtrSetupAuditOutcome(
                decision,
                decision.decision_id in sent,
                decision.decision_id in committed,
            )
            for decision in plan.decisions
        )
        self._audit.append(outcomes, now)


class QtrSetupPilotLoop:
    def __init__(
        self,
        notifier: QtrSetupPilotNotifier,
        send: QtrSetupSender,
        *,
        interval_seconds: float,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("QTR Setup interval must be positive.")
        self._notifier = notifier
        self._send = send
        self._interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if not self.running:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while True:
            await self._notifier.run_once(self._send)
            await asyncio.sleep(self._interval_seconds)


def select_qtr_setup_decisions(
    plan: QtrSetupNotificationPlan,
) -> tuple[QtrSetupDecision, ...]:
    selected = [
        item
        for item in plan.decisions
        if item.should_notify
        and item.event is not None
        and item.event.state is SetupState.READY_TO_CONSIDER
    ]
    selected.sort(
        key=lambda item: (
            -(item.event.quality_score if item.event is not None else 0.0),
            -item.candidate.result.confidence,
            item.candidate.result.symbol,
        )
    )
    return tuple(selected[:QTR_SETUP_MAXIMUM_EVENTS])


def format_qtr_setup_event(event: QtrSetupEvent) -> str:
    if event.state is not SetupState.READY_TO_CONSIDER:
        raise ValueError("Telegram formatter accepts only elite READY candidates.")
    result = event.candidate.result
    direction, marker = _direction_signal(event.direction)
    lines = [
        "🔥 QTR A+ CANDIDATE",
        "",
        f"{result.symbol} — {direction} {marker}",
        f"Setup: {_type_ru(event.setup_type)}",
        f"Quality: {event.quality_score:.0f}/100",
    ]
    if result.trigger_level is not None:
        lines.extend(("", f"Уровень: {_number(result.trigger_level)}"))
    if result.current_price is not None:
        lines.append(f"Цена: {_number(result.current_price)}")
    if result.distance_to_trigger_atr is not None:
        lines.append(
            f"Distance: {_number(abs(result.distance_to_trigger_atr))} ATR"
        )
    lines.append("")
    confirmations = event.visible_confirmations[:6]
    lines.extend(f"✅ {_confirmation_short(item)}" for item in confirmations)
    lines.extend(
        (
            "",
            "👤 Требуется ручная проверка:",
            "уровни / объём / стоп / риск.",
            "",
            "Quality — внутренний рейтинг QTR,",
            "НЕ вероятность выигрыша.",
        )
    )
    text = "\n".join(lines)
    forbidden = ("WATCHING", "FORMING", "CONFIRMING", "READY", "FALSE_BREAKOUT")
    if any(item in text for item in forbidden):
        raise ValueError("Machine setup state leaked into Telegram text.")
    return text


def _direction_signal(direction: SetupDirection) -> tuple[str, str]:
    return {
        SetupDirection.UP: ("LONG", "🟢"),
        SetupDirection.DOWN: ("SHORT", "🔴"),
        SetupDirection.NEUTRAL: ("WATCH", "⚪"),
    }[direction]


def _type_ru(setup_type: SetupType) -> str:
    return {
        SetupType.BREAKOUT: "ПРОБОЙ",
        SetupType.RETEST: "РЕТЕСТ",
        SetupType.IMPULSE: "ИМПУЛЬС",
        SetupType.COMPRESSION: "СЖАТИЕ",
        SetupType.CONTINUATION: "ПРОДОЛЖЕНИЕ",
        SetupType.FALSE_BREAKOUT: "ЛОЖНЫЙ ПРОБОЙ",
        SetupType.REVERSAL: "РАЗВОРОТ",
        SetupType.NO_TRADE: "НЕТ ПОДХОДЯЩЕЙ КОНСТРУКЦИИ",
    }[setup_type]


def _confirmation_short(value: str) -> str:
    return {
        "Структура подтверждена": "Структура",
        "Цена удерживает нужную сторону уровня": "Нужная сторона уровня",
        "Ретест уровня удержан": "Ретест удержан",
        "Объём подтверждает движение": "Объём",
        "Волатильность подтверждает импульс": "Волатильность",
        "Ликвидность и спред допустимы": "Ликвидность / spread",
        "Конструкция остаётся свежей": "Свежий setup",
    }.get(value, value)


def _number(value: float) -> str:
    if abs(value) >= 1000:
        rendered = f"{value:,.2f}".replace(",", " ").rstrip("0").rstrip(".")
    else:
        rendered = f"{value:.8f}".rstrip("0").rstrip(".")
    return rendered.replace(".", ",")
