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


class QtrSetupPilotNotifier:
    def __init__(
        self,
        *,
        scanner: QtrSetupScanner,
        notification_service: QtrSetupNotificationService,
        audit_store: JsonlQtrSetupTelegramAuditStore,
        allowed_chat_ids: frozenset[int],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._scanner = scanner
        self._notifications = notification_service
        self._audit = audit_store
        self._allowed_chat_ids = allowed_chat_ids
        self._clock = clock or (lambda: datetime.now(UTC))
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
        item for item in plan.decisions if item.should_notify and item.event is not None
    ]
    selected.sort(
        key=lambda item: (
            _priority(
                item.event.state
                if item.event is not None
                else SetupState.FORMING
            ),
            -item.candidate.result.confidence,
            item.candidate.result.symbol,
        )
    )
    return tuple(selected[:QTR_SETUP_MAXIMUM_EVENTS])


def format_qtr_setup_event(event: QtrSetupEvent) -> str:
    result = event.candidate.result
    title = _title(event.state)
    lines = [
        title,
        "",
        result.symbol,
        f"Направление: {_direction_ru(event.direction)}",
        f"Конструкция: {_type_ru(event.setup_type)}",
        f"Статус: {_state_ru(event.state)}",
    ]
    if result.trigger_level is not None:
        lines.append(f"Ключевой уровень: {_number(result.trigger_level)}")
    if result.current_price is not None:
        lines.append(f"Текущая цена: {_number(result.current_price)}")
    if result.distance_to_trigger_atr is not None:
        lines.append(
            f"Расстояние от уровня: {_number(result.distance_to_trigger_atr)} ATR"
        )
    lines.extend(("", "Подтверждения:"))
    confirmations = event.visible_confirmations[:6]
    lines.extend(f"• {item}" for item in confirmations)
    if not confirmations:
        lines.append("• Явных подтверждений пока недостаточно")
    lines.extend(("", _action(event)))
    if event.warnings:
        lines.extend(("", f"Риск: {event.warnings[0]}"))
    lines.extend(("", "Информационный анализ, не торговая рекомендация."))
    text = "\n".join(lines)
    forbidden = ("WATCHING", "FORMING", "CONFIRMING", "READY", "FALSE_BREAKOUT")
    if any(item in text for item in forbidden):
        raise ValueError("Machine setup state leaked into Telegram text.")
    return text


def _priority(state: SetupState) -> int:
    return {
        SetupState.READY_TO_CONSIDER: 0,
        SetupState.CANCELLED: 1,
        SetupState.LATE: 2,
        SetupState.CONFIRMING: 3,
        SetupState.FORMING: 4,
        SetupState.WATCHING: 5,
    }[state]


def _title(state: SetupState) -> str:
    return {
        SetupState.FORMING: "🟡 QTR SCANNER — ФОРМИРУЕТСЯ",
        SetupState.CONFIRMING: "🟠 QTR SCANNER — ПОДТВЕРЖДАЕТСЯ",
        SetupState.READY_TO_CONSIDER: "🟢 QTR SCANNER — ГОТОВО К РАССМОТРЕНИЮ",
        SetupState.LATE: "⛔ QTR SCANNER — ПОЗДНО",
        SetupState.CANCELLED: "⚫ QTR SCANNER — ОТМЕНЕНО",
        SetupState.WATCHING: "⚪ QTR SCANNER — НАБЛЮДАЕМ",
    }[state]


def _direction_ru(direction: SetupDirection) -> str:
    return {
        SetupDirection.UP: "ВВЕРХ",
        SetupDirection.DOWN: "ВНИЗ",
        SetupDirection.NEUTRAL: "НЕЙТРАЛЬНО",
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


def _state_ru(state: SetupState) -> str:
    return {
        SetupState.WATCHING: "НАБЛЮДАЕМ",
        SetupState.FORMING: "ФОРМИРУЕТСЯ",
        SetupState.CONFIRMING: "ПОДТВЕРЖДАЕТСЯ",
        SetupState.READY_TO_CONSIDER: "ГОТОВО К РАССМОТРЕНИЮ",
        SetupState.LATE: "ПОЗДНО",
        SetupState.CANCELLED: "ОТМЕНЕНО",
    }[state]


def _action(event: QtrSetupEvent) -> str:
    return {
        SetupState.FORMING: "Действие: наблюдать за развитием конструкции.",
        SetupState.CONFIRMING: "Действие: дождаться полного подтверждения условий.",
        SetupState.READY_TO_CONSIDER: (
            "Следующий этап: определить точку входа, стоп и риск."
        ),
        SetupState.LATE: "Действие: не догонять движение; ждать новую конструкцию.",
        SetupState.CANCELLED: "Действие: исключить эту конструкцию из рассмотрения.",
        SetupState.WATCHING: "Действие: продолжать наблюдение.",
    }[event.state]


def _number(value: float) -> str:
    if abs(value) >= 1000:
        rendered = f"{value:,.2f}".replace(",", " ").rstrip("0").rstrip(".")
    else:
        rendered = f"{value:.8f}".rstrip("0").rstrip(".")
    return rendered.replace(".", ",")
