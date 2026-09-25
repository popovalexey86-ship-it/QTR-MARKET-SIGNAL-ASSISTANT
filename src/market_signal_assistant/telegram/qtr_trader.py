from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from market_signal_assistant.qtr_micro.models import MicroExitReason, MicroStage
from market_signal_assistant.qtr_micro.runtime import (
    QtrMicroPositionEvent,
    QtrMicroPositionSnapshot,
)

TraderButtons = tuple[tuple[tuple[str, str], ...], ...]
_CONFIRMATION_TTL = timedelta(seconds=30)
_ACTIVE_STAGES = frozenset(
    {
        MicroStage.OPEN,
        MicroStage.TP1_FILLED,
        MicroStage.TP2_FILLED,
        MicroStage.RUNNER,
    }
)


class TraderRuntime(Protocol):
    async def get_position_snapshot(
        self, trade_id: str
    ) -> QtrMicroPositionSnapshot | None: ...

    async def request_human_close(
        self, trade_id: str
    ) -> QtrMicroPositionSnapshot | None: ...


class TraderMessenger(Protocol):
    async def send_card(
        self, chat_id: int, text: str, buttons: TraderButtons
    ) -> int | None: ...

    async def edit_card(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        buttons: TraderButtons,
    ) -> None: ...


class QtrTraderTelegramController:
    """Human-supervised Telegram UI; never makes trading decisions."""

    def __init__(
        self,
        runtime: TraderRuntime,
        messenger: TraderMessenger,
        allowed_chat_ids: frozenset[int],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._runtime = runtime
        self._messenger = messenger
        self._allowed_chat_ids = allowed_chat_ids
        self._clock = clock or (lambda: datetime.now(UTC))
        self._cards: dict[tuple[int, str], int] = {}
        self._confirmations: dict[tuple[int, str], datetime] = {}

    async def handle_position_event(self, event: QtrMicroPositionEvent) -> None:
        snapshot = event.snapshot
        text, buttons = format_position_card(
            snapshot,
            exit_reason=event.exit_reason,
            close_pending=event.event_type == "CLOSE_PENDING",
        )
        for chat_id in sorted(self._allowed_chat_ids):
            key = (chat_id, snapshot.trade_id)
            message_id = self._cards.get(key)
            if message_id is None:
                sent = await self._messenger.send_card(chat_id, text, buttons)
                if sent is not None:
                    self._cards[key] = sent
            else:
                await self._messenger.edit_card(chat_id, message_id, text, buttons)

    async def handle_callback(self, update: Any, context: Any) -> None:
        del context
        query = getattr(update, "callback_query", None)
        if query is None:
            return
        message = getattr(query, "message", None)
        data = getattr(query, "data", None)
        if message is None or not isinstance(data, str):
            await query.answer("Некорректная команда.", show_alert=True)
            return
        chat = getattr(message, "chat", None)
        chat_id = getattr(chat, "id", None)
        chat_type = str(getattr(chat, "type", "")).lower()
        if (
            not isinstance(chat_id, int)
            or chat_id not in self._allowed_chat_ids
            or chat_type != "private"
        ):
            await query.answer("Доступ запрещён.", show_alert=True)
            return
        parsed = _parse_callback(data)
        if parsed is None:
            await query.answer("Некорректная команда.", show_alert=True)
            return
        action, trade_id = parsed
        message_id = int(message.message_id)
        key = (chat_id, trade_id)

        if action == "hold":
            self._confirmations.pop(key, None)
            snapshot = await self._runtime.get_position_snapshot(trade_id)
            if snapshot is None:
                await query.answer("Позиция не найдена.", show_alert=True)
                return
            await self._render(chat_id, message_id, snapshot)
            await query.answer("🟢 Держим позицию — без изменений.")
            return

        if action == "refresh":
            snapshot = await self._runtime.get_position_snapshot(trade_id)
            if snapshot is None:
                await query.answer("Позиция не найдена.", show_alert=True)
                return
            await self._render(chat_id, message_id, snapshot)
            await query.answer("🔄 Данные обновлены.")
            return

        if action == "close":
            snapshot = await self._runtime.get_position_snapshot(trade_id)
            if snapshot is None:
                await query.answer("Позиция не найдена.", show_alert=True)
                return
            if snapshot.stage not in _ACTIVE_STAGES:
                await self._render(chat_id, message_id, snapshot)
                await query.answer(
                    "⚠️ Эта позиция уже недоступна для ручного закрытия.", show_alert=True
                )
                return
            self._confirmations[key] = self._clock() + _CONFIRMATION_TTL
            text, buttons = format_close_confirmation(snapshot)
            await self._messenger.edit_card(chat_id, message_id, text, buttons)
            self._cards[key] = message_id
            await query.answer()
            return

        if action == "cancel":
            self._confirmations.pop(key, None)
            snapshot = await self._runtime.get_position_snapshot(trade_id)
            if snapshot is None:
                await query.answer("Позиция не найдена.", show_alert=True)
                return
            await self._render(chat_id, message_id, snapshot)
            await query.answer("↩️ Закрытие отменено.")
            return

        if action == "confirm":
            expires_at = self._confirmations.pop(key, None)
            if expires_at is None or self._clock() > expires_at:
                snapshot = await self._runtime.get_position_snapshot(trade_id)
                if snapshot is not None:
                    await self._render(chat_id, message_id, snapshot)
                await query.answer(
                    "⏱ Подтверждение закрытия истекло.", show_alert=True
                )
                return
            snapshot = await self._runtime.request_human_close(trade_id)
            if snapshot is None:
                await query.answer("Позиция не найдена.", show_alert=True)
                return
            await self._render(
                chat_id,
                message_id,
                snapshot,
                exit_reason=MicroExitReason.HUMAN_CLOSE,
                close_pending=snapshot.stage is MicroStage.EXIT_ACKNOWLEDGED,
            )
            await query.answer("🔴 Команда на закрытие принята.")
            return

        await query.answer("Некорректная команда.", show_alert=True)

    async def _render(
        self,
        chat_id: int,
        message_id: int,
        snapshot: QtrMicroPositionSnapshot,
        *,
        exit_reason: MicroExitReason | None = None,
        close_pending: bool = False,
    ) -> None:
        text, buttons = format_position_card(
            snapshot,
            exit_reason=exit_reason,
            close_pending=close_pending,
        )
        await self._messenger.edit_card(chat_id, message_id, text, buttons)
        self._cards[(chat_id, snapshot.trade_id)] = message_id


def format_position_card(
    snapshot: QtrMicroPositionSnapshot,
    *,
    exit_reason: MicroExitReason | None = None,
    close_pending: bool = False,
) -> tuple[str, TraderButtons]:
    direction = _direction_label(snapshot.direction)
    if snapshot.stage is MicroStage.CLOSED:
        reason = _exit_reason_label(exit_reason)
        text = "\n".join(
            (
                "🏁 QTR TRADER — ПОЗИЦИЯ ЗАКРЫТА",
                "",
                f"{snapshot.symbol} • {direction}",
                f"📌 Причина: {reason}",
                "",
                f"🎯 Вход            {_number(snapshot.actual_entry)}",
                f"🏁 Выход           {_number(snapshot.exit_price)}",
                f"📦 Объём           {_number(snapshot.initial_qty)}",
                "",
                f"💵 PnL до комиссий {_signed_money(snapshot.gross_pnl)}",
                f"💸 Комиссии        -{_money(abs(snapshot.fees))}",
                f"💰 Итоговый PnL    {_signed_money(snapshot.net_pnl)}",
                f"📊 Результат       {_signed_r(snapshot.current_r)}",
                "",
                f"🚀 Макс. плюс MFE  {_signed_r(snapshot.mfe_r)}",
                f"📉 Макс. минус MAE {_signed_r(snapshot.mae_r)}",
                f"⏱ В позиции       {_duration(snapshot.duration_seconds)}",
                "",
                f"🆔 Сделка          {snapshot.trade_id}",
            )
        )
        return text, ()

    state = (
        "⏳ ЗАКРЫТИЕ ОЖИДАЕТСЯ"
        if close_pending
        else _stage_label(snapshot.stage)
    )
    risk = (
        f"{_money(snapshot.initial_risk_usdt)} = 1R"
        if snapshot.initial_risk_usdt is not None
        else "—"
    )
    text = "\n".join(
        (
            "📡 QTR TRADER — MICRO DEMO",
            "",
            f"{snapshot.symbol} • {direction}",
            f"🧩 Сетап: {snapshot.setup}",
            f"📊 Состояние: {state}",
            "",
            f"🔎 Уровень Scanner {_number(snapshot.scanner_level)}",
            f"🎯 Вход            {_number(snapshot.actual_entry)}",
            f"💹 Цена сейчас     {_number(snapshot.current_price)}",
            "",
            f"📦 Объём           {_number(snapshot.current_qty)}",
            f"💵 Номинал         {_money(snapshot.notional)}",
            f"⚙️ Плечо           x{snapshot.leverage}",
            "",
            f"🛡 SL               {_number(snapshot.current_sl)}",
            f"🎯 TP1              {_number(snapshot.tp1)}",
            f"🎯 TP2              {_number(snapshot.tp2)}",
            f"🚀 Runner           {_number(snapshot.runner_target)}",
            "",
            f"💰 PnL сейчас      {_signed_money(snapshot.current_pnl_est)}",
            f"📊 Текущий R       {_signed_r(snapshot.current_r)}",
            f"🚀 MFE             {_signed_r(snapshot.mfe_r)}",
            f"📉 MAE             {_signed_r(snapshot.mae_r)}",
            f"🏆 Max R           {_signed_r(snapshot.max_r)}",
            "",
            f"🛡 Риск            {risk}",
            f"⏱ В позиции       {_duration(snapshot.duration_seconds)}",
            "",
            f"🆔 Сделка          {snapshot.trade_id}",
        )
    )
    if snapshot.stage is MicroStage.EXIT_ACKNOWLEDGED or close_pending:
        return text, (
            (("🔄 ОБНОВИТЬ", _callback("refresh", snapshot.trade_id)),),
        )
    if snapshot.stage in _ACTIVE_STAGES:
        return text, (
            (
                ("🟢 ДЕРЖАТЬ", _callback("hold", snapshot.trade_id)),
                ("🔴 ЗАКРЫТЬ", _callback("close", snapshot.trade_id)),
            ),
            (("🔄 ОБНОВИТЬ", _callback("refresh", snapshot.trade_id)),),
        )
    return text, ()


def format_close_confirmation(
    snapshot: QtrMicroPositionSnapshot,
) -> tuple[str, TraderButtons]:
    text = "\n".join(
        (
            "⚠️ QTR TRADER — ПОДТВЕРДИТЬ ЗАКРЫТИЕ?",
            "",
            f"{snapshot.symbol} • {_direction_label(snapshot.direction)}",
            f"💰 PnL сейчас: {_signed_money(snapshot.current_pnl_est)}",
            f"📊 Текущий R: {_signed_r(snapshot.current_r)}",
            "",
            "Первое нажатие «🔴 ЗАКРЫТЬ» ордер не отправляет.",
            "Ордер уйдёт только после отдельного подтверждения.",
        )
    )
    return text, (
        (
            (
                "✅ ПОДТВЕРДИТЬ ЗАКРЫТИЕ",
                _callback("confirm", snapshot.trade_id),
            ),
        ),
        (("↩️ ОТМЕНА", _callback("cancel", snapshot.trade_id)),),
    )


def _direction_label(direction: str) -> str:
    normalized = direction.upper()
    if normalized == "LONG":
        return "🟢 ЛОНГ"
    if normalized == "SHORT":
        return "🔴 ШОРТ"
    return direction


def _stage_label(stage: MicroStage) -> str:
    labels = {
        MicroStage.OPEN: "🟢 ОТКРЫТА",
        MicroStage.TP1_FILLED: "✅ TP1 ВЫПОЛНЕН",
        MicroStage.TP2_FILLED: "✅ TP2 ВЫПОЛНЕН",
        MicroStage.RUNNER: "🚀 RUNNER",
        MicroStage.EXIT_ACKNOWLEDGED: "⏳ ЗАКРЫТИЕ ОТПРАВЛЕНО",
    }
    return labels.get(stage, stage.value)


def _exit_reason_label(reason: MicroExitReason | None) -> str:
    if reason is None:
        return "не указана"
    labels = {
        MicroExitReason.STOP: "🛑 Стоп-лосс",
        MicroExitReason.TP1: "🎯 TP1",
        MicroExitReason.TP2: "🎯 TP2",
        MicroExitReason.RUNNER_TARGET: "🚀 Цель Runner",
        MicroExitReason.TIME_EXIT: "⏱ Выход по времени",
        MicroExitReason.RUNNER_TIME_EXIT: "⏱ Выход Runner по времени",
        MicroExitReason.STRUCTURE_EXIT: "📉 Выход по структуре",
        MicroExitReason.HUMAN_CLOSE: "👤 Закрыто через QTR Trader",
        MicroExitReason.EXTERNAL_MANUAL_CLOSE: "⚠️ Закрыто вручную вне QTR",
        MicroExitReason.STOP_PROTECTION_FAILED: "🚨 Ошибка защиты стопом",
    }
    return labels.get(reason, reason.value)

def _callback(action: str, trade_id: str) -> str:
    return f"qtrt:{action}:{trade_id}"


def _parse_callback(value: str) -> tuple[str, str] | None:
    parts = value.split(":", 2)
    if len(parts) != 3 or parts[0] != "qtrt":
        return None
    action, trade_id = parts[1], parts[2]
    if action not in {"hold", "close", "confirm", "cancel", "refresh"}:
        return None
    if not trade_id.startswith("QTRM-"):
        return None
    return action, trade_id


def _number(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.8f}".rstrip("0").rstrip(".")


def _money(value: float) -> str:
    return "$" + f"{value:.2f}"


def _signed_money(value: float) -> str:
    if value == 0:
        return "$0.00"
    prefix = "+" if value > 0 else "-"
    return prefix + _money(abs(value))


def _signed_r(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:+.2f}R" if value != 0 else "0.00R"


def _duration(seconds: int) -> str:
    total = max(0, seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
