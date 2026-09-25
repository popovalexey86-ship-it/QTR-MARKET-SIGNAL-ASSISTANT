from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from market_signal_assistant.qtr_micro.models import MicroExitReason, MicroStage
from market_signal_assistant.qtr_micro.runtime import (
    QtrMicroPositionEvent,
    QtrMicroPositionSnapshot,
)
from market_signal_assistant.telegram.qtr_trader import (
    QtrTraderTelegramController,
    TraderButtons,
    format_position_card,
)
from market_signal_assistant.telegram.trader_transport import TraderTelegramTransport

NOW = datetime(2026, 9, 24, 5, 0, tzinfo=UTC)


def snapshot(**changes: Any) -> QtrMicroPositionSnapshot:
    base = QtrMicroPositionSnapshot(
        trade_id="QTRM-12345678901234567890",
        symbol="BTCUSDT",
        direction="LONG",
        setup="РЕТЕСТ",
        scanner_level=100.8,
        actual_entry=101.0,
        current_price=102.0,
        initial_qty=2.0,
        current_qty=2.0,
        notional=202.0,
        leverage=5,
        initial_sl=100.0,
        current_sl=100.0,
        tp1=102.0,
        tp2=103.0,
        runner_target=104.0,
        initial_risk_usdt=2.0,
        current_pnl_est=1.8,
        current_r=0.9,
        mfe_r=1.2,
        mae_r=-0.3,
        max_r=1.2,
        opened_at=NOW - timedelta(minutes=5),
        duration_seconds=300,
        stage=MicroStage.OPEN,
        exit_price=None,
        gross_pnl=2.0,
        fees=0.2,
        net_pnl=1.8,
    )
    return replace(base, **changes)


class FakeRuntime:
    def __init__(self, current: QtrMicroPositionSnapshot) -> None:
        self.current = current
        self.close_calls: list[str] = []

    async def get_position_snapshot(
        self, trade_id: str
    ) -> QtrMicroPositionSnapshot | None:
        if trade_id != self.current.trade_id:
            return None
        return self.current

    async def request_human_close(
        self, trade_id: str
    ) -> QtrMicroPositionSnapshot | None:
        if trade_id != self.current.trade_id:
            return None
        self.close_calls.append(trade_id)
        self.current = replace(
            self.current, stage=MicroStage.EXIT_ACKNOWLEDGED
        )
        return self.current


class FakeMessenger:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str, TraderButtons]] = []
        self.edited: list[tuple[int, int, str, TraderButtons]] = []

    async def send_card(
        self, chat_id: int, text: str, buttons: TraderButtons
    ) -> int | None:
        self.sent.append((chat_id, text, buttons))
        return 77

    async def edit_card(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        buttons: TraderButtons,
    ) -> None:
        self.edited.append((chat_id, message_id, text, buttons))


class FakeQuery:
    def __init__(
        self,
        data: str,
        *,
        chat_id: int = 200,
        chat_type: str = "private",
    ) -> None:
        self.data = data
        self.message = SimpleNamespace(
            chat=SimpleNamespace(id=chat_id, type=chat_type),
            message_id=77,
        )
        self.answers: list[tuple[str | None, bool]] = []

    async def answer(
        self, text: str | None = None, show_alert: bool = False
    ) -> None:
        self.answers.append((text, show_alert))


def run_callback(
    controller: QtrTraderTelegramController, query: FakeQuery
) -> None:
    update = SimpleNamespace(callback_query=query)
    asyncio.run(controller.handle_callback(update, None))


def test_position_opened_event_sends_one_trader_card() -> None:
    current = snapshot()
    runtime = FakeRuntime(current)
    messenger = FakeMessenger()
    controller = QtrTraderTelegramController(
        runtime, messenger, frozenset({200})
    )
    event = QtrMicroPositionEvent("POSITION_OPENED", current)
    asyncio.run(controller.handle_position_event(event))
    assert len(messenger.sent) == 1
    assert messenger.sent[0][0] == 200
    assert "QTR TRADER — MICRO DEMO" in messenger.sent[0][1]
    assert "🔎 Уровень Scanner 100.8" in messenger.sent[0][1]
    assert "🛡 Риск            $2.00 = 1R" in messenger.sent[0][1]


def test_hold_is_read_only_and_never_requests_execution() -> None:
    current = snapshot()
    runtime = FakeRuntime(current)
    messenger = FakeMessenger()
    controller = QtrTraderTelegramController(
        runtime, messenger, frozenset({200})
    )
    query = FakeQuery(f"qtrt:hold:{current.trade_id}")
    run_callback(controller, query)
    assert runtime.close_calls == []
    assert messenger.edited
    assert query.answers[-1][0] == "🟢 Держим позицию — без изменений."


def test_refresh_is_read_only_and_uses_russian_ui() -> None:
    current = snapshot()
    runtime = FakeRuntime(current)
    messenger = FakeMessenger()
    controller = QtrTraderTelegramController(
        runtime, messenger, frozenset({200})
    )

    query = FakeQuery(f"qtrt:refresh:{current.trade_id}")
    run_callback(controller, query)

    assert runtime.close_calls == []
    assert messenger.edited
    assert query.answers[-1][0] == "🔄 Данные обновлены."
    buttons = messenger.edited[-1][3]
    assert "🟢 ДЕРЖАТЬ" in str(buttons)
    assert "🔴 ЗАКРЫТЬ" in str(buttons)
    assert "🔄 ОБНОВИТЬ" in str(buttons)


def test_close_is_two_step_and_repeated_confirm_is_idempotent_at_ui() -> None:
    current = snapshot()
    runtime = FakeRuntime(current)
    messenger = FakeMessenger()
    controller = QtrTraderTelegramController(
        runtime, messenger, frozenset({200}), clock=lambda: NOW
    )

    close_query = FakeQuery(f"qtrt:close:{current.trade_id}")
    run_callback(controller, close_query)
    assert runtime.close_calls == []
    assert "✅ ПОДТВЕРДИТЬ ЗАКРЫТИЕ" in str(messenger.edited[-1][3])

    confirm_query = FakeQuery(f"qtrt:confirm:{current.trade_id}")
    run_callback(controller, confirm_query)
    assert runtime.close_calls == [current.trade_id]
    assert "⏳ ЗАКРЫТИЕ ОЖИДАЕТСЯ" in messenger.edited[-1][2]

    repeated = FakeQuery(f"qtrt:confirm:{current.trade_id}")
    run_callback(controller, repeated)
    assert runtime.close_calls == [current.trade_id]
    assert repeated.answers[-1] == ("⏱ Подтверждение закрытия истекло.", True)


def test_cancel_never_requests_execution() -> None:
    current = snapshot()
    runtime = FakeRuntime(current)
    messenger = FakeMessenger()
    controller = QtrTraderTelegramController(
        runtime, messenger, frozenset({200}), clock=lambda: NOW
    )
    run_callback(
        controller, FakeQuery(f"qtrt:close:{current.trade_id}")
    )
    cancel = FakeQuery(f"qtrt:cancel:{current.trade_id}")
    run_callback(controller, cancel)
    assert runtime.close_calls == []
    assert cancel.answers[-1][0] == "↩️ Закрытие отменено."


def test_expired_confirmation_never_requests_execution() -> None:
    current = snapshot()
    runtime = FakeRuntime(current)
    messenger = FakeMessenger()
    clock = [NOW]
    controller = QtrTraderTelegramController(
        runtime,
        messenger,
        frozenset({200}),
        clock=lambda: clock[0],
    )
    run_callback(
        controller, FakeQuery(f"qtrt:close:{current.trade_id}")
    )
    clock[0] = NOW + timedelta(seconds=31)
    confirm = FakeQuery(f"qtrt:confirm:{current.trade_id}")
    run_callback(controller, confirm)
    assert runtime.close_calls == []
    assert confirm.answers[-1] == ("⏱ Подтверждение закрытия истекло.", True)


def test_execution_callbacks_require_allowlisted_private_chat() -> None:
    current = snapshot()
    runtime = FakeRuntime(current)
    messenger = FakeMessenger()
    controller = QtrTraderTelegramController(
        runtime, messenger, frozenset({200})
    )
    unknown = FakeQuery(
        f"qtrt:close:{current.trade_id}", chat_id=999, chat_type="private"
    )
    run_callback(controller, unknown)
    group = FakeQuery(
        f"qtrt:close:{current.trade_id}", chat_id=200, chat_type="group"
    )
    run_callback(controller, group)
    assert runtime.close_calls == []
    assert unknown.answers[-1] == ("Доступ запрещён.", True)
    assert group.answers[-1] == ("Доступ запрещён.", True)


def test_closed_card_without_reason_is_explicit_not_generic_english_closed() -> None:
    closed = snapshot(
        stage=MicroStage.CLOSED,
        current_qty=0.0,
        exit_price=101.5,
    )

    text, buttons = format_position_card(closed)

    assert "🏁 QTR TRADER — ПОЗИЦИЯ ЗАКРЫТА" in text
    assert "📌 Причина: не указана" in text
    assert "Reason:" not in text
    assert "POSITION CLOSED" not in text
    assert buttons == ()


def test_closed_card_uses_factual_exit_and_human_close_reason() -> None:
    closed = snapshot(
        stage=MicroStage.CLOSED,
        current_qty=0.0,
        current_price=102.25,
        exit_price=102.25,
        gross_pnl=2.5,
        fees=0.25,
        net_pnl=2.25,
        current_pnl_est=2.25,
        current_r=1.125,
    )
    text, buttons = format_position_card(
        closed, exit_reason=MicroExitReason.HUMAN_CLOSE
    )
    assert "📌 Причина: 👤 Закрыто через QTR Trader" in text
    assert "🏁 Выход           102.25" in text
    assert "💰 Итоговый PnL    +$2.25" in text
    assert "📊 Результат       +1.12R" in text
    assert buttons == ()


class FakeUpdater:
    def __init__(self) -> None:
        self.running = False
        self.started = False
        self.stopped = False

    async def start_polling(self, **kwargs: Any) -> None:
        assert kwargs["drop_pending_updates"] is True
        self.started = True
        self.running = True

    async def stop(self) -> None:
        self.stopped = True
        self.running = False


class FakeApplication:
    def __init__(self) -> None:
        self.updater = FakeUpdater()
        self.bot = SimpleNamespace()
        self.running = False
        self.initialized = False
        self.shutdown_called = False

    async def initialize(self) -> None:
        self.initialized = True

    async def start(self) -> None:
        assert self.updater.started
        self.running = True

    async def stop(self) -> None:
        self.running = False

    async def shutdown(self) -> None:
        self.shutdown_called = True


def test_separate_trader_application_polls_without_second_runtime() -> None:
    application = FakeApplication()
    callback_seen: list[bool] = []

    async def callback(update: Any, context: Any) -> None:
        del update, context
        callback_seen.append(True)

    transport = TraderTelegramTransport(
        "trader-token",
        application_factory=lambda token, handler: application,
    )
    transport.set_callback_handler(callback)

    async def exercise() -> None:
        assert await transport.start()
        await transport.close()

    asyncio.run(exercise())
    assert application.initialized
    assert application.updater.started
    assert application.updater.stopped
    assert application.shutdown_called
    assert callback_seen == []
