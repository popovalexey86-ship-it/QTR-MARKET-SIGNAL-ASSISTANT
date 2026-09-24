from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from market_signal_assistant.settings import TelegramSettings, TraderTelegramSettings
from market_signal_assistant.telegram.bot import (
    _discard_trader_message,
    _micro_candidate_handler,
)
from market_signal_assistant.telegram.trader_transport import TraderTelegramTransport


class FakeTraderBot:
    def __init__(self, *, failing_chat: int | None = None) -> None:
        self.failing_chat = failing_chat
        self.started = False
        self.closed = False
        self.messages: list[tuple[int, str]] = []

    async def initialize(self) -> None:
        self.started = True

    async def shutdown(self) -> None:
        self.closed = True

    async def send_message(self, *, chat_id: int, text: str) -> object:
        if chat_id == self.failing_chat:
            raise RuntimeError("delivery failed")
        self.messages.append((chat_id, text))
        return object()


def test_trader_settings_never_fall_back_to_scanner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "scanner-secret")
    monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "100")
    monkeypatch.delenv("QTR_TRADER_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("QTR_TRADER_TELEGRAM_ALLOWED_CHAT_IDS", raising=False)
    missing = TraderTelegramSettings.from_environment()
    assert not missing.configured
    assert missing.allowed_chat_ids == frozenset()
    assert "scanner-secret" not in repr(missing)
    monkeypatch.setenv("QTR_TRADER_TELEGRAM_BOT_TOKEN", "trader-secret")
    monkeypatch.setenv("QTR_TRADER_TELEGRAM_ALLOWED_CHAT_IDS", "200,201")
    configured = TraderTelegramSettings.from_environment()
    assert configured.configured
    assert configured.bot_token == "trader-secret"
    assert configured.allowed_chat_ids == frozenset({200, 201})
    assert "trader-secret" not in repr(configured)
    scanner = TelegramSettings.from_environment()
    assert scanner.bot_token == "scanner-secret"
    assert scanner.allowed_chat_ids == frozenset({100})


def test_partial_or_invalid_trader_config_never_uses_scanner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "scanner-secret")
    monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "100")
    monkeypatch.setenv("QTR_TRADER_TELEGRAM_BOT_TOKEN", "trader-secret")
    monkeypatch.delenv("QTR_TRADER_TELEGRAM_ALLOWED_CHAT_IDS", raising=False)
    assert not TraderTelegramSettings.from_environment().configured
    monkeypatch.setenv("QTR_TRADER_TELEGRAM_ALLOWED_CHAT_IDS", "not-a-number")
    with pytest.raises(ValueError, match="QTR_TRADER_TELEGRAM_ALLOWED_CHAT_IDS"):
        TraderTelegramSettings.from_environment()


def test_transport_is_outbound_only_and_isolates_chat_errors() -> None:
    bot = FakeTraderBot(failing_chat=200)
    transport = TraderTelegramTransport("trader-token", bot_factory=lambda token: bot)

    async def exercise() -> None:
        assert await transport.start()
        await transport.send(200, "ENTRY")
        await transport.send(201, "TP1")
        await transport.close()

    asyncio.run(exercise())
    assert bot.started and bot.closed
    assert bot.messages == [(201, "TP1")]


def test_micro_entry_tp_and_closed_only_use_trader_sender() -> None:
    trader_calls: list[tuple[int, str]] = []
    scanner_calls: list[tuple[int, str]] = []

    class FakeRuntime:
        async def handle_candidates(
            self,
            candidates: tuple[object, ...],
            send: Callable[[int, str], Awaitable[None]],
        ) -> None:
            assert candidates == ("candidate",)
            for event in ("ENTRY", "TP1", "CLOSED/TIME_EXIT"):
                await send(201, event)

    async def trader_send(chat_id: int, text: str) -> None:
        trader_calls.append((chat_id, text))

    async def scanner_send(chat_id: int, text: str) -> None:
        scanner_calls.append((chat_id, text))

    handler = _micro_candidate_handler(FakeRuntime().handle_candidates, trader_send)
    asyncio.run(handler(("candidate",), scanner_send))
    assert scanner_calls == []
    assert trader_calls == [
        (201, "ENTRY"),
        (201, "TP1"),
        (201, "CLOSED/TIME_EXIT"),
    ]


def test_missing_trader_transport_never_falls_back_to_scanner() -> None:
    scanner_calls: list[tuple[int, str]] = []

    async def fake_micro(
        candidates: tuple[object, ...],
        send: Callable[[int, str], Awaitable[None]],
    ) -> None:
        assert candidates == ("candidate",)
        await send(100, "ENTRY")

    async def scanner_send(chat_id: int, text: str) -> None:
        scanner_calls.append((chat_id, text))

    handler = _micro_candidate_handler(fake_micro, _discard_trader_message)
    asyncio.run(handler(("candidate",), scanner_send))
    assert scanner_calls == []


def test_trader_failure_does_not_abort_micro_cycle_or_other_chat() -> None:
    bot = FakeTraderBot(failing_chat=200)
    transport = TraderTelegramTransport("trader-token", bot_factory=lambda token: bot)
    completed: list[str] = []

    async def fake_micro(
        candidates: tuple[object, ...],
        send: Callable[[int, str], Awaitable[None]],
    ) -> None:
        assert candidates == ("candidate",)
        await send(200, "ENTRY")
        await send(201, "ENTRY")
        completed.append("trade cycle continued")

    async def scanner_send(chat_id: int, text: str) -> None:
        raise AssertionError((chat_id, text))

    async def exercise() -> None:
        assert await transport.start()
        handler = _micro_candidate_handler(fake_micro, transport.send)
        await handler(("candidate",), scanner_send)
        await transport.close()

    asyncio.run(exercise())
    assert completed == ["trade cycle continued"]
    assert bot.messages == [(201, "ENTRY")]


def test_trader_startup_failure_isolated_and_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingBot(FakeTraderBot):
        async def initialize(self) -> None:
            raise RuntimeError("no connection")

    bot = FailingBot()
    transport = TraderTelegramTransport("trader-token", bot_factory=lambda token: bot)

    async def exercise() -> None:
        assert not await transport.start()
        await transport.send(201, "ENTRY")
        await transport.close()

    asyncio.run(exercise())
    assert bot.closed
    assert bot.messages == []
    assert "trader-token" not in caplog.text


def test_trader_delivery_timeout_is_bounded() -> None:
    class SlowBot(FakeTraderBot):
        async def send_message(self, *, chat_id: int, text: str) -> object:
            await asyncio.sleep(10)
            return await super().send_message(chat_id=chat_id, text=text)

    bot = SlowBot()
    transport = TraderTelegramTransport(
        "trader-token", bot_factory=lambda token: bot, timeout_seconds=0.01
    )

    async def exercise() -> None:
        assert await transport.start()
        await transport.send(201, "ENTRY")
        await transport.close()

    asyncio.run(asyncio.wait_for(exercise(), timeout=1))
    assert bot.closed
    assert bot.messages == []
