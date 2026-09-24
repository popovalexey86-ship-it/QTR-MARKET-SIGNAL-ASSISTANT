from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Protocol

_LOGGER = logging.getLogger(__name__)
_TIMEOUT_SECONDS = 5.0


class TraderBot(Protocol):
    async def initialize(self) -> None: ...

    async def send_message(self, *, chat_id: int, text: str) -> object: ...

    async def shutdown(self) -> None: ...


def _build_bot(token: str) -> TraderBot:
    from telegram import Bot

    return Bot(token)


class TraderTelegramTransport:
    """One outbound Bot API client; never polls or propagates delivery failures."""

    def __init__(
        self,
        bot_token: str,
        *,
        bot_factory: Callable[[str], TraderBot] = _build_bot,
        timeout_seconds: float = _TIMEOUT_SECONDS,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("Trader Telegram timeout must be positive.")
        self._bot_token = bot_token
        self._bot_factory = bot_factory
        self._timeout_seconds = timeout_seconds
        self._bot: TraderBot | None = None
        self._unavailable_logged = False

    async def start(self) -> bool:
        if not self._bot_token:
            _LOGGER.warning(
                "QTR Trader Telegram не настроен; Micro delivery отключена."
            )
            return False
        if self._bot is not None:
            return True
        bot: TraderBot | None = None
        try:
            bot = self._bot_factory(self._bot_token)
            await asyncio.wait_for(bot.initialize(), timeout=self._timeout_seconds)
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram startup failed (%s).", type(error).__name__
            )
            if bot is not None:
                await self._shutdown(bot)
            return False
        self._bot = bot
        self._unavailable_logged = False
        return True

    async def send(self, chat_id: int, text: str) -> None:
        bot = self._bot
        if bot is None:
            if not self._unavailable_logged:
                _LOGGER.warning("QTR Trader Telegram delivery unavailable.")
                self._unavailable_logged = True
            return
        try:
            await asyncio.wait_for(
                bot.send_message(chat_id=chat_id, text=text),
                timeout=self._timeout_seconds,
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram delivery failed for chat %d (%s).",
                chat_id,
                type(error).__name__,
            )

    async def close(self) -> None:
        bot = self._bot
        self._bot = None
        if bot is not None:
            await self._shutdown(bot)

    async def _shutdown(self, bot: TraderBot) -> None:
        try:
            await asyncio.wait_for(bot.shutdown(), timeout=self._timeout_seconds)
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram shutdown failed (%s).", type(error).__name__
            )
