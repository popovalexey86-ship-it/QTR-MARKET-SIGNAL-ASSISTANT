from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from market_signal_assistant.telegram.qtr_trader import TraderButtons

_LOGGER = logging.getLogger(__name__)
_TIMEOUT_SECONDS = 5.0
TraderCallback = Callable[[Any, Any], Awaitable[None]]


class TraderBot(Protocol):
    async def initialize(self) -> None: ...

    async def send_message(self, *, chat_id: int, text: str) -> object: ...

    async def shutdown(self) -> None: ...


def _build_application(
    token: str, callback_handler: TraderCallback | None
) -> Any:
    from telegram.ext import ApplicationBuilder, CallbackQueryHandler

    application = ApplicationBuilder().token(token).build()
    if callback_handler is not None:
        callback: Any = callback_handler
        application.add_handler(
            CallbackQueryHandler(callback, pattern=r"^qtrt:")
        )
    return application


class TraderTelegramTransport:
    """Separate Trader bot identity with bounded polling in the Scanner process."""

    def __init__(
        self,
        bot_token: str,
        *,
        bot_factory: Callable[[str], TraderBot] | None = None,
        application_factory: Callable[[str, TraderCallback | None], Any] = (
            _build_application
        ),
        timeout_seconds: float = _TIMEOUT_SECONDS,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("Trader Telegram timeout must be positive.")
        self._bot_token = bot_token
        self._bot_factory = bot_factory
        self._application_factory = application_factory
        self._timeout_seconds = timeout_seconds
        self._bot: TraderBot | None = None
        self._application: Any | None = None
        self._callback_handler: TraderCallback | None = None
        self._unavailable_logged = False

    def set_callback_handler(
        self, handler: TraderCallback | None
    ) -> None:
        if self._bot is not None or self._application is not None:
            raise RuntimeError("Trader callback handler must be set before start.")
        self._callback_handler = handler

    async def start(self) -> bool:
        if not self._bot_token:
            _LOGGER.warning(
                "QTR Trader Telegram не настроен; Micro delivery отключена."
            )
            return False
        if self._bot is not None or self._application is not None:
            return True
        if self._bot_factory is not None:
            return await self._start_legacy_bot()
        return await self._start_application()

    async def send(self, chat_id: int, text: str) -> None:
        bot = self._active_bot()
        if bot is None:
            self._log_unavailable_once()
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

    async def send_card(
        self,
        chat_id: int,
        text: str,
        buttons: TraderButtons,
    ) -> int | None:
        bot = self._active_bot()
        if bot is None:
            self._log_unavailable_once()
            return None
        try:
            message = await asyncio.wait_for(
                bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=_reply_markup(buttons),
                ),
                timeout=self._timeout_seconds,
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram card delivery failed for chat %d (%s).",
                chat_id,
                type(error).__name__,
            )
            return None
        message_id = getattr(message, "message_id", None)
        return int(message_id) if isinstance(message_id, int) else None

    async def edit_card(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        buttons: TraderButtons,
    ) -> None:
        bot = self._active_bot()
        if bot is None:
            self._log_unavailable_once()
            return
        try:
            await asyncio.wait_for(
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=_reply_markup(buttons),
                ),
                timeout=self._timeout_seconds,
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram card edit failed for chat %d (%s).",
                chat_id,
                type(error).__name__,
            )

    async def close(self) -> None:
        application = self._application
        self._application = None
        if application is not None:
            await self._shutdown_application(application)
        bot = self._bot
        self._bot = None
        if bot is not None:
            await self._shutdown_bot(bot)

    async def _start_legacy_bot(self) -> bool:
        assert self._bot_factory is not None
        bot: TraderBot | None = None
        try:
            bot = self._bot_factory(self._bot_token)
            await asyncio.wait_for(
                bot.initialize(), timeout=self._timeout_seconds
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram startup failed (%s).",
                type(error).__name__,
            )
            if bot is not None:
                await self._shutdown_bot(bot)
            return False
        self._bot = bot
        self._unavailable_logged = False
        return True

    async def _start_application(self) -> bool:
        application: Any | None = None
        try:
            application = self._application_factory(
                self._bot_token, self._callback_handler
            )
            await asyncio.wait_for(
                application.initialize(), timeout=self._timeout_seconds
            )
            updater = application.updater
            if updater is None:
                raise RuntimeError("Trader Telegram updater is unavailable.")
            await asyncio.wait_for(
                updater.start_polling(drop_pending_updates=True),
                timeout=self._timeout_seconds,
            )
            await asyncio.wait_for(
                application.start(), timeout=self._timeout_seconds
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram polling startup failed (%s).",
                type(error).__name__,
            )
            if application is not None:
                await self._shutdown_application(application)
            return False
        self._application = application
        self._unavailable_logged = False
        return True

    def _active_bot(self) -> Any | None:
        if self._application is not None:
            return self._application.bot
        return self._bot

    def _log_unavailable_once(self) -> None:
        if not self._unavailable_logged:
            _LOGGER.warning("QTR Trader Telegram delivery unavailable.")
            self._unavailable_logged = True

    async def _shutdown_application(self, application: Any) -> None:
        try:
            updater = application.updater
            if updater is not None and getattr(updater, "running", False):
                await asyncio.wait_for(
                    updater.stop(), timeout=self._timeout_seconds
                )
            if getattr(application, "running", False):
                await asyncio.wait_for(
                    application.stop(), timeout=self._timeout_seconds
                )
            await asyncio.wait_for(
                application.shutdown(), timeout=self._timeout_seconds
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram application shutdown failed (%s).",
                type(error).__name__,
            )

    async def _shutdown_bot(self, bot: TraderBot) -> None:
        try:
            await asyncio.wait_for(
                bot.shutdown(), timeout=self._timeout_seconds
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader Telegram shutdown failed (%s).",
                type(error).__name__,
            )


def _reply_markup(buttons: TraderButtons) -> object | None:
    if not buttons:
        return None
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(label, callback_data=data)
                for label, data in row
            ]
            for row in buttons
        ]
    )
