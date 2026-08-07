from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from market_signal_assistant.application.models import ScreeningReport, ScreeningRequest
from market_signal_assistant.application.presentation import ReportView, present_report
from market_signal_assistant.inplay.audit import ScanSource
from market_signal_assistant.inplay.early_discovery import EarlyDiscoveryService
from market_signal_assistant.inplay.models import InPlayReport
from market_signal_assistant.inplay.notifications import InPlayNotificationService
from market_signal_assistant.localized_argparse import RussianArgumentParser
from market_signal_assistant.news.models import NewsReport
from market_signal_assistant.news.notifications import NewsNotificationService
from market_signal_assistant.news.provider import NewsDataError
from market_signal_assistant.settings import (
    EarlyDiscoverySettings,
    InPlayAutoSettings,
    InPlayTimingAuditSettings,
    LiveDerivativesSettings,
    NewsAutoSettings,
    NewsSettings,
    TelegramSettings,
)
from market_signal_assistant.telegram.formatting import (
    NEWS_ERROR_MESSAGE,
    format_alarm_report,
    format_inplay_report,
    format_news_report,
    format_report,
    format_screen_report,
)
from market_signal_assistant.telegram.inplay_auto import (
    InPlayAutoLoop,
    InPlayAutoNotifier,
)
from market_signal_assistant.telegram.inplay_early_discovery import (
    EarlyDiscoveryLoop,
    EarlyDiscoveryRunner,
)
from market_signal_assistant.telegram.inplay_timing_audit import (
    InPlayTimingAuditLoop,
    InPlayTimingAuditRunner,
)
from market_signal_assistant.telegram.news_auto import NewsAutoLoop, NewsAutoNotifier
from market_signal_assistant.telegram.parsing import ParsedCommand, parse_command

_LOGGER = logging.getLogger(__name__)

HELP_TEXT = """Информационный скринер.
/screen BTCUSDT ETHUSDT interval=1h min_score=60
/crypto — криптовалюты
/inplay — активные USDT-криптовалюты для наблюдения
/news — важные официальные объявления Bybit
/markets — несколько классов активов
/status — локальный статус
Информационный анализ, не торговая рекомендация."""


class ScreeningService(Protocol):
    def screen(self, request: ScreeningRequest) -> ScreeningReport: ...


class InPlayScanner(Protocol):
    def scan(
        self,
        maximum_results: int = 10,
        *,
        scan_source: ScanSource = "manual",
    ) -> InPlayReport: ...


class NewsReader(Protocol):
    def get_important(self) -> NewsReport: ...


@dataclass(frozen=True, slots=True)
class TelegramExecution:
    command: ParsedCommand
    report: ScreeningReport | None
    view: ReportView | None
    messages: tuple[str, ...]


TelegramSdk = tuple[Any, Any, Any]


def is_allowed(
    chat_id: int,
    allowed_chat_ids: frozenset[int],
    *,
    allow_all: bool = False,
) -> bool:
    return allow_all or chat_id in allowed_chat_ids


def execute_command(
    text: str,
    *,
    chat_id: int,
    allowed_chat_ids: frozenset[int],
    service: ScreeningService,
    inplay_service: InPlayScanner | None = None,
    live_active: bool = False,
    allow_all: bool = False,
    inplay_auto_enabled: bool = False,
    inplay_scan_interval_minutes: int = 15,
    news_service: NewsReader | None = None,
    news_enabled: bool = True,
    news_auto_enabled: bool = False,
    news_scan_interval_minutes: int = 60,
) -> TelegramExecution:
    if not is_allowed(chat_id, allowed_chat_ids, allow_all=allow_all):
        raise PermissionError("Chat is not allowed.")
    command = parse_command(text)
    if command.name in {"start", "help"}:
        return TelegramExecution(command, None, None, (HELP_TEXT,))
    if command.name == "status":
        stream_status = "включён" if live_active else "отключён"
        return TelegramExecution(
            command,
            None,
            None,
            (
                "Статус: готов\n"
                f"Онлайн-поток ликвидаций: {stream_status}\n"
                "Режим деривативов: REST\n"
                "Автосканирование IN PLAY: "
                f"{'включено' if inplay_auto_enabled else 'отключено'}\n"
                f"Интервал IN PLAY: {inplay_scan_interval_minutes} минут\n"
                "Автоновости: "
                f"{'включены' if news_auto_enabled else 'отключены'}\n"
                f"Интервал новостей: {news_scan_interval_minutes} минут",
            ),
        )
    if command.name == "inplay":
        if inplay_service is None:
            raise ValueError("Сервис IN PLAY недоступен.")
        inplay_report = inplay_service.scan(
            maximum_results=10,
            scan_source="manual",
        )
        return TelegramExecution(
            command,
            None,
            None,
            format_inplay_report(inplay_report),
        )
    if command.name == "news":
        if not news_enabled:
            return TelegramExecution(
                command,
                None,
                None,
                ("📰 Новостной модуль отключён.",),
            )
        if news_service is None:
            raise ValueError("Сервис новостей недоступен.")
        try:
            news_report = news_service.get_important()
        except NewsDataError as error:
            _LOGGER.warning(
                "Не удалось получить новости Bybit (%s).",
                type(error).__name__,
            )
            return TelegramExecution(command, None, None, (NEWS_ERROR_MESSAGE,))
        return TelegramExecution(
            command,
            None,
            None,
            format_news_report(news_report),
        )
    if command.request is None:
        raise ValueError("Screening request is missing.")
    report = service.screen(command.request)
    view = present_report(report)
    if command.name == "screen":
        messages = format_screen_report(view)
    elif command.name == "crypto":
        messages = format_alarm_report(view)
    else:
        messages = format_report(view)
    return TelegramExecution(command, report, view, messages)


def main(argv: Sequence[str] | None = None) -> None:
    parser = RussianArgumentParser(
        description="Telegram-бот информационного помощника по рынку."
    )
    parser.parse_args(argv)
    telegram_settings = TelegramSettings.from_environment()
    live_settings = LiveDerivativesSettings.from_environment()
    auto_settings = InPlayAutoSettings.from_environment()
    timing_audit_settings = InPlayTimingAuditSettings.from_environment()
    early_discovery_settings = EarlyDiscoverySettings.from_environment()
    news_settings = NewsSettings.from_environment()
    news_auto_settings = NewsAutoSettings.from_environment()
    sdk = _load_telegram_sdk()
    from market_signal_assistant.composition import (
        build_early_discovery_service,
        build_inplay_notification_service,
        build_inplay_service,
        build_news_notification_service,
        build_news_service,
        build_screening_service,
    )

    service, derivatives = build_screening_service()
    inplay_service = build_inplay_service(
        derivatives=derivatives,
        audit_settings=timing_audit_settings,
    )
    notification_service = build_inplay_notification_service()
    early_discovery_service = build_early_discovery_service(
        early_discovery_settings,
        inplay_evaluator=inplay_service,
    )
    news_service = build_news_service(news_settings)
    news_notification_service = build_news_notification_service(news_settings)
    if live_settings.enabled:
        derivatives.stream.start(list(live_settings.symbols))
    try:
        _run_sdk_bot(
            telegram_settings,
            service,
            inplay_service,
            derivatives.stream.running,
            auto_settings,
            notification_service,
            news_service,
            news_settings.enabled,
            news_auto_settings,
            news_notification_service,
            timing_audit_settings=timing_audit_settings,
            early_discovery_settings=early_discovery_settings,
            early_discovery_service=early_discovery_service,
            sdk=sdk,
        )
    finally:
        derivatives.stream.stop()


def _run_sdk_bot(
    settings: TelegramSettings,
    service: ScreeningService,
    inplay_service: InPlayScanner,
    live_active: bool,
    auto_settings: InPlayAutoSettings,
    notification_service: InPlayNotificationService,
    news_service: NewsReader,
    news_enabled: bool,
    news_auto_settings: NewsAutoSettings,
    news_notification_service: NewsNotificationService,
    *,
    timing_audit_settings: InPlayTimingAuditSettings | None = None,
    early_discovery_settings: EarlyDiscoverySettings | None = None,
    early_discovery_service: EarlyDiscoveryService | None = None,
    sdk: TelegramSdk | None = None,
) -> None:
    ApplicationBuilder, MessageHandler, filters = sdk or _load_telegram_sdk()
    _run_sdk_bot_handlers(
        settings,
        service,
        inplay_service,
        live_active,
        auto_settings,
        notification_service,
        ApplicationBuilder,
        MessageHandler,
        filters,
        news_service=news_service,
        news_enabled=news_enabled,
        news_auto_settings=news_auto_settings,
        news_notification_service=news_notification_service,
        timing_audit_settings=timing_audit_settings,
        early_discovery_settings=early_discovery_settings,
        early_discovery_service=early_discovery_service,
    )


def _load_telegram_sdk() -> TelegramSdk:
    try:
        from telegram.ext import (
            ApplicationBuilder,
            MessageHandler,
            filters,
        )
    except ImportError:
        raise RuntimeError(
            "Для Telegram требуется optional dependency 'telegram'."
        ) from None
    return ApplicationBuilder, MessageHandler, filters


def _run_sdk_bot_handlers(
    settings: TelegramSettings,
    service: ScreeningService,
    inplay_service: InPlayScanner,
    live_active: bool,
    auto_settings: InPlayAutoSettings,
    notification_service: InPlayNotificationService,
    ApplicationBuilder: Any,
    MessageHandler: Any,
    filters: Any,
    news_service: NewsReader | None = None,
    news_enabled: bool = True,
    news_auto_settings: NewsAutoSettings | None = None,
    news_notification_service: NewsNotificationService | None = None,
    timing_audit_settings: InPlayTimingAuditSettings | None = None,
    early_discovery_settings: EarlyDiscoverySettings | None = None,
    early_discovery_service: EarlyDiscoveryService | None = None,
) -> None:
    resolved_news_auto = news_auto_settings or NewsAutoSettings()
    resolved_timing_audit = timing_audit_settings or InPlayTimingAuditSettings()
    resolved_early_discovery = (
        early_discovery_settings or EarlyDiscoverySettings()
    )

    async def handle(update: Any, context: Any) -> None:
        del context
        chat = update.effective_chat
        message = update.effective_message
        if chat is None or message is None or message.text is None:
            return
        try:
            result = await asyncio.to_thread(
                execute_command,
                message.text,
                chat_id=chat.id,
                allowed_chat_ids=settings.allowed_chat_ids,
                service=service,
                inplay_service=inplay_service,
                live_active=live_active,
                allow_all=settings.allow_all,
                inplay_auto_enabled=auto_settings.enabled,
                inplay_scan_interval_minutes=auto_settings.interval_minutes,
                news_service=news_service,
                news_enabled=news_enabled,
                news_auto_enabled=resolved_news_auto.enabled,
                news_scan_interval_minutes=resolved_news_auto.interval_minutes,
            )
            replies = result.messages
        except PermissionError:
            replies = ("Доступ запрещён.",)
        except ValueError as error:
            replies = (f"Ошибка команды: {error}",)
        for reply in replies:
            await message.reply_text(reply)

    auto_loop: InPlayAutoLoop | None = None
    timing_audit_loop: InPlayTimingAuditLoop | None = None
    early_discovery_loop: EarlyDiscoveryLoop | None = None
    news_auto_loop: NewsAutoLoop | None = None

    async def start_auto(application: Any) -> None:
        nonlocal auto_loop, timing_audit_loop, early_discovery_loop, news_auto_loop

        async def send(chat_id: int, text: str) -> None:
            await application.bot.send_message(chat_id=chat_id, text=text)

        if auto_settings.enabled:
            notifier = InPlayAutoNotifier(
                scanner=inplay_service,
                notification_service=notification_service,
                allowed_chat_ids=settings.allowed_chat_ids,
            )
            auto_loop = InPlayAutoLoop(
                notifier,
                send,
                interval_seconds=auto_settings.interval_minutes * 60,
            )
            auto_loop.start()
        if resolved_timing_audit.enabled and resolved_timing_audit.auto_enabled:
            timing_audit_loop = InPlayTimingAuditLoop(
                InPlayTimingAuditRunner(inplay_service),
                interval_seconds=resolved_timing_audit.interval_minutes * 60,
            )
            timing_audit_loop.start()
        if resolved_early_discovery.enabled and early_discovery_service is not None:
            early_discovery_loop = EarlyDiscoveryLoop(
                EarlyDiscoveryRunner(early_discovery_service),
                interval_seconds=resolved_early_discovery.interval_minutes * 60,
            )
            early_discovery_loop.start()
        if (
            resolved_news_auto.enabled
            and news_service is not None
            and news_notification_service is not None
        ):
            news_notifier = NewsAutoNotifier(
                reader=news_service,
                notification_service=news_notification_service,
                allowed_chat_ids=settings.allowed_chat_ids,
            )
            news_auto_loop = NewsAutoLoop(
                news_notifier,
                send,
                interval_seconds=resolved_news_auto.interval_minutes * 60,
            )
            news_auto_loop.start()

    async def stop_auto(application: Any) -> None:
        del application
        if timing_audit_loop is not None:
            await timing_audit_loop.stop()
        if early_discovery_loop is not None:
            await early_discovery_loop.stop()
        if news_auto_loop is not None:
            await news_auto_loop.stop()
        if auto_loop is not None:
            await auto_loop.stop()

    builder = ApplicationBuilder().token(settings.bot_token)
    lifecycle_enabled = False
    if auto_settings.enabled and not settings.allowed_chat_ids:
        _LOGGER.warning(
            "Автосканирование IN PLAY не запущено: "
            "TELEGRAM_ALLOWED_CHAT_IDS не задан."
        )
    elif auto_settings.enabled:
        lifecycle_enabled = True
    if resolved_timing_audit.enabled and resolved_timing_audit.auto_enabled:
        lifecycle_enabled = True
    if resolved_early_discovery.enabled:
        if early_discovery_service is None:
            _LOGGER.warning(
                "Early Discovery не запущен: diagnostic service недоступен."
            )
        else:
            lifecycle_enabled = True
    if resolved_news_auto.enabled and not settings.allowed_chat_ids:
        _LOGGER.warning(
            "Автоновости не запущены: TELEGRAM_ALLOWED_CHAT_IDS не задан."
        )
    elif resolved_news_auto.enabled:
        if news_service is None or news_notification_service is None:
            _LOGGER.warning(
                "Автоновости не запущены: news service недоступен."
            )
        else:
            lifecycle_enabled = True
    if lifecycle_enabled:
        builder = builder.post_init(start_auto).post_shutdown(stop_auto)
    application = builder.build()
    application.add_handler(MessageHandler(filters.COMMAND, handle))
    application.run_polling()


if __name__ == "__main__":
    main()
