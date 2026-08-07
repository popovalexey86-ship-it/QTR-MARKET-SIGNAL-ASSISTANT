from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from market_signal_assistant.application.presentation import (
    ReportView,
    SignalView,
    format_number,
)
from market_signal_assistant.inplay.models import (
    InPlayDirection,
    InPlayReport,
    InPlayResult,
)
from market_signal_assistant.inplay.safety import (
    AutomaticDisplayStatus,
    AutomaticInPlaySemantics,
    automatic_semantics,
)
from market_signal_assistant.news.models import (
    NewsAssetType,
    NewsCategory,
    NewsImportance,
    NewsItem,
    NewsReport,
)
from market_signal_assistant.news.notifications import (
    NewsNotificationDecision,
    NewsNotificationDecisionKind,
    NewsNotificationRecord,
)

TELEGRAM_MESSAGE_LIMIT = 4096
SAFE_MESSAGE_LIMIT = 3900
ALARM_MINIMUM_FINAL_SCORE = 60.0
ALARM_MINIMUM_TECHNICAL_SCORE = 70.0
ALARM_MINIMUM_CONFIDENCE = 70.0
ALARM_MINIMUM_CONFIRMATIONS = 3
NO_STRONG_SIGNALS_MESSAGE = "Сильных торговых сигналов сейчас нет."
ALARM_DISCLAIMER = "Информационный сигнал, не торговая рекомендация."
NO_INPLAY_RESULTS_MESSAGE = "Активных IN PLAY монет сейчас нет."
NEWS_ERROR_MESSAGE = (
    "⚠️ Не удалось получить новости Bybit.\n"
    "Попробуйте повторить команду позже."
)
AUTO_NEWS_HEADER = "🚨 ВАЖНЫЕ НОВОСТИ РЫНКА"

_NUMBER_IN_REASON = re.compile(r"[+−-]?\d+(?:[.,]\d+)?")


@dataclass(frozen=True, slots=True)
class _InPlayMetrics:
    price_change: float | None
    volatility: float | None
    relative_volume: float | None


def format_report(view: ReportView) -> tuple[str, ...]:
    sections = [
        _signal_section(index, signal)
        for index, signal in enumerate(view.ranked_signals, start=1)
    ]
    sections.extend(
        f"Ошибка {item.symbol} ({item.stage}): {item.message}"
        for item in view.failed_instruments
    )
    if not sections:
        sections.append("Подходящих сигналов не найдено.")
    sections.append(view.disclaimer)
    return split_messages("\n\n".join(sections))


def format_alarm_report(view: ReportView) -> tuple[str, ...]:
    """Render only strong directional signals for /screen and /crypto.

    ``combined_score`` is signed, so SHORT strength is its absolute value.
    """
    signals = _alarm_signals(view)
    if not signals:
        return (NO_STRONG_SIGNALS_MESSAGE,)
    sections = tuple(_alarm_signal_section(signal) for signal in signals)
    return split_messages("\n\n".join(sections))


def format_screen_report(view: ReportView) -> tuple[str, ...]:
    """Render /screen results without treating technical failures as weak signals."""
    signals = _alarm_signals(view)
    sections = [_alarm_signal_section(signal) for signal in signals]
    failed_symbols = tuple(
        dict.fromkeys(
            item.symbol
            for item in view.failed_instruments
            if item.stage == "технический анализ"
        )
    )
    if failed_symbols:
        sections.append(
            "\n".join(
                f"{symbol}: инструмент не найден или рыночные данные недоступны."
                for symbol in failed_symbols
            )
        )
    if not signals and view.successful_results:
        sections.append(NO_STRONG_SIGNALS_MESSAGE)
    if not sections:
        sections.append(NO_STRONG_SIGNALS_MESSAGE)
    return split_messages("\n\n".join(sections))


def format_inplay_report(report: InPlayReport) -> tuple[str, ...]:
    if not report.results:
        return (NO_INPLAY_RESULTS_MESSAGE,)
    sections = ["🔥 IN PLAY"]
    sections.extend(
        _inplay_section(index, item)
        for index, item in enumerate(report.results[:10], start=1)
    )
    return split_messages("\n\n".join(sections))


def format_auto_inplay_results(results: tuple[InPlayResult, ...]) -> str:
    """Render one combined automatic alert with at most three instruments."""
    if not results:
        raise ValueError("Automatic IN PLAY notification requires results.")
    return "\n\n".join(_auto_inplay_section(item) for item in results[:3])


def format_news_report(report: NewsReport) -> tuple[str, ...]:
    if not report.items:
        period = _hours_text(report.lookback_hours)
        return (f"📰 Важных новостей за последние {period} не найдено.",)
    sections = ["📰 ВАЖНЫЕ НОВОСТИ"]
    sections.extend(
        _news_section(item, report.generated_at) for item in report.items[:10]
    )
    return split_messages("\n\n".join(sections))


def format_auto_news_event(
    decision: NewsNotificationDecision,
    previous: NewsNotificationRecord | None,
    generated_at: datetime,
) -> str:
    """Render one whole automatic event so batching never splits it."""
    item = decision.item
    category = _news_category_label_for_item(item)
    subject = ", ".join(item.symbols) if item.symbols else item.title
    if decision.kind is NewsNotificationDecisionKind.INITIAL:
        lines = [
            f"🟠 НОВОЕ — {category}",
            "",
            subject,
            item.description,
            "",
            f"🕒 Опубликовано: {_relative_time(item.published_at, generated_at)}",
        ]
        if item.event_starts_at is not None:
            lines.append(f"⏰ Начало события: {_utc_text(item.event_starts_at)}")
        elif (
            item.category is NewsCategory.NETWORK
            and item.event_start_date is not None
        ):
            lines.append(
                "⏰ Ограничение начинается: "
                f"{item.event_start_date.isoformat()}, точное время не указано."
            )
        lines.extend(_automatic_news_context(item))
        return "\n".join(lines)
    if decision.kind is NewsNotificationDecisionKind.UPDATED:
        lines = [f"🔄 ОБНОВЛЕНО — {category}", "", subject, ""]
        if (
            previous is not None
            and previous.event_starts_at != item.event_starts_at
            and (
                previous.event_starts_at is not None
                or item.event_starts_at is not None
            )
        ):
            lines.extend(
                (
                    "Изменился официальный срок события:",
                    f"Было: {_optional_utc_text(previous.event_starts_at)}",
                    f"Стало: {_optional_utc_text(item.event_starts_at)}",
                )
            )
        else:
            lines.append(
                "Официальное объявление получило существенное обновление."
            )
        lines.extend(_automatic_news_context(item))
        return "\n".join(lines)
    if decision.kind is NewsNotificationDecisionKind.CANCELLED:
        lines = [
            f"✅ СОБЫТИЕ ОТМЕНЕНО — {category}",
            "",
            subject,
            item.description,
            "",
            "🛡 Что делать:",
            item.recommended_action,
            "",
            f"🔗 Источник: {item.source}",
        ]
        if item.url:
            lines.append(item.url)
        return "\n".join(lines)
    raise ValueError("Decision is not an automatic news event.")


def _automatic_news_context(item: NewsItem) -> tuple[str, ...]:
    lines = [
        "",
        "⚠️ Почему важно:",
        item.reason,
        "",
        "🛡 Что делать:",
        item.recommended_action,
        "",
        f"🔗 Источник: {item.source}",
    ]
    if item.url:
        lines.append(item.url)
    return tuple(lines)


def _utc_text(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M UTC")


def _optional_utc_text(value: datetime | None) -> str:
    return "не указано" if value is None else _utc_text(value)


def pack_auto_news_sections(
    sections: tuple[str, ...],
    limit: int = SAFE_MESSAGE_LIMIT,
) -> tuple[str, ...]:
    """Pack complete news sections without splitting an individual event."""
    if not sections:
        return ()
    if limit <= 0 or limit > TELEGRAM_MESSAGE_LIMIT:
        raise ValueError("Telegram message limit is invalid.")
    messages: list[str] = []
    current = AUTO_NEWS_HEADER
    for section in sections:
        candidate = f"{current}\n\n{section}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current != AUTO_NEWS_HEADER:
            messages.append(current)
            current = f"{AUTO_NEWS_HEADER}\n\n{section}"
        else:
            current = candidate
        if len(current) > limit:
            raise ValueError("One automatic news event exceeds Telegram limit.")
    messages.append(current)
    return tuple(messages)


def split_messages(text: str, limit: int = SAFE_MESSAGE_LIMIT) -> tuple[str, ...]:
    if limit <= 0 or limit > TELEGRAM_MESSAGE_LIMIT:
        raise ValueError("Telegram message limit is invalid.")
    if not text:
        return ("",)
    return tuple(text[index : index + limit] for index in range(0, len(text), limit))


def _signal_section(index: int, signal: object) -> str:
    from market_signal_assistant.application.presentation import SignalView

    if not isinstance(signal, SignalView):
        raise TypeError("Expected SignalView.")
    reasons = "; ".join(signal.explanations[:3]) or "причины не определены"
    lines = [
        f"{index}. {signal.symbol} — {signal.direction}",
        f"Итоговый балл: {format_number(signal.combined_score)}",
        f"Техническая сила сигнала: {format_number(signal.technical_score)}",
        f"Уверенность: {format_number(signal.confidence)}%",
    ]
    if signal.derivatives_score is None:
        state = (
            "недоступны"
            if signal.derivatives_context.startswith("Данные деривативов недоступны")
            else "не включены"
        )
        lines.append(f"Деривативы: {state}")
    else:
        lines.append(
            f"Деривативы: {format_number(signal.derivatives_score)}; "
            f"режим: {signal.regime}"
        )
    lines.extend(
        (
            signal.derivatives_context,
            f"Подтверждения: {signal.confirmations}",
        )
    )
    if signal.conflicts:
        lines.append(f"Противоречия: {signal.conflicts}")
    lines.append(f"Причины: {reasons}")
    lines.append(
        "Риски и предупреждения: "
        + ("; ".join(signal.warnings) if signal.warnings else "риски не выявлены")
    )
    return "\n".join(lines)


def _alarm_signals(view: ReportView) -> tuple[SignalView, ...]:
    failed_technical_symbols = {
        item.symbol
        for item in view.failed_instruments
        if item.stage == "технический анализ"
    }
    return tuple(
        signal
        for signal in view.ranked_signals
        if signal.symbol not in failed_technical_symbols
        and _is_alarm_signal(signal)
    )


def _is_alarm_signal(signal: SignalView) -> bool:
    return (
        abs(signal.combined_score) >= ALARM_MINIMUM_FINAL_SCORE
        and signal.technical_score >= ALARM_MINIMUM_TECHNICAL_SCORE
        and signal.confidence >= ALARM_MINIMUM_CONFIDENCE
        and signal.confirmations >= ALARM_MINIMUM_CONFIRMATIONS
        and signal.direction in {"ЛОНГ", "ШОРТ"}
    )


def _alarm_signal_section(signal: SignalView) -> str:
    reasons = _unique(signal.explanations)[:3]
    reason_lines = tuple(f"• {reason}" for reason in reasons)
    if not reason_lines:
        reason_lines = ("• причины не определены",)
    warnings = _unique(signal.warnings)
    risk = warnings[0] if warnings else "Существенные предупреждения не выявлены."
    return "\n".join(
        (
            f"{signal.symbol} — {signal.direction}",
            "",
            f"Итоговый балл: {format_number(abs(signal.combined_score))}",
            f"Уверенность: {format_number(signal.confidence)}%",
            f"Подтверждения: {signal.confirmations}",
            "",
            "Главное:",
            *reason_lines,
            "",
            "Риск:",
            risk,
            "",
            ALARM_DISCLAIMER,
        )
    )


def _unique(items: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item for item in items if item))


def _inplay_section(index: int, result: InPlayResult) -> str:
    metrics = _inplay_metrics(result)
    return "\n".join(
        (
            f"{index}. {_inplay_heading(result, metrics)}",
            "",
            *_inplay_indicator_lines(result, metrics),
            "",
            *_inplay_conclusion(result, metrics),
        )
    )


def _auto_inplay_section(result: InPlayResult) -> str:
    semantics = automatic_semantics(result)
    header = (
        "🚨 СИЛЬНЫЙ IN PLAY"
        if semantics.directional_entry_allowed
        else "🔥 IN PLAY — ТРЕБУЕТ ВНИМАНИЯ"
    )
    metrics = _inplay_metrics(result)
    return "\n".join(
        (
            header,
            "",
            _auto_inplay_heading(semantics),
            "",
            *_inplay_indicator_lines(result, metrics),
            *_auto_confirmation_lines(semantics),
            "",
            *_auto_inplay_conclusion(semantics, metrics),
        )
    )


def _auto_inplay_heading(semantics: AutomaticInPlaySemantics) -> str:
    listing = "🆕 " if semantics.result.is_new_listing else ""
    state = {
        AutomaticDisplayStatus.LONG: "🟢",
        AutomaticDisplayStatus.SHORT: "🔴",
        AutomaticDisplayStatus.WATCH: "🟡",
        AutomaticDisplayStatus.LATE_ENTRY: "🟡",
        AutomaticDisplayStatus.DO_NOT_CHASE: "🔴",
    }[semantics.display_status]
    return (
        f"{listing}{state} {semantics.result.symbol} — "
        f"{semantics.display_status.value}"
    )


def _auto_confirmation_lines(
    semantics: AutomaticInPlaySemantics,
) -> tuple[str, ...]:
    return tuple(
        f"✅ Подтверждение: {confirmation}."
        for confirmation in semantics.visible_confirmations
    )


def _auto_inplay_conclusion(
    semantics: AutomaticInPlaySemantics,
    metrics: _InPlayMetrics,
) -> tuple[str, ...]:
    if semantics.display_status is AutomaticDisplayStatus.DO_NOT_CHASE:
        return (
            "🚨 Вывод:",
            "Резкое движение уже состоялось.",
            "Высокий риск позднего входа и сильного отката.",
            "",
            "❌ Не догонять цену.",
        )
    if semantics.display_status is AutomaticDisplayStatus.LATE_ENTRY:
        return (
            "⚠️ Вывод:",
            "Значительная часть движения уже реализована.",
            "",
            "❌ Сейчас не входить.",
        )
    if (
        semantics.display_status is AutomaticDisplayStatus.WATCH
        and "Пробой локального диапазона" in semantics.visible_confirmations
    ):
        return (
            "💡 Вывод:",
            "Пробой диапазона зафиксирован, но направление входа ещё не подтверждено.",
            "Ждём закрепление, ретест или продолжение движения с объёмом.",
        )
    return _inplay_conclusion(semantics.result, metrics)


def _clean_reason(reason: str) -> str:
    return reason.strip().rstrip(" .;")


def _inplay_metrics(result: InPlayResult) -> _InPlayMetrics:
    return _InPlayMetrics(
        price_change=_reason_number(result.reasons, "Изменение цены"),
        volatility=_reason_number(result.reasons, "Волатильность ATR"),
        relative_volume=_reason_number(result.reasons, "Относительный объём"),
    )


def _reason_number(reasons: tuple[str, ...], prefix: str) -> float | None:
    for reason in _unique(reasons):
        cleaned = _clean_reason(reason)
        if not cleaned.startswith(prefix):
            continue
        match = _NUMBER_IN_REASON.search(cleaned[len(prefix) :])
        if match is None:
            return None
        value = match.group().replace(",", ".").replace("−", "-")
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _inplay_heading(result: InPlayResult, metrics: _InPlayMetrics) -> str:
    listing = "🆕 " if result.is_new_listing else ""
    if result.direction is InPlayDirection.LONG:
        state = "🟢"
        label = "ЛОНГ"
    elif result.direction is InPlayDirection.SHORT:
        state = "🔴"
        label = "ШОРТ"
    else:
        absolute_change = abs(metrics.price_change or 0.0)
        if absolute_change >= 30.0:
            state = "🔴"
            label = "НЕ ДОГОНЯТЬ"
        elif absolute_change >= 15.0:
            state = "🟡"
            label = "ПОЗДНИЙ ВХОД"
        else:
            state = "🟡"
            label = "СЛЕДИМ"
    return f"{listing}{state} {result.symbol} — {label}"


def _inplay_indicator_lines(
    result: InPlayResult,
    metrics: _InPlayMetrics,
) -> tuple[str, ...]:
    lines = [
        f"📊 Активность: {_rounded_score(result.inplay_score)}/100 — "
        f"{_activity_label(result.inplay_score)}"
    ]
    if metrics.price_change is not None:
        lines.append(_price_line(metrics.price_change))
    if metrics.volatility is not None:
        lines.append(_volatility_line(metrics.volatility))
    if metrics.relative_volume is not None:
        lines.append(_volume_line(metrics.relative_volume))
    return tuple(lines[:4])


def _rounded_score(score: float) -> int:
    return int(score + 0.5)


def _activity_label(score: float) -> str:
    if score < 60.0:
        return "умеренная"
    if score < 70.0:
        return "заметная"
    if score < 80.0:
        return "сильная"
    return "экстремальная"


def _volume_line(value: float) -> str:
    if value < 1.0:
        emoji, label = "🔈", "ниже обычного"
    elif value < 1.3:
        emoji, label = "🔉", "почти обычный"
    elif value < 1.8:
        emoji, label = "📊", "повышенный"
    elif value < 2.5:
        emoji, label = "🔥", "высокий"
    else:
        emoji, label = "🔥", "очень высокий"
    return f"{emoji} Объём: {label} — {_decimal(value)}×"


def _volatility_line(value: float) -> str:
    if value < 1.0:
        emoji, label = "😴", "низкая"
    elif value < 3.0:
        emoji, label = "〰️", "умеренная"
    elif value < 6.0:
        emoji, label = "🌊", "высокая"
    else:
        emoji, label = "🌪", "очень высокая"
    return f"{emoji} Волатильность: {label} — {_decimal(value)}%"


def _price_line(value: float) -> str:
    absolute = abs(value)
    direction = "выросла" if value >= 0 else "упала"
    if absolute >= 30.0:
        emoji = "🚀" if value >= 0 else "💥"
        label = f"Цена уже {direction}"
    elif absolute >= 15.0:
        emoji = "📈" if value >= 0 else "📉"
        label = f"Цена уже {direction}"
    else:
        emoji = "📈" if value >= 0 else "📉"
        label = "Цена"
    return f"{emoji} {label}: {_signed_decimal(value)}%"


def _inplay_conclusion(
    result: InPlayResult,
    metrics: _InPlayMetrics,
) -> tuple[str, ...]:
    if result.direction is InPlayDirection.LONG:
        return (
            "✅ Вывод:",
            "Направление вверх подтверждено текущими условиями IN PLAY.",
            "",
            "⚠️ Перед входом проверь уровень, стоп и соотношение риска к прибыли.",
        )
    if result.direction is InPlayDirection.SHORT:
        return (
            "✅ Вывод:",
            "Направление вниз подтверждено текущими условиями IN PLAY.",
            "",
            "⚠️ Перед входом проверь уровень, стоп и соотношение риска к прибыли.",
        )

    price_change = metrics.price_change or 0.0
    if price_change >= 15.0:
        return (
            "🚨 Вывод:",
            "Основное движение вверх уже произошло.",
            "Высокий риск купить перед сильным откатом.",
            "",
            "❌ Не догонять цену.",
        )
    if price_change <= -15.0:
        return (
            "⚠️ Вывод:",
            "Падение уже состоялось. Для шорта вход может быть поздним,",
            "а разворот в лонг ещё не подтверждён.",
            "",
            "❌ Сейчас не входить.",
        )

    if metrics.relative_volume is not None and metrics.relative_volume >= 1.8:
        summary = "Интерес к монете вырос."
        action = "👀 Ждём пробой или сильный импульс."
    else:
        summary = "Рыночная активность требует наблюдения."
        action = "👀 Ждём дополнительное подтверждение."
    warnings = _unique(result.warnings)
    if warnings:
        risk = f"⚠️ {_clean_reason(warnings[0])}"
        return ("💡 Вывод:", summary, action, "", risk)
    return (
        "💡 Вывод:",
        summary,
        action,
        "",
        "⚠️ Риск позднего входа: не выявлен.",
        "Направление пока не подтверждено.",
    )


def _decimal(value: float) -> str:
    return f"{value:.1f}".replace(".", ",")


def _signed_decimal(value: float) -> str:
    if value < 0:
        return f"−{_decimal(abs(value))}"
    return f"+{_decimal(value)}"


def _news_section(item: NewsItem, generated_at: datetime) -> str:
    heading = _news_importance_heading(item.importance)
    category = _news_category_label_for_item(item)
    subject = ", ".join(item.symbols) if item.symbols else item.title
    lines = [
        f"{heading} — {category}",
        "",
        subject,
        item.description,
        "",
        f"🕒 Опубликовано: {_relative_time(item.published_at, generated_at)}",
    ]
    if item.event_starts_at is not None:
        lines.append(
            "⏰ Начало события: "
            f"{item.event_starts_at.strftime('%Y-%m-%d %H:%M')} UTC"
        )
    elif item.category is NewsCategory.NETWORK and item.event_start_date is not None:
        lines.append(
            "⏰ Ограничение начинается: "
            f"{item.event_start_date.isoformat()}, точное время не указано."
        )
    lines.extend(
        (
            "⚠️ Почему важно:",
            item.reason,
            "",
            "🛡 Что делать:",
            item.recommended_action,
            "",
            f"🔗 Источник: {item.source}",
        )
    )
    if item.url:
        lines.append(item.url)
    return "\n".join(lines)


def _news_importance_heading(importance: NewsImportance) -> str:
    return {
        NewsImportance.CRITICAL: "🔴 КРИТИЧНО",
        NewsImportance.HIGH: "🟠 ВАЖНО",
        NewsImportance.MEDIUM: "🟡 СЛЕДИТЬ",
        NewsImportance.LOW: "⚪ СПРАВОЧНО",
    }[importance]


def _news_category_label(category: NewsCategory) -> str:
    return {
        NewsCategory.LISTING: "НОВЫЙ ЛИСТИНГ",
        NewsCategory.DELISTING: "ДЕЛИСТИНГ",
        NewsCategory.MAINTENANCE: "ТЕХНИЧЕСКИЕ РАБОТЫ",
        NewsCategory.SECURITY: "БЕЗОПАСНОСТЬ",
        NewsCategory.NETWORK: "СЕТЬ И ПЕРЕВОДЫ",
        NewsCategory.TRADING_CHANGE: "ИЗМЕНЕНИЕ УСЛОВИЙ ТОРГОВЛИ",
        NewsCategory.REGULATION: "РЕГУЛИРОВАНИЕ",
        NewsCategory.OTHER: "ОБНОВЛЕНИЕ",
    }[category]


def _news_category_label_for_item(item: NewsItem) -> str:
    if (
        item.category is NewsCategory.LISTING
        and item.asset_type is NewsAssetType.STOCK
    ):
        return "АКЦИОННЫЙ ПЕРПЕТУАЛ"
    if item.category is NewsCategory.LISTING and re.search(
        r"\b(?:perpetual\s+)?pre[-\s]market(?:\s+trading)?\b",
        item.title,
        flags=re.IGNORECASE,
    ):
        return "ПРЕДРЫНОЧНЫЙ ЛИСТИНГ"
    return _news_category_label(item.category)


def _relative_time(published_at: datetime, generated_at: datetime) -> str:
    elapsed = max(timedelta(), generated_at - published_at)
    total_minutes = int(elapsed.total_seconds() // 60)
    if total_minutes < 60:
        minutes = max(1, total_minutes)
        return f"{minutes} {_plural(minutes, 'минуту', 'минуты', 'минут')} назад"
    total_hours = int(elapsed.total_seconds() // 3600)
    if total_hours < 24:
        return f"{total_hours} {_plural(total_hours, 'час', 'часа', 'часов')} назад"
    if total_hours < 48:
        return "вчера"
    days = total_hours // 24
    return f"{days} {_plural(days, 'день', 'дня', 'дней')} назад"


def _hours_text(hours: int) -> str:
    return f"{hours} {_plural(hours, 'час', 'часа', 'часов')}"


def _plural(value: int, one: str, few: str, many: str) -> str:
    if value % 10 == 1 and value % 100 != 11:
        return one
    if value % 10 in {2, 3, 4} and value % 100 not in {12, 13, 14}:
        return few
    return many
