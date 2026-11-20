"""Russian, factual presentation of immutable Watchdog event evidence."""

from __future__ import annotations

import hashlib
import math
from datetime import UTC, timedelta, timezone, tzinfo
from urllib.parse import quote
from zoneinfo import ZoneInfo

from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.models import AnomalyType, FeatureObservation

_CATEGORIES = (
    (AnomalyType.PRICE_ACCELERATION, "⚡ Ценовой импульс"),
    (AnomalyType.VOLUME_SHOCK, "📊 Аномальный объём"),
    (AnomalyType.VOLUME_ACCELERATION, "📊 Аномальный объём"),
    (AnomalyType.RANGE_BUILDUP, "👀 Формирование диапазона"),
    (AnomalyType.COMPRESSION, "🔄 Сжатие волатильности"),
    (AnomalyType.VOLATILITY_EXPANSION, "💥 Расширение волатильности"),
    (AnomalyType.OI_SHOCK, "🧲 Аномалия открытого интереса"),
    (AnomalyType.FUNDING_ANOMALY, "💰 Аномальная ставка финансирования"),
)
_REASONS = {
    "NEW_STRONG": "сильная новая аномалия",
    "NEW_EPISODE": "новый эпизод наблюдения",
    "MATERIAL_ESCALATION": "существенное усиление",
    "COOLDOWN_EXPIRED": "новое событие после паузы",
}


def display_timezone(name: str) -> tzinfo:
    if name == "Europe/Moscow":
        return timezone(timedelta(hours=3), name="МСК")
    return ZoneInfo(name)


def detail_token(event_id: str) -> str:
    return hashlib.sha256(event_id.encode("utf-8")).hexdigest()[:24]


def alert_keyboard(event: WatchdogEventEvidence) -> list[list[dict[str, str]]]:
    rows: list[list[dict[str, str]]] = []
    symbol = event.symbol
    if symbol.isascii() and symbol.isalnum() and symbol.endswith("USDT"):
        rows.append(
            [
                {
                    "text": "📊 Открыть график",
                    "url": (
                        "https://www.tradingview.com/symbols/"
                        f"{quote(symbol, safe='')}.P/?exchange=BYBIT"
                    ),
                }
            ]
        )
    rows.append(
        [
            {
                "text": "🔍 Подробнее",
                "callback_data": f"detail:{detail_token(event.event_id)}",
            },
            {"text": "🔥 IN-PLAY", "callback_data": "cmd:inplay"},
        ]
    )
    rows.append([{"text": "🏠 Главное меню", "callback_data": "cmd:start"}])
    return rows


def digest_keyboard() -> list[list[dict[str, str]]]:
    return [[{"text": "🔥 IN-PLAY", "callback_data": "cmd:inplay"}]]


def _feature(event: WatchdogEventEvidence, name: str) -> FeatureObservation | None:
    return next((item for item in event.features if item.name == name), None)


def _percent(value: float) -> str:
    return f"{value * 100:+.2f}%"


def _period(feature: FeatureObservation) -> str:
    minutes = (feature.available_at - feature.observed_at).total_seconds() / 60
    if minutes > 0 and minutes.is_integer():
        return f"{int(minutes)} мин"
    return "период не указан"


def compact_alert(
    event: WatchdogEventEvidence,
    *,
    reason: str,
    now: float,
    timezone_name: str = "Europe/Moscow",
) -> str:
    zone = display_timezone(timezone_name)
    category = (
        "🔥 Активная монета — IN-PLAY"
        if event.state_after.value == "IN_PLAY"
        else next(
            (label for kind, label in _CATEGORIES if kind in event.anomaly_types),
            "👀 Рыночная аномалия",
        )
    )
    price = _feature(event, "price_change_1")
    volume = _feature(event, "relative_volume_20")
    oi = _feature(event, "open_interest_change")
    funding = _feature(event, "funding_rate")
    detected = event.detected_at.astimezone(zone).strftime("%d.%m %H:%M %Z")
    age = max(0, int(now - event.available_at.timestamp()))
    lines = [
        f"🐺 {event.symbol} — {category}",
        f"Сила аномалии: {event.anomaly_score:.1f}/100",
        f"Почему сейчас: {_REASONS[reason]}",
    ]
    if price is not None and math.isfinite(price.value):
        movement = (
            "рост"
            if price.value > 0
            else "снижение"
            if price.value < 0
            else "без изменения"
        )
        lines.append(f"Цена: {movement}, {_percent(price.value)} за {_period(price)}")
    if volume is not None and math.isfinite(volume.value):
        lines.append(f"Объём: {volume.value:.2f}× к среднему 20 свечей")
    if oi is not None and math.isfinite(oi.value):
        lines.append(f"Открытый интерес: {_percent(oi.value)} (период не сохранён)")
    if funding is not None and math.isfinite(funding.value):
        lines.append(
            f"Ставка финансирования: {funding.value * 100:+.4f}% "
            "(период не сохранён)"
        )
    lines.append(f"Обнаружено: {detected} · возраст данных: {age} с")
    if price is not None and volume is not None and volume.value > 1:
        direction = (
            "Рост" if price.value > 0 else "Снижение" if price.value < 0 else "Движение"
        )
        fact = f"{direction.lower()} цены сопровождается повышенным объёмом."
    elif price is not None and oi is not None and price.value < 0 and oi.value < 0:
        fact = "Цена и открытый интерес снизились."
    elif price is not None and oi is not None and price.value > 0 and oi.value > 0:
        fact = "Цена и открытый интерес выросли."
    elif AnomalyType.COMPRESSION in event.anomaly_types:
        fact = "Диапазон сжат относительно собственной истории инструмента."
    else:
        fact = "Зафиксированы указанные выше рыночные признаки."
    lines.append(f"Что происходит: {fact}")
    if price is not None and oi is not None and price.value < 0 and oi.value < 0:
        watch = "Возможная трактовка: сокращение позиций; проверьте дальнейшие данные."
    elif AnomalyType.COMPRESSION in event.anomaly_types:
        watch = "Наблюдайте, появится ли выход из диапазона; направление неизвестно."
    else:
        watch = "Сверьте свежесть данных и развитие события в следующем обновлении."
    lines.append(f"На что обратить внимание: {watch}")
    lines.append("Индекс аномальности не является прогнозом доходности.")
    return "\n".join(lines)[:3800]


def detail_text(event: WatchdogEventEvidence) -> str:
    lines = [
        f"🔍 {event.symbol} — подробности события",
        f"Время UTC: {event.detected_at.astimezone(UTC).isoformat()}",
        f"Рыночные данные доступны с: {event.available_at.astimezone(UTC).isoformat()}",
        f"Состояние: {event.state_before.value} → {event.state_after.value}",
        f"Сила аномалии: {event.anomaly_score:.1f}/100",
        f"Типы: {', '.join(item.value for item in event.anomaly_types)}",
        f"Категория universe: {event.universe_tier}",
    ]
    for feature in event.features[:16]:
        unit = " %" if feature.unit == "percent" else f" {feature.unit}"
        lines.append(f"{feature.name}: {feature.value:.6g}{unit}")
    if event.missing_data or event.stale_data:
        lines.append("Часть данных отсутствовала или была устаревшей.")
    lines.append(f"ID события: {event.event_id}")
    return "\n".join(lines)[:3800]
