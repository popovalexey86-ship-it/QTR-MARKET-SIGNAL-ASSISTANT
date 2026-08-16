from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from market_signal_assistant.qtr_micro_scalper.analytics import (
    AnalyticsReason,
    AnalyticsSlice,
    AnalyticsSnapshot,
)
from market_signal_assistant.qtr_micro_scalper.metrics import MetricsSnapshot

_MARKET_STATES = {
    "BUY_PRESSURE": "ДАВЛЕНИЕ ПОКУПАТЕЛЕЙ 🟢",
    "SELL_PRESSURE": "ДАВЛЕНИЕ ПРОДАВЦОВ 🔴",
    "UPWARD_SWEEP": "ВОСХОДЯЩИЙ СБОР ЛИКВИДНОСТИ 🟢",
    "DOWNWARD_SWEEP": "НИСХОДЯЩИЙ СБОР ЛИКВИДНОСТИ 🔴",
    "BUY_FLOW_ABSORBED": "ПОКУПКИ ПОГЛОЩАЮТСЯ 🟠",
    "SELL_FLOW_ABSORBED": "ПРОДАЖИ ПОГЛОЩАЮТСЯ 🟠",
    "TWO_SIDED_LIQUIDITY": "ДВУСТОРОННЯЯ ЛИКВИДНОСТЬ 🟡",
    "BALANCED": "БАЛАНС 🟡",
    "NOT_READY": "ДАННЫЕ НЕ ГОТОВЫ ⚪",
    "UNKNOWN": "НЕ ОПРЕДЕЛЕНО ⚪",
}
_DIRECTIONS = {
    "LONG": "ТЕНЕВОЙ ЛОНГ 🟢",
    "SHORT": "ТЕНЕВОЙ ШОРТ 🔴",
    "NONE": "НАБЛЮДЕНИЕ ⚪",
    "UNKNOWN": "НАБЛЮДЕНИЕ ⚪",
}
_REASONS = {
    "spread": "широкий спред",
    "liquidity conflict": "конфликт ликвидности",
    "stale data": "устаревшие данные",
    "low score": "недостаточная оценка",
    "risk": "повышенный риск",
    "stop": "срабатывание стопа",
    "expired": "истечение ожидания входа",
    "failed setup": "несостоявшийся сетап",
    "other": "другая причина",
}


@dataclass(frozen=True, slots=True)
class ShadowObserverSummary:
    generated_at: datetime
    text: str

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("Observer timestamp must be timezone-aware.")
        if not self.text.strip():
            raise ValueError("Observer summary cannot be empty.")
        object.__setattr__(self, "generated_at", self.generated_at.astimezone(UTC))


class ShadowObserver:
    """Pure formatter for existing offline shadow metrics and analytics."""

    def summarize(
        self,
        metrics: MetricsSnapshot,
        analytics: AnalyticsSnapshot,
    ) -> ShadowObserverSummary:
        generated_at = max(metrics.generated_at, analytics.generated_at)
        return ShadowObserverSummary(
            generated_at=generated_at,
            text=format_shadow_summary(metrics, analytics),
        )


def format_shadow_summary(
    metrics: MetricsSnapshot,
    analytics: AnalyticsSnapshot,
) -> str:
    """Return a deterministic Russian summary without sending it anywhere."""

    symbol = _dominant_symbol(analytics)
    market_state = _dominant_key(analytics.by_market_state)
    direction = _dominant_direction(analytics.by_direction)
    score_bucket = _dominant_score_bucket(analytics.by_score_bucket)
    score_slice = _slice(analytics.by_score_bucket, score_bucket)
    overall = analytics.overall
    performance = metrics.overall
    observed_at = max(metrics.generated_at, analytics.generated_at)

    lines = [
        "🧠 QTR MICRO SCALPER V2 — ТЕНЕВОЙ НАБЛЮДАТЕЛЬ",
        "",
        "📡 Рынок:",
        symbol,
        "",
        "💎 Состояние:",
        _MARKET_STATES.get(market_state, market_state.replace("_", " ")),
        "",
        "🔥 Ликвидность:",
        "Детализация сбора ликвидности отсутствует в агрегированных журналах.",
        "",
        "⚡ Поток:",
        "Delta недоступна в агрегированных журнальных снимках.",
        "",
        "🎯 Оценка:",
        _score_text(score_bucket, score_slice),
        "",
        "⚔️ Решение:",
        _DIRECTIONS.get(direction, "НАБЛЮДЕНИЕ ⚪"),
        "",
        "📊 Решения:",
        f"Всего: {overall.total_decisions}",
        f"Заблокировано: {overall.blocked_decisions}",
        f"Теневых входов: {overall.shadow_entries}",
        "",
        "📈 Виртуальные сделки:",
        f"Активных: {overall.active_trades}",
        f"Завершено: {overall.completed_trades}",
        f"Доля побед: {_percent(overall.win_rate)}",
        f"Средний результат: {_number(overall.average_r)} R",
        f"Суммарный результат: {_number(overall.total_r)} R",
        f"Достижение TP1: {_percent(performance.tp1_hit_rate)}",
        f"Достижение TP2: {_percent(performance.tp2_hit_rate)}",
        f"Срабатывание стопа: {_percent(performance.stop_rate)}",
        f"Истечение ожидания: {_percent(performance.expired_rate)}",
        "",
        "🚫 Основные причины блокировки:",
        _reason_text(analytics.top_blocked_reasons),
        "",
        "📉 Основные причины неудач:",
        _reason_text(analytics.top_loss_reasons),
        "",
        f"🕒 Сформировано: {observed_at:%Y-%m-%d %H:%M} UTC",
        "",
        "Только теневое наблюдение. Реальные ордера не используются.",
    ]
    return "\n".join(lines)


def _dominant_symbol(snapshot: AnalyticsSnapshot) -> str:
    selected = _dominant(snapshot.by_symbol)
    return "НЕТ ДАННЫХ" if selected is None else selected.key


def _dominant_direction(values: tuple[AnalyticsSlice, ...]) -> str:
    if not values:
        return "UNKNOWN"
    selected = sorted(
        values,
        key=lambda item: (
            -item.metrics.shadow_entries,
            -item.metrics.completed_trades,
            -item.metrics.total_decisions,
            item.key,
        ),
    )[0]
    return selected.key if selected.metrics.shadow_entries else "UNKNOWN"


def _dominant_key(values: tuple[AnalyticsSlice, ...]) -> str:
    selected = _dominant(values)
    return "UNKNOWN" if selected is None else selected.key


def _dominant_score_bucket(values: tuple[AnalyticsSlice, ...]) -> str:
    if not values:
        return "UNKNOWN"
    bucket_rank = {"0-49": 0, "50-64": 1, "65-79": 2, "80-100": 3}
    return sorted(
        values,
        key=lambda item: (
            -item.metrics.total_decisions,
            -bucket_rank.get(item.key, -1),
            item.key,
        ),
    )[0].key


def _dominant(values: tuple[AnalyticsSlice, ...]) -> AnalyticsSlice | None:
    if not values:
        return None
    return sorted(
        values,
        key=lambda item: (
            -item.metrics.total_decisions,
            -item.metrics.shadow_entries,
            -item.metrics.completed_trades,
            item.key,
        ),
    )[0]


def _slice(
    values: tuple[AnalyticsSlice, ...],
    key: str,
) -> AnalyticsSlice | None:
    return next((item for item in values if item.key == key), None)


def _score_text(bucket: str, value: AnalyticsSlice | None) -> str:
    if value is None or bucket == "UNKNOWN":
        return "НЕТ ДАННЫХ"
    return f"Основной диапазон: {bucket} ({value.metrics.total_decisions} решений)"


def _reason_text(values: tuple[AnalyticsReason, ...]) -> str:
    if not values:
        return "Нет данных."
    return "; ".join(
        f"{_REASONS.get(item.reason, item.reason)} — {item.count}"
        for item in values
    )


def _number(value: float) -> str:
    return f"{value:.2f}".replace(".", ",")


def _percent(value: float) -> str:
    return f"{_number(value)}%"
