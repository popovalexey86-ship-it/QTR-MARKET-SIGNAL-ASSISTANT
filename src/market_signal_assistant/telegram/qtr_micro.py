from __future__ import annotations

from market_signal_assistant.qtr_micro.models import (
    EntryPlan,
    MicroDirection,
    MicroExitReason,
    MicroPosition,
)


def format_micro_entry(plan: EntryPlan) -> str:
    return (
        "🚀 QTR MICRO — DEMO ВХОД\n\n"
        f"{plan.symbol} — {_direction(plan.direction)}\n"
        f"Конструкция: {plan.setup_type.name_ru}\n\n"
        f"💰 Вход: {_number(plan.entry_price)}\n"
        f"🛡 Стоп: {_number(plan.stop_price)}\n"
        f"📏 Риск: {_number(plan.risk_pct)}%\n"
        f"⚙️ Плечо: x{plan.leverage}\n"
        f"🎯 TP1: {_number(plan.tp1_price)} (+1R / 40%)\n"
        f"🎯 TP2: {_number(plan.tp2_price)} (+2R / 30%)\n"
        "🏃 Остаток: 30%\n\n"
        "Режим: BYBIT DEMO"
    )


def format_micro_tp(symbol: str, reason: MicroExitReason, result_r: float) -> str:
    label = "TP1" if reason is MicroExitReason.TP1 else "TP2"
    closed = "40%" if reason is MicroExitReason.TP1 else "30%"
    return (
        f"✅ QTR MICRO — {label}\n\n"
        f"{symbol}\n"
        f"Закрыто: {closed}\n"
        f"Результат: +{_number(result_r)}R\n\n"
        "BYBIT DEMO"
    )


def format_micro_closed(
    position: MicroPosition,
    *,
    reason: str,
    pnl: float,
    result_r: float,
    hold_minutes: int,
) -> str:
    return (
        "🏁 QTR MICRO — СДЕЛКА ЗАКРЫТА\n\n"
        f"{position.symbol} — {_direction(position.direction)}\n\n"
        f"Причина: {reason}\n"
        f"Результат: {_signed(pnl)} USDT\n"
        f"Результат: {_signed(result_r)}R\n"
        f"Время позиции: {hold_minutes} мин\n\n"
        "DEMO"
    )


def _direction(value: MicroDirection) -> str:
    return "ЛОНГ" if value is MicroDirection.LONG else "ШОРТ"


def _number(value: float) -> str:
    return f"{value:.8f}".rstrip("0").rstrip(".").replace(".", ",")


def _signed(value: float) -> str:
    prefix = "+" if value > 0 else ""
    return f"{prefix}{_number(value)}"
