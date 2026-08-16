from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest
from test_analytics import decision
from test_metrics import record as trade_record
from test_metrics import runtime_event

from market_signal_assistant.qtr_micro_scalper.analytics import (
    analyze_shadow_journals,
)
from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    ShadowDecisionEventType,
)
from market_signal_assistant.qtr_micro_scalper.metrics import (
    aggregate_shadow_metrics,
)
from market_signal_assistant.qtr_micro_scalper.observer import (
    ShadowObserver,
    format_shadow_summary,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEventType,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def test_human_readable_summary_uses_russian_labels_and_emojis() -> None:
    decisions = (
        decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED),
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            symbol="ETHUSDT",
            score=55.0,
            market_state="SELL_PRESSURE",
            reasons=("Spread is too wide.",),
            seconds=1,
        ),
    )
    trade = trade_record("btc")
    metrics = aggregate_shadow_metrics((trade,), generated_at=NOW)
    analytics = analyze_shadow_journals(decisions, (trade,), generated_at=NOW)
    text = format_shadow_summary(metrics, analytics)
    assert text.startswith("🧠 QTR MICRO SCALPER V2 — ТЕНЕВОЙ НАБЛЮДАТЕЛЬ")
    assert "📡 Рынок:\nBTCUSDT" in text
    assert "💎 Состояние:\nДАВЛЕНИЕ ПОКУПАТЕЛЕЙ 🟢" in text
    assert "🎯 Оценка:\nОсновной диапазон: 80-100" in text
    assert "⚔️ Решение:\nТЕНЕВОЙ ЛОНГ 🟢" in text
    assert "Заблокировано: 1" in text
    assert "широкий спред — 1" in text


def test_metrics_snapshot_contributes_trade_performance() -> None:
    tp1 = runtime_event("win", "BTCUSDT", ShadowRuntimeEventType.TP1_REACHED)
    tp2 = runtime_event("win", "BTCUSDT", ShadowRuntimeEventType.TP2_REACHED)
    trade = trade_record("win", events=(tp1, tp2))
    metrics = aggregate_shadow_metrics((trade,), generated_at=NOW)
    analytics = analyze_shadow_journals(
        (decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED),),
        (trade,),
        generated_at=NOW,
    )
    text = format_shadow_summary(metrics, analytics)
    assert "Достижение TP1: 100,00%" in text
    assert "Достижение TP2: 100,00%" in text
    assert "Средний результат: 2,00 R" in text


def test_short_direction_and_loss_reason_are_translated() -> None:
    stopped = runtime_event(
        "loss",
        "ETHUSDT",
        ShadowRuntimeEventType.STOPPED,
    )
    trade = trade_record(
        "loss",
        symbol="ETHUSDT",
        direction=ShadowDirection.SHORT,
        score=70.0,
        outcome=ShadowOutcomeStatus.LOSS,
        result_r=-1.0,
        events=(stopped,),
    )
    decision_record = decision(
        ShadowDecisionEventType.SHADOW_ENTRY_CREATED,
        symbol="ETHUSDT",
        score=70.0,
        market_state="SELL_PRESSURE",
    )
    metrics = aggregate_shadow_metrics((trade,), generated_at=NOW)
    analytics = analyze_shadow_journals(
        (decision_record,),
        (trade,),
        generated_at=NOW,
    )
    text = format_shadow_summary(metrics, analytics)
    assert "ДАВЛЕНИЕ ПРОДАВЦОВ 🔴" in text
    assert "ТЕНЕВОЙ ШОРТ 🔴" in text
    assert "срабатывание стопа — 1" in text


def test_missing_microstructure_values_are_not_invented() -> None:
    metrics = aggregate_shadow_metrics((), generated_at=NOW)
    analytics = analyze_shadow_journals((), (), generated_at=NOW)
    text = format_shadow_summary(metrics, analytics)
    assert "📡 Рынок:\nНЕТ ДАННЫХ" in text
    assert "Delta недоступна" in text
    assert "Детализация сбора ликвидности отсутствует" in text
    assert "🎯 Оценка:\nНЕТ ДАННЫХ" in text
    assert "⚔️ Решение:\nНАБЛЮДЕНИЕ ⚪" in text


def test_observer_is_deterministic_and_returns_immutable_summary() -> None:
    trade = trade_record("one")
    metrics = aggregate_shadow_metrics((trade,), generated_at=NOW)
    analytics = analyze_shadow_journals(
        (decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED),),
        (trade,),
        generated_at=NOW,
    )
    observer = ShadowObserver()
    first = observer.summarize(metrics, analytics)
    second = observer.summarize(metrics, analytics)
    assert first == second
    assert first.generated_at.tzinfo is UTC
    with pytest.raises(FrozenInstanceError):
        first.text = "changed"  # type: ignore[misc]


def test_formatter_has_no_sending_side_effects() -> None:
    metrics = aggregate_shadow_metrics((), generated_at=NOW)
    analytics = analyze_shadow_journals((), (), generated_at=NOW)
    text = format_shadow_summary(metrics, analytics)
    assert "Только теневое наблюдение" in text
    assert "Реальные ордера не используются" in text
