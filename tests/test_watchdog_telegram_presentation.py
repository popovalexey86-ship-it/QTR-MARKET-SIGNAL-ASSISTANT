from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from watchdog_evidence_helpers import NOW, evidence

from market_signal_assistant.watchdog.models import (
    AnomalyType,
    FeatureObservation,
    WatchdogState,
)
from market_signal_assistant.watchdog.telegram_alert_policy import decide_alert
from market_signal_assistant.watchdog.telegram_presentation import (
    alert_keyboard,
    compact_alert,
    detail_text,
)


def _feature(name: str, value: float) -> FeatureObservation:
    return FeatureObservation(
        name,
        value,
        NOW - timedelta(minutes=5),
        NOW,
        unit="ratio",
    )


def test_compact_russian_alert_uses_factual_units_moscow_and_buttons() -> None:
    event = replace(
        evidence(anomaly_types=(AnomalyType.PRICE_ACCELERATION,)),
        features=(
            _feature("price_change_1", 0.023),
            _feature("relative_volume_20", 2.5),
            _feature("open_interest_change", 0.05),
            _feature("funding_rate", 0.0001),
        ),
    )
    text = compact_alert(event, reason="NEW_STRONG", now=NOW.timestamp() + 30)
    for fragment in (
        "⚡ Ценовой импульс",
        "+2.30% за 5 мин",
        "2.50× к среднему 20 свечей",
        "Открытый интерес: +5.00% (период не сохранён)",
        "Ставка финансирования: +0.0100%",
        "11:00 МСК",
        "возраст данных: 30 с",
        "Что происходит:",
        "На что обратить внимание:",
        "не является прогнозом доходности",
    ):
        assert fragment in text
    assert "ликвидац" not in text.lower()
    assert "entry" not in text.lower()
    keyboard = alert_keyboard(event)
    assert keyboard[0][0]["url"].endswith("ABCUSDT.P/?exchange=BYBIT")
    assert keyboard[1][0]["callback_data"].startswith("detail:")
    assert keyboard[1][1]["callback_data"] == "cmd:inplay"
    assert keyboard[2][0]["callback_data"] == "cmd:start"
    assert event.event_id in detail_text(event)


def test_price_down_oi_down_is_marked_as_interpretation() -> None:
    event = replace(
        evidence(anomaly_types=(AnomalyType.OI_SHOCK,)),
        features=(
            _feature("price_change_1", -0.03),
            _feature("open_interest_change", -0.02),
        ),
    )
    text = compact_alert(event, reason="NEW_STRONG", now=NOW.timestamp())
    assert "Цена и открытый интерес снизились" in text
    assert "Возможная трактовка: сокращение позиций" in text
    assert "направление будущего" not in text


def test_low_score_real_example_contracts_do_not_send_individual_alerts() -> None:
    for symbol, score in (
        ("ENAUSDT", 15.3),
        ("STRKUSDT", 15.6),
        ("XAUTUSDT", 15.6),
        ("IRYSUSDT", 5.8),
    ):
        at = NOW
        event = replace(
            evidence(detected_at=at, score=score),
            event_id=f"watchdog:{symbol}:{at:%Y%m%dT%H%M%S}.000000Z",
            symbol=symbol,
            state_before=WatchdogState.NORMAL,
            state_after=WatchdogState.NORMAL,
        )
        decision = decide_alert(
            event, now=at.timestamp(), previous=None, sent_last_day=0
        )
        assert not decision.send
        assert decision.reason == "SECONDARY"
