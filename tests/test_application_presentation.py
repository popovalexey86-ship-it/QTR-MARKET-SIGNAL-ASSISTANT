import json
from datetime import UTC, datetime

import pytest

from market_signal_assistant.application.models import (
    MarketSummary,
    ScreeningDirection,
    ScreeningReport,
    ScreeningSignalResult,
    ScreeningWarning,
    ScreeningWarningCode,
)
from market_signal_assistant.application.presentation import (
    _evidence_text,
    present_report,
)
from market_signal_assistant.models import (
    AssetClass,
    Instrument,
    MarketSignal,
    SignalDirection,
    SignalEvidence,
)

NOW = datetime(2026, 7, 31, tzinfo=UTC)


def test_report_mapping_has_shared_interface_semantics() -> None:
    instrument = Instrument("BTCUSDT", AssetClass.CRYPTO)
    technical = MarketSignal(
        instrument=instrument,
        interval="1h",
        timestamp=NOW,
        direction=SignalDirection.BEARISH,
        score=78,
        confidence=73,
        confirmations=3,
        conflicts=1,
        price=100,
        evidence=(
            SignalEvidence("trend", SignalDirection.BEARISH, 25, "trend down"),
            SignalEvidence("volume", SignalDirection.BULLISH, 10, "volume conflict"),
        ),
    )
    result = ScreeningSignalResult(
        instrument=instrument,
        direction=ScreeningDirection.SHORT,
        technical_signal=technical,
        warnings=(
            ScreeningWarning(
                ScreeningWarningCode.TECHNICAL_CONFLICT,
                "One conflicting factor.",
            ),
        ),
    )
    report = ScreeningReport(
        generated_at=NOW,
        successful_results=(result,),
        failed_instruments=(),
        ranked_signals=(result,),
        market_summary=MarketSummary(1, 1, 0, 0, 1, 0),
    )

    view = present_report(report)

    assert view.ranked_signals[0].direction == "ШОРТ"
    assert view.ranked_signals[0].technical_score == 78
    assert view.ranked_signals[0].combined_score == -78
    assert view.ranked_signals[0].confirmations == 3
    assert view.ranked_signals[0].conflicts == 1
    assert view.ranked_signals[0].explanations == (
        "Фактор «тренд» указывает на медвежий контекст.",
        "Фактор «объём» указывает на бычий контекст.",
    )
    assert view.ranked_signals[0].warnings == (
        "Обнаружены противоречащие технические факторы.",
    )
    assert view.ranked_signals[0].derivatives_context == "Деривативы не включены."
    assert view.disclaimer == "Информационный анализ, не торговая рекомендация."
    assert json.dumps(
        view.as_dict(), sort_keys=True, ensure_ascii=False
    ) == json.dumps(
        present_report(report).as_dict(), sort_keys=True, ensure_ascii=False
    )


def test_long_short_and_neutral_directions_are_localized() -> None:
    instrument = Instrument("BTCUSDT", AssetClass.CRYPTO)

    def item(direction: ScreeningDirection) -> ScreeningSignalResult:
        return ScreeningSignalResult(instrument, direction, None)

    results = tuple(
        item(direction)
        for direction in (
            ScreeningDirection.LONG,
            ScreeningDirection.SHORT,
            ScreeningDirection.NEUTRAL,
        )
    )
    report = ScreeningReport(
        NOW,
        results,
        (),
        results[:2],
        MarketSummary(3, 3, 0, 1, 1, 1),
    )
    view = present_report(report)
    assert tuple(item.direction for item in view.successful_results) == (
        "ЛОНГ",
        "ШОРТ",
        "НЕЙТРАЛЬНО",
    )
    assert view.successful_results[2].explanations == (
        "Подходящий технический сигнал не найден.",
    )


@pytest.mark.parametrize(
    ("evidence", "expected"),
    (
        (
            SignalEvidence(
                "trend",
                SignalDirection.BEARISH,
                25,
                "EMA20 101.5 is below EMA50 103.25",
            ),
            "EMA20 101,5 находится ниже EMA50 103,25.",
        ),
        (
            SignalEvidence(
                "momentum", SignalDirection.BEARISH, 15, "RSI14 is 24.07"
            ),
            "RSI14 составляет 24,07.",
        ),
        (
            SignalEvidence(
                "range_breakout",
                SignalDirection.BEARISH,
                35,
                "Close broke the prior 20-bar low 99.5",
            ),
            "Цена закрылась ниже минимума последних 20 свечей (99,5).",
        ),
    ),
)
def test_structured_technical_evidence_is_presented_in_russian(
    evidence: SignalEvidence, expected: str
) -> None:
    assert _evidence_text(evidence) == expected
