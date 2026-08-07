from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any

from market_signal_assistant.application.models import (
    InstrumentFailure,
    MarketSummary,
    ScreeningDirection,
    ScreeningReport,
    ScreeningSignalResult,
    ScreeningWarningCode,
)
from market_signal_assistant.derivatives.models import MarketPositioning
from market_signal_assistant.models import SignalDirection, SignalEvidence
from market_signal_assistant.signals.fusion import FusionEffect

DISCLAIMER = "Информационный анализ, не торговая рекомендация."

DIRECTION_LABELS = {
    ScreeningDirection.LONG: "ЛОНГ",
    ScreeningDirection.SHORT: "ШОРТ",
    ScreeningDirection.NEUTRAL: "НЕЙТРАЛЬНО",
}
SIGNAL_DIRECTION_LABELS = {
    SignalDirection.BULLISH: "бычий",
    SignalDirection.BEARISH: "медвежий",
}
FUSION_LABELS = {
    FusionEffect.STRENGTHENED: "усилили сигнал",
    FusionEffect.WEAKENED: "ослабили сигнал",
    FusionEffect.NEUTRAL: "нейтрально",
}
REGIME_LABELS = {
    MarketPositioning.SUSTAINABLE_GROWTH: "устойчивый рост",
    MarketPositioning.OVERHEATED_LONG: "перегретый лонг",
    MarketPositioning.SHORT_ACCUMULATION: "накопление шортов",
    MarketPositioning.SHORT_SQUEEZE: "шорт-сквиз",
    MarketPositioning.LONG_SQUEEZE: "лонг-сквиз",
    MarketPositioning.UNCONFIRMED_MOVE: "движение без подтверждения OI",
    MarketPositioning.NEUTRAL: "нейтральный",
}
WARNING_LABELS = {
    ScreeningWarningCode.TECHNICAL_CONFLICT: (
        "Обнаружены противоречащие технические факторы."
    ),
    ScreeningWarningCode.DERIVATIVES_UNAVAILABLE: (
        "Данные деривативов недоступны. Итог основан на техническом анализе."
    ),
    ScreeningWarningCode.DERIVATIVES_WEAKENED: (
        "Деривативы не подтвердили техническое движение и ослабили итоговый сигнал."
    ),
    ScreeningWarningCode.OI_UNCONFIRMED: (
        "Движение цены не подтверждено изменением открытого интереса."
    ),
    ScreeningWarningCode.OVERHEATED_LONG: "Позиционирование в лонг перегрето.",
    ScreeningWarningCode.LIVE_LIQUIDATIONS_INACTIVE: (
        "Онлайн-поток ликвидаций не запущен."
    ),
}
EVIDENCE_LABELS = {
    "trend": "тренд",
    "momentum": "импульс",
    "range_breakout": "пробой диапазона",
    "volume": "объём",
    "volume_expansion": "рост объёма",
    "volatility_expansion": "рост волатильности",
    "structure": "структура",
    "support": "поддержка",
    "resistance": "сопротивление",
    "open_interest": "открытый интерес",
    "funding": "ставка финансирования",
    "liquidations": "ликвидации",
}
DERIVATIVES_REASON_LABELS = {
    "price_up": "цена растёт",
    "price_down": "цена снижается",
    "open_interest_up": "открытый интерес растёт",
    "open_interest_down": "открытый интерес снижается",
    "open_interest_not_confirming": "открытый интерес не подтверждает движение",
    "short_liquidations": "преобладают ликвидации шортов",
    "long_liquidations": "преобладают ликвидации лонгов",
    "funding_high": "ставка финансирования повышена",
    "funding_negative": "ставка финансирования отрицательная",
    "volume_confirmed": "движение подтверждено объёмом",
    "price_move": "зафиксировано движение цены",
    "no_clear_regime": "выраженный режим не определён",
}


@dataclass(frozen=True, slots=True)
class SignalView:
    symbol: str
    asset_class: str
    direction: str
    technical_score: float
    derivatives_score: float | None
    combined_score: float
    confidence: float
    fusion_effect: str
    regime: str
    confirmations: int
    conflicts: int
    explanations: tuple[str, ...]
    warnings: tuple[str, ...]
    derivatives_context: str


@dataclass(frozen=True, slots=True)
class FailureView:
    symbol: str
    stage: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class ReportView:
    generated_at: str
    successful_results: tuple[SignalView, ...]
    failed_instruments: tuple[FailureView, ...]
    ranked_signals: tuple[SignalView, ...]
    market_summary: MarketSummary
    disclaimer: str = DISCLAIMER

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def present_report(report: ScreeningReport) -> ReportView:
    mapped = {
        id(item): _signal_view(item) for item in report.successful_results
    }
    return ReportView(
        generated_at=report.generated_at.isoformat(),
        successful_results=tuple(
            mapped[id(item)] for item in report.successful_results
        ),
        failed_instruments=tuple(
            _failure_view(item) for item in report.failed_instruments
        ),
        ranked_signals=tuple(
            mapped.get(id(item), _signal_view(item))
            for item in report.ranked_signals
        ),
        market_summary=report.market_summary,
    )


def _signal_view(result: ScreeningSignalResult) -> SignalView:
    technical = result.technical_signal
    fused = result.fused_signal
    derivatives = result.derivatives_signal
    technical_score = technical.score if technical is not None else 0.0
    technical_signed = (
        -technical_score
        if result.direction is ScreeningDirection.SHORT
        else technical_score
    )
    explanations = (
        tuple(_evidence_text(item) for item in technical.evidence)
        if technical is not None
        else ("Подходящий технический сигнал не найден.",)
    )
    if derivatives is not None:
        explanations = (
            *explanations,
            *(_derivatives_reason(reason) for reason in derivatives.reasons),
        )
    warning_codes = {warning.code for warning in result.warnings}
    return SignalView(
        symbol=result.instrument.symbol,
        asset_class=result.instrument.asset_class.value,
        direction=DIRECTION_LABELS[result.direction],
        technical_score=technical_score,
        derivatives_score=(
            derivatives.directional_score * 100.0
            if derivatives is not None
            else None
        ),
        combined_score=(
            fused.combined_score if fused is not None else technical_signed
        ),
        confidence=technical.confidence if technical is not None else 0.0,
        fusion_effect=(
            FUSION_LABELS[fused.effect] if fused is not None else "нейтрально"
        ),
        regime=(
            REGIME_LABELS[derivatives.regime]
            if derivatives is not None
            else "нейтральный"
        ),
        confirmations=technical.confirmations if technical is not None else 0,
        conflicts=technical.conflicts if technical is not None else 0,
        explanations=explanations,
        warnings=tuple(WARNING_LABELS[warning.code] for warning in result.warnings),
        derivatives_context=_derivatives_context(
            derivatives is not None,
            derivatives.directional_score if derivatives is not None else None,
            fused.effect if fused is not None else None,
            ScreeningWarningCode.DERIVATIVES_UNAVAILABLE in warning_codes,
        ),
    )


def _failure_view(failure: InstrumentFailure) -> FailureView:
    stage = {
        "technical": "технический анализ",
        "derivatives": "деривативы",
    }.get(failure.stage, "анализ")
    message = (
        "Рыночные данные недоступны."
        if failure.stage == "technical"
        else "Данные деривативов недоступны."
        if failure.stage == "derivatives"
        else "Не удалось выполнить анализ инструмента."
    )
    return FailureView(
        symbol=failure.instrument.symbol,
        stage=stage,
        error_type=failure.error_type,
        message=message,
    )


def format_number(value: float) -> str:
    return f"{value:.1f}".replace(".", ",")


def _evidence_text(evidence: SignalEvidence) -> str:
    detail = evidence.detail
    match = re.fullmatch(
        r"EMA20 ([0-9.eE+-]+) is (above|below) EMA50 ([0-9.eE+-]+)", detail
    )
    if match:
        relation = "выше" if match.group(2) == "above" else "ниже"
        return (
            f"EMA20 {match.group(1).replace('.', ',')} находится {relation} "
            f"EMA50 {match.group(3).replace('.', ',')}."
        )
    match = re.fullmatch(r"RSI14 is ([0-9.eE+-]+)", detail)
    if match:
        return f"RSI14 составляет {match.group(1).replace('.', ',')}."
    match = re.fullmatch(
        r"Close broke the prior 20-bar (high|low) ([0-9.eE+-]+)", detail
    )
    if match:
        boundary = "выше максимума" if match.group(1) == "high" else "ниже минимума"
        return (
            f"Цена закрылась {boundary} последних 20 свечей "
            f"({match.group(2).replace('.', ',')})."
        )
    if detail == "ATR14 expanded at least 25% above its prior baseline":
        return "ATR14 вырос как минимум на 25% относительно предыдущей базы."
    if detail == "Volume is at least 1.5x its 20-bar average":
        return "Объём как минимум в 1,5 раза выше среднего за 20 свечей."
    name = EVIDENCE_LABELS.get(evidence.name, "технический фактор")
    direction = SIGNAL_DIRECTION_LABELS[evidence.direction]
    return f"Фактор «{name}» указывает на {direction} контекст."


def _derivatives_reason(reason: str) -> str:
    return DERIVATIVES_REASON_LABELS.get(
        reason, "Учтён дополнительный фактор деривативов."
    )


def _derivatives_context(
    available: bool,
    directional_score: float | None,
    effect: FusionEffect | None,
    unavailable_warning: bool,
) -> str:
    if not available:
        if unavailable_warning:
            return (
                "Данные деривативов недоступны. "
                "Итог основан на техническом анализе."
            )
        return "Деривативы не включены."
    if effect is FusionEffect.WEAKENED:
        return (
            "Деривативы не подтвердили техническое движение "
            "и ослабили итоговый сигнал."
        )
    if directional_score == 0.0:
        return (
            "Деривативы нейтральны и не подтвердили техническое движение; "
            "итоговый балл учитывает вес и уверенность обоих источников."
        )
    if effect is FusionEffect.STRENGTHENED:
        return "Деривативы подтвердили и усилили технический сигнал."
    return "Деривативы не изменили направление технического сигнала."
