from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from market_signal_assistant.inplay.models import InPlayDirection, InPlayResult

SIGNIFICANT_MOVE_WARNING_PREFIX = "Движение уже значительно реализовано"
EXTREME_MOVE_WARNING_PREFIX = "Резкое движение уже состоялось"
_PRICE_CHANGE = re.compile(
    r"^[Ии]зменение цены\s+([+−-]?\d+(?:[.,]\d+)?)%"
)


class AutomaticDisplayStatus(Enum):
    LONG = "ЛОНГ"
    SHORT = "ШОРТ"
    WATCH = "СЛЕДИМ"
    LATE_ENTRY = "ПОЗДНИЙ ВХОД"
    DO_NOT_CHASE = "НЕ ДОГОНЯТЬ"


class AutomaticRiskClass(Enum):
    NORMAL = "normal"
    LATE_ENTRY = "late_entry"
    EXTREME = "extreme"


@dataclass(frozen=True, slots=True)
class AutomaticInPlaySemantics:
    result: InPlayResult
    display_status: AutomaticDisplayStatus
    risk_class: AutomaticRiskClass
    user_action: str
    visible_confirmations: tuple[str, ...]
    price_change_24h_pct: float | None

    @property
    def internal_direction(self) -> InPlayDirection:
        return self.result.direction

    @property
    def directional_entry_allowed(self) -> bool:
        return self.display_status in {
            AutomaticDisplayStatus.LONG,
            AutomaticDisplayStatus.SHORT,
        }

    @property
    def protective_watch_allowed(self) -> bool:
        return (
            self.display_status is AutomaticDisplayStatus.DO_NOT_CHASE
            and self.result.inplay_score >= 70.0
        )


def automatic_semantics(result: InPlayResult) -> AutomaticInPlaySemantics:
    price_change = _price_change_24h(result.reasons)
    risk = _risk_class(result.warnings, price_change)
    display = _display_status(result.direction, risk)
    return AutomaticInPlaySemantics(
        result=result,
        display_status=display,
        risk_class=risk,
        user_action=_user_action(display),
        visible_confirmations=visible_confirmations(result.reasons),
        price_change_24h_pct=price_change,
    )


def _risk_class(
    warnings: tuple[str, ...],
    price_change: float | None,
) -> AutomaticRiskClass:
    normalized_warnings = tuple(item.strip().casefold() for item in warnings)
    if any(
        item.startswith(EXTREME_MOVE_WARNING_PREFIX.casefold())
        for item in normalized_warnings
    ) or (price_change is not None and abs(price_change) >= 30.0 - 1e-12):
        return AutomaticRiskClass.EXTREME
    if any(
        item.startswith(SIGNIFICANT_MOVE_WARNING_PREFIX.casefold())
        for item in normalized_warnings
    ) or (price_change is not None and abs(price_change) >= 15.0 - 1e-12):
        return AutomaticRiskClass.LATE_ENTRY
    return AutomaticRiskClass.NORMAL


def _display_status(
    direction: InPlayDirection,
    risk: AutomaticRiskClass,
) -> AutomaticDisplayStatus:
    if risk is AutomaticRiskClass.EXTREME:
        return AutomaticDisplayStatus.DO_NOT_CHASE
    if risk is AutomaticRiskClass.LATE_ENTRY:
        return AutomaticDisplayStatus.LATE_ENTRY
    if direction is InPlayDirection.LONG:
        return AutomaticDisplayStatus.LONG
    if direction is InPlayDirection.SHORT:
        return AutomaticDisplayStatus.SHORT
    return AutomaticDisplayStatus.WATCH


def _user_action(status: AutomaticDisplayStatus) -> str:
    if status is AutomaticDisplayStatus.DO_NOT_CHASE:
        return "Не догонять цену."
    if status is AutomaticDisplayStatus.LATE_ENTRY:
        return "Не входить после уже реализованного движения."
    if status in {AutomaticDisplayStatus.LONG, AutomaticDisplayStatus.SHORT}:
        return "Проверить уровень, стоп и соотношение риска к прибыли."
    return "Ждать пробой или дополнительное подтверждение."


def visible_confirmations(reasons: tuple[str, ...]) -> tuple[str, ...]:
    confirmations: set[str] = set()
    for reason in reasons:
        normalized = " ".join(reason.casefold().replace("ё", "е").split())
        if (
            "пробой" in normalized
            or ("вышла" in normalized and "локальн" in normalized)
            or ("выход" in normalized and "диапазон" in normalized)
        ):
            confirmations.add("Пробой локального диапазона")
    return tuple(sorted(confirmations))


def _price_change_24h(reasons: tuple[str, ...]) -> float | None:
    for reason in reasons:
        match = _PRICE_CHANGE.match(reason.strip())
        if match is None:
            continue
        value = match.group(1).replace(",", ".").replace("−", "-")
        try:
            return float(value)
        except ValueError:
            return None
    return None
