from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from market_signal_assistant.qtr_micro.models import MicroDirection
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate


class StrategyAction(StrEnum):
    TRADE = "trade"
    SKIP = "skip"


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    """
    Pure strategy-layer decision.

    The strategy decides whether a candidate deserves a trade attempt.
    It does not size positions, calculate leverage, submit orders,
    manage exchange state, or mutate QTR Micro state.
    """

    action: StrategyAction
    reason: str
    direction: MicroDirection | None = None
    trigger_price: float | None = None
    invalidation_price: float | None = None

    @property
    def accepted(self) -> bool:
        return self.action is StrategyAction.TRADE

    @classmethod
    def skip(cls, reason: str) -> StrategyDecision:
        return cls(
            action=StrategyAction.SKIP,
            reason=reason,
        )

    @classmethod
    def trade(
        cls,
        *,
        direction: MicroDirection,
        trigger_price: float,
        invalidation_price: float,
        reason: str,
    ) -> StrategyDecision:
        if trigger_price <= 0:
            raise ValueError("trigger_price must be positive")
        if invalidation_price <= 0:
            raise ValueError("invalidation_price must be positive")

        return cls(
            action=StrategyAction.TRADE,
            reason=reason,
            direction=direction,
            trigger_price=trigger_price,
            invalidation_price=invalidation_price,
        )


class QtrMicroStrategy(Protocol):
    """
    Contract for independently testable QTR Micro strategies.
    """

    name: str

    def evaluate(
        self,
        candidate: QtrSetupCandidate,
        *,
        now: datetime,
    ) -> StrategyDecision: ...
