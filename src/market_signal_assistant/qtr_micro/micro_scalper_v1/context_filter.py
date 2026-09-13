from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MarketContext:
    spread_ok: bool
    volatility_ok: bool
    sell_pressure_blocked: bool
    market_data_healthy: bool


@dataclass(frozen=True, slots=True)
class ContextDecision:
    accepted: bool
    reason: str


class LongContextFilter:
    """
    Phase A context gate for QTR Micro Scalper V1.

    The filter answers only one question:
    is the market context acceptable for searching for a LONG setup?
    """

    name = "micro_scalper_v1_long_context"

    def evaluate(self, context: MarketContext) -> ContextDecision:
        if not context.market_data_healthy:
            return ContextDecision(False, "market_data_unhealthy")

        if not context.spread_ok:
            return ContextDecision(False, "spread_not_ok")

        if not context.volatility_ok:
            return ContextDecision(False, "volatility_too_low")

        if context.sell_pressure_blocked:
            return ContextDecision(False, "strong_sell_pressure")

        return ContextDecision(True, "long_context_accepted")
