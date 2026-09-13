from __future__ import annotations

from dataclasses import dataclass

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    AbsorptionDetection,
    FlowSide,
)


@dataclass(frozen=True, slots=True)
class AbsorptionDecision:
    accepted: bool
    reason: str
    aggressive_flow_ratio: float | None = None
    favorable_price_move_bps: float | None = None
    opposing_depth_retention: float | None = None


class LongAbsorptionGate:
    """
    Phase B absorption gate for QTR Micro Scalper V1.

    Accepts only bullish absorption:
    aggressive sellers hit the book, but price fails to continue lower
    and bid-side liquidity remains present.

    The legacy absorption score is intentionally ignored.
    """

    name = "micro_scalper_v1_long_absorption"

    def evaluate(
        self,
        detection: AbsorptionDetection,
    ) -> AbsorptionDecision:
        if not detection.detected:
            return AbsorptionDecision(
                accepted=False,
                reason="absorption_not_detected",
            )

        if detection.aggressive_side is not FlowSide.SELL:
            return AbsorptionDecision(
                accepted=False,
                reason="absorption_not_sell_side",
            )

        return AbsorptionDecision(
            accepted=True,
            reason="bullish_sell_absorption",
            aggressive_flow_ratio=detection.aggressive_flow_ratio,
            favorable_price_move_bps=detection.favorable_price_move_bps,
            opposing_depth_retention=detection.opposing_depth_retention,
        )
