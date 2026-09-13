from __future__ import annotations

from dataclasses import dataclass

from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookMetrics
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics


@dataclass(frozen=True, slots=True)
class BuyerConfirmationDecision:
    accepted: bool
    reason: str
    delta_1s: float
    delta_5s: float
    imbalance_l5: float | None


class BuyerConfirmationGate:
    """
    Phase C buyer confirmation for QTR Micro Scalper V1.

    This gate is evaluated only after SELL sweep + bullish absorption.

    V1 requires:
    - positive immediate trade-flow delta;
    - improving short-term delta;
    - non-negative L5 order-book imbalance.

    Thresholds are intentionally simple and provisional.
    They are not treated as validated trading parameters.
    """

    name = "micro_scalper_v1_buyer_confirmation"

    def evaluate(
        self,
        trade_flow: TradeFlowMetrics,
        orderbook: OrderBookMetrics,
    ) -> BuyerConfirmationDecision:
        if not orderbook.ready:
            return BuyerConfirmationDecision(
                accepted=False,
                reason="orderbook_not_ready",
                delta_1s=trade_flow.delta_1s,
                delta_5s=trade_flow.delta_5s,
                imbalance_l5=orderbook.imbalance_l5,
            )

        if trade_flow.delta_1s <= 0:
            return BuyerConfirmationDecision(
                accepted=False,
                reason="immediate_buyer_flow_missing",
                delta_1s=trade_flow.delta_1s,
                delta_5s=trade_flow.delta_5s,
                imbalance_l5=orderbook.imbalance_l5,
            )

        normalized_delta_15s = trade_flow.delta_15s / 3.0
        if trade_flow.delta_5s <= normalized_delta_15s:
            return BuyerConfirmationDecision(
                accepted=False,
                reason="short_term_flow_not_improving",
                delta_1s=trade_flow.delta_1s,
                delta_5s=trade_flow.delta_5s,
                imbalance_l5=orderbook.imbalance_l5,
            )

        if orderbook.imbalance_l5 is None:
            return BuyerConfirmationDecision(
                accepted=False,
                reason="imbalance_unavailable",
                delta_1s=trade_flow.delta_1s,
                delta_5s=trade_flow.delta_5s,
                imbalance_l5=None,
            )

        if orderbook.imbalance_l5 < 0:
            return BuyerConfirmationDecision(
                accepted=False,
                reason="orderbook_still_sell_heavy",
                delta_1s=trade_flow.delta_1s,
                delta_5s=trade_flow.delta_5s,
                imbalance_l5=orderbook.imbalance_l5,
            )

        return BuyerConfirmationDecision(
            accepted=True,
            reason="buyer_confirmed",
            delta_1s=trade_flow.delta_1s,
            delta_5s=trade_flow.delta_5s,
            imbalance_l5=orderbook.imbalance_l5,
        )
