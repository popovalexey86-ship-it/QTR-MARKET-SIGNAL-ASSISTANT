from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LiquiditySnapshot:
    local_low: float
    trade_price: float
    sell_volume: float
    recent_sell_volume: float


@dataclass(frozen=True, slots=True)
class LiquidityEventDecision:
    detected: bool
    reason: str
    sweep_price: float | None = None


class SellSweepDetector:
    """
    Detect a downside liquidity sweep.

    V1 definition:
    - price trades below the recent local low;
    - aggressive sell volume expands versus recent baseline.
    """

    def __init__(
        self,
        *,
        minimum_volume_ratio: float = 1.5,
    ) -> None:
        self._minimum_volume_ratio = minimum_volume_ratio

    def evaluate(
        self,
        snapshot: LiquiditySnapshot,
    ) -> LiquidityEventDecision:
        if snapshot.local_low <= 0 or snapshot.trade_price <= 0:
            return LiquidityEventDecision(False, "invalid_price")

        if snapshot.recent_sell_volume <= 0:
            return LiquidityEventDecision(False, "invalid_volume_baseline")

        if snapshot.trade_price >= snapshot.local_low:
            return LiquidityEventDecision(False, "local_low_not_swept")

        volume_ratio = (
            snapshot.sell_volume / snapshot.recent_sell_volume
        )

        if volume_ratio < self._minimum_volume_ratio:
            return LiquidityEventDecision(
                False,
                "sell_volume_not_expanded",
            )

        return LiquidityEventDecision(
            True,
            "sell_sweep_detected",
            sweep_price=snapshot.trade_price,
        )
