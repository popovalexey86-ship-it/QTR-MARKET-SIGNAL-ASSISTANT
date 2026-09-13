from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReclaimSnapshot:
    reclaimed_level: float
    sweep_price: float
    current_price: float


@dataclass(frozen=True, slots=True)
class ReclaimDecision:
    accepted: bool
    reason: str
    trigger_price: float | None = None
    invalidation_price: float | None = None


class LongReclaimTrigger:
    """
    Phase D reclaim trigger for QTR Micro Scalper V1.

    The setup is accepted only after price sweeps below a structural level
    and subsequently reclaims that level.

    The sweep low becomes the structural invalidation reference.
    """

    name = "micro_scalper_v1_long_reclaim"

    def evaluate(
        self,
        snapshot: ReclaimSnapshot,
    ) -> ReclaimDecision:
        if (
            snapshot.reclaimed_level <= 0
            or snapshot.sweep_price <= 0
            or snapshot.current_price <= 0
        ):
            return ReclaimDecision(
                accepted=False,
                reason="invalid_price",
            )

        if snapshot.sweep_price >= snapshot.reclaimed_level:
            return ReclaimDecision(
                accepted=False,
                reason="no_valid_downside_sweep",
            )

        if snapshot.current_price < snapshot.reclaimed_level:
            return ReclaimDecision(
                accepted=False,
                reason="level_not_reclaimed",
            )

        return ReclaimDecision(
            accepted=True,
            reason="long_reclaim_confirmed",
            trigger_price=snapshot.current_price,
            invalidation_price=snapshot.sweep_price,
        )
