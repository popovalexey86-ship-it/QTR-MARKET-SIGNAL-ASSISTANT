from __future__ import annotations

from dataclasses import dataclass

from market_signal_assistant.derivatives.models import (
    DerivativesSnapshot,
    MarketPositioning,
    MarketPositioningSignal,
)


@dataclass(frozen=True, slots=True)
class DerivativesThresholds:
    price_move: float = 0.01
    open_interest_move: float = 0.02
    high_funding: float = 0.0005
    volume_confirmation: float = 0.20
    liquidation_imbalance: float = 2.0


class DerivativesIntelligence:
    def __init__(self, thresholds: DerivativesThresholds | None = None) -> None:
        self._thresholds = thresholds or DerivativesThresholds()

    def analyze(self, snapshot: DerivativesSnapshot) -> MarketPositioningSignal:
        t = self._thresholds
        price = snapshot.price_change
        oi = snapshot.open_interest_change
        long_liq = snapshot.long_liquidations
        short_liq = snapshot.short_liquidations

        if (
            price >= t.price_move
            and oi <= -t.open_interest_move
            and self._dominates(short_liq, long_liq, t.liquidation_imbalance)
        ):
            return self._signal(
                snapshot,
                MarketPositioning.SHORT_SQUEEZE,
                0.9,
                90.0,
                "price_up",
                "open_interest_down",
                "short_liquidations",
            )
        if (
            price <= -t.price_move
            and oi <= -t.open_interest_move
            and self._dominates(long_liq, short_liq, t.liquidation_imbalance)
        ):
            return self._signal(
                snapshot,
                MarketPositioning.LONG_SQUEEZE,
                -0.9,
                90.0,
                "price_down",
                "open_interest_down",
                "long_liquidations",
            )
        if (
            price >= t.price_move
            and oi >= t.open_interest_move
            and snapshot.funding_rate >= t.high_funding
        ):
            return self._signal(
                snapshot,
                MarketPositioning.OVERHEATED_LONG,
                -0.35,
                85.0,
                "price_up",
                "open_interest_up",
                "funding_high",
            )
        if (
            oi >= t.open_interest_move
            and price <= 0
            and snapshot.funding_rate <= -t.high_funding
        ):
            return self._signal(
                snapshot,
                MarketPositioning.SHORT_ACCUMULATION,
                -0.65,
                80.0,
                "open_interest_up",
                "funding_negative",
            )
        if (
            price >= t.price_move
            and oi >= t.open_interest_move
            and snapshot.volume_change >= t.volume_confirmation
        ):
            return self._signal(
                snapshot,
                MarketPositioning.SUSTAINABLE_GROWTH,
                0.8,
                80.0,
                "price_up",
                "open_interest_up",
                "volume_confirmed",
            )
        if abs(price) >= t.price_move and abs(oi) < t.open_interest_move:
            return self._signal(
                snapshot,
                MarketPositioning.UNCONFIRMED_MOVE,
                0.2 if price > 0 else -0.2,
                65.0,
                "price_move",
                "open_interest_not_confirming",
            )
        return self._signal(
            snapshot,
            MarketPositioning.NEUTRAL,
            0.0,
            40.0,
            "no_clear_regime",
        )

    @staticmethod
    def _dominates(value: float, other: float, ratio: float) -> bool:
        return value > 0 and (other == 0 or value >= other * ratio)

    @staticmethod
    def _signal(
        snapshot: DerivativesSnapshot,
        regime: MarketPositioning,
        score: float,
        confidence: float,
        *reasons: str,
    ) -> MarketPositioningSignal:
        return MarketPositioningSignal(
            regime=regime,
            directional_score=score,
            confidence=confidence,
            snapshot=snapshot,
            reasons=tuple(reasons),
        )
