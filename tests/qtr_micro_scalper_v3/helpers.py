from __future__ import annotations

from datetime import UTC, datetime, timedelta

from market_signal_assistant.qtr_micro_scalper_v3.models import (
    ImpulseDirection,
    ImpulseSnapshot,
    SweepDirection,
)

NOW = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def snapshot(direction: ImpulseDirection = ImpulseDirection.LONG) -> ImpulseSnapshot:
    sign = 1.0 if direction is ImpulseDirection.LONG else -1.0
    return ImpulseSnapshot(
        symbol="BTCUSDT",
        observed_at=NOW,
        source_at=NOW - timedelta(milliseconds=100),
        impulse_id=f"BTC-{direction.value}-1",
        impulse_started_at=NOW - timedelta(seconds=6),
        direction=direction,
        market_price=100.0,
        best_bid=99.99,
        best_ask=100.01,
        spread_bps=2.0,
        bid_depth_10bps=100_000.0,
        ask_depth_10bps=100_000.0,
        delta_1s=30_000.0 * sign,
        delta_5s=100_000.0 * sign,
        delta_15s=150_000.0 * sign,
        flow_imbalance_5s=0.50 * sign,
        flow_acceleration=1.5,
        price_displacement_1s_bps=2.0 * sign,
        price_displacement_5s_bps=8.0 * sign,
        price_displacement_15s_bps=12.0 * sign,
        impulse_displacement_bps=12.0,
        price_response_bps_per_10k=0.8,
        estimated_potential_bps=40.0,
        local_volatility_bps=18.0,
        orderbook_imbalance=0.20 * sign,
        sweep_direction=(
            SweepDirection.UP
            if direction is ImpulseDirection.LONG
            else SweepDirection.DOWN
        ),
        absorption_detected=False,
        trigger_progress_atr=0.4,
    )
