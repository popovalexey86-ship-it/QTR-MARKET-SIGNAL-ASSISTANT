from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from market_signal_assistant.qtr_signal_outcome.models import (
    Direction,
    MarketCandle,
    SignalSnapshot,
)

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)


def source_record(**changes: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "timestamp": NOW.isoformat(),
        "symbol": "BTCUSDT",
        "direction": "UP",
        "type": "BREAKOUT",
        "decision": "send",
        "sent": True,
        "delivery_committed": True,
        "semantic_fingerprint": "fingerprint-1",
        "telegram_quality_score": 95.0,
        "quality_components": {
            "structure": 20.0,
            "correct_side": 15.0,
        },
        "price_context": {
            "observed_at": (NOW - timedelta(seconds=5)).isoformat(),
            "market_price": 100.0,
            "trigger_price": 99.5,
            "invalidation_price": 98.0,
            "atr": 2.0,
            "setup_confidence": 88.0,
            "volume_confirmation": True,
            "volatility_confirmation": True,
            "liquidity_ok": True,
            "confirmations": ["volume", "structure"],
            "warnings": ["manual review"],
        },
    }
    record.update(changes)
    return record


def signal(direction: Direction = Direction.LONG) -> SignalSnapshot:
    return SignalSnapshot(
        signal_id="signal-1",
        symbol="BTCUSDT",
        direction=direction,
        setup_type="BREAKOUT",
        signal_timestamp=NOW,
        source_observed_at=NOW - timedelta(seconds=5),
        semantic_fingerprint="fingerprint-1",
        signal_price=100.0,
        trigger_price=99.5,
        invalidation_price=98.0 if direction is Direction.LONG else 102.0,
        atr=2.0,
        setup_confidence=88.0,
        telegram_quality_score=95.0,
        quality_components=(("correct_side", 15.0), ("structure", 20.0)),
        volume_confirmation=True,
        volatility_confirmation=True,
        liquidity_ok=True,
        confirmations=("structure", "volume"),
        warnings=("manual review",),
    )


def candle(
    minute: int,
    *,
    open_price: float = 100.0,
    high: float = 100.4,
    low: float = 99.8,
    close: float = 100.2,
) -> MarketCandle:
    opened = NOW + timedelta(minutes=minute - 1)
    return MarketCandle(
        symbol="BTCUSDT",
        opened_at=opened,
        closed_at=opened + timedelta(minutes=1),
        open=open_price,
        high=high,
        low=low,
        close=close,
    )
