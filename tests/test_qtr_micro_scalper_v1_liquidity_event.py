from market_signal_assistant.qtr_micro.micro_scalper_v1.liquidity_event import (
    LiquiditySnapshot,
    SellSweepDetector,
)


def test_sell_sweep_detected() -> None:
    decision = SellSweepDetector().evaluate(
        LiquiditySnapshot(
            local_low=100.0,
            trade_price=99.8,
            sell_volume=180.0,
            recent_sell_volume=100.0,
        )
    )

    assert decision.detected
    assert decision.reason == "sell_sweep_detected"
    assert decision.sweep_price == 99.8


def test_sell_sweep_rejects_price_without_low_break() -> None:
    decision = SellSweepDetector().evaluate(
        LiquiditySnapshot(
            local_low=100.0,
            trade_price=100.1,
            sell_volume=200.0,
            recent_sell_volume=100.0,
        )
    )

    assert not decision.detected
    assert decision.reason == "local_low_not_swept"


def test_sell_sweep_requires_volume_expansion() -> None:
    decision = SellSweepDetector().evaluate(
        LiquiditySnapshot(
            local_low=100.0,
            trade_price=99.8,
            sell_volume=120.0,
            recent_sell_volume=100.0,
        )
    )

    assert not decision.detected
    assert decision.reason == "sell_volume_not_expanded"
