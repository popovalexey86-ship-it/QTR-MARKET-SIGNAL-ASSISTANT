from market_signal_assistant.qtr_micro.micro_scalper_v1.absorption import (
    LongAbsorptionGate,
)
from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    AbsorptionDetection,
    FlowSide,
)


def _detection(
    *,
    detected: bool,
    side: FlowSide,
) -> AbsorptionDetection:
    return AbsorptionDetection(
        detected=detected,
        aggressive_side=side,
        score=99.0,
        aggressive_notional=150_000.0,
        aggressive_flow_ratio=1.8,
        favorable_price_move_bps=0.7,
        opposing_depth_retention=0.92,
        reasons=(),
    )


def test_accepts_detected_sell_side_absorption() -> None:
    decision = LongAbsorptionGate().evaluate(
        _detection(detected=True, side=FlowSide.SELL)
    )

    assert decision.accepted is True
    assert decision.reason == "bullish_sell_absorption"
    assert decision.aggressive_flow_ratio == 1.8
    assert decision.opposing_depth_retention == 0.92


def test_rejects_buy_side_absorption() -> None:
    decision = LongAbsorptionGate().evaluate(
        _detection(detected=True, side=FlowSide.BUY)
    )

    assert decision.accepted is False
    assert decision.reason == "absorption_not_sell_side"


def test_rejects_missing_absorption() -> None:
    decision = LongAbsorptionGate().evaluate(
        _detection(detected=False, side=FlowSide.SELL)
    )

    assert decision.accepted is False
    assert decision.reason == "absorption_not_detected"
