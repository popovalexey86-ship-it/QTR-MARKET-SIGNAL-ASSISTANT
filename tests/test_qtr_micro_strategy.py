from market_signal_assistant.qtr_micro.models import MicroDirection
from market_signal_assistant.qtr_micro.strategy import (
    StrategyAction,
    StrategyDecision,
)


def test_strategy_skip_is_not_accepted() -> None:
    decision = StrategyDecision.skip("No micro edge.")

    assert decision.action is StrategyAction.SKIP
    assert not decision.accepted
    assert decision.direction is None
    assert decision.trigger_price is None
    assert decision.invalidation_price is None


def test_strategy_trade_contains_only_strategy_intent() -> None:
    decision = StrategyDecision.trade(
        direction=MicroDirection.LONG,
        trigger_price=100.0,
        invalidation_price=99.0,
        reason="Micro setup confirmed.",
    )

    assert decision.action is StrategyAction.TRADE
    assert decision.accepted
    assert decision.direction is MicroDirection.LONG
    assert decision.trigger_price == 100.0
    assert decision.invalidation_price == 99.0


def test_strategy_trade_rejects_invalid_prices() -> None:
    try:
        StrategyDecision.trade(
            direction=MicroDirection.LONG,
            trigger_price=0.0,
            invalidation_price=99.0,
            reason="invalid",
        )
    except ValueError as error:
        assert "trigger_price" in str(error)
    else:
        raise AssertionError("Expected ValueError")
