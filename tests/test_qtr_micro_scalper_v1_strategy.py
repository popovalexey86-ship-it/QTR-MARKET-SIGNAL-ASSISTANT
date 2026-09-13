from market_signal_assistant.qtr_micro.micro_scalper_v1.absorption import (
    AbsorptionDecision,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.confirmation import (
    BuyerConfirmationDecision,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.context_filter import (
    ContextDecision,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.liquidity_event import (
    LiquidityEventDecision,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.reclaim import (
    ReclaimDecision,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.state_machine import (
    SetupStage,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.strategy import (
    MicroScalperV1,
    MicroScalperV1Input,
)
from market_signal_assistant.qtr_micro.models import MicroDirection


def _valid_input() -> MicroScalperV1Input:
    return MicroScalperV1Input(
        context=ContextDecision(
            accepted=True,
            reason="long_context_accepted",
        ),
        sweep=LiquidityEventDecision(
            detected=True,
            reason="sell_sweep_detected",
            sweep_price=99.7,
        ),
        absorption=AbsorptionDecision(
            accepted=True,
            reason="bullish_sell_absorption",
            aggressive_flow_ratio=1.8,
            favorable_price_move_bps=0.7,
            opposing_depth_retention=0.92,
        ),
        confirmation=BuyerConfirmationDecision(
            accepted=True,
            reason="buyer_confirmed",
            delta_1s=1000.0,
            delta_5s=3000.0,
            imbalance_l5=0.15,
        ),
        reclaim=ReclaimDecision(
            accepted=True,
            reason="long_reclaim_confirmed",
            trigger_price=100.1,
            invalidation_price=99.7,
        ),
        reclaimed_level=100.0,
    )


def test_complete_sequence_emits_long_trade() -> None:
    result = MicroScalperV1().evaluate(_valid_input())

    assert result.state.stage is SetupStage.COMPLETE
    assert result.decision.accepted is True
    assert result.decision.direction is MicroDirection.LONG
    assert result.decision.trigger_price == 100.1
    assert result.decision.invalidation_price == 99.7


def test_context_failure_skips() -> None:
    setup = _valid_input()
    setup = MicroScalperV1Input(
        context=ContextDecision(False, "spread_not_ok"),
        sweep=setup.sweep,
        absorption=setup.absorption,
        confirmation=setup.confirmation,
        reclaim=setup.reclaim,
        reclaimed_level=setup.reclaimed_level,
    )

    result = MicroScalperV1().evaluate(setup)

    assert result.decision.accepted is False
    assert result.decision.reason == "spread_not_ok"


def test_absorption_failure_skips() -> None:
    setup = _valid_input()
    setup = MicroScalperV1Input(
        context=setup.context,
        sweep=setup.sweep,
        absorption=AbsorptionDecision(
            accepted=False,
            reason="absorption_not_detected",
        ),
        confirmation=setup.confirmation,
        reclaim=setup.reclaim,
        reclaimed_level=setup.reclaimed_level,
    )

    result = MicroScalperV1().evaluate(setup)

    assert result.decision.accepted is False
    assert result.decision.reason == "absorption_not_detected"


def test_reclaim_failure_skips() -> None:
    setup = _valid_input()
    setup = MicroScalperV1Input(
        context=setup.context,
        sweep=setup.sweep,
        absorption=setup.absorption,
        confirmation=setup.confirmation,
        reclaim=ReclaimDecision(
            accepted=False,
            reason="level_not_reclaimed",
        ),
        reclaimed_level=setup.reclaimed_level,
    )

    result = MicroScalperV1().evaluate(setup)

    assert result.decision.accepted is False
    assert result.decision.reason == "level_not_reclaimed"
