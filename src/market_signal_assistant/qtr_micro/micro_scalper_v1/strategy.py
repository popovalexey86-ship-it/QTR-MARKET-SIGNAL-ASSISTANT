from __future__ import annotations

from dataclasses import dataclass

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
    LongSetupStateMachine,
    SetupStage,
    SetupState,
)
from market_signal_assistant.qtr_micro.models import MicroDirection
from market_signal_assistant.qtr_micro.strategy import StrategyDecision


@dataclass(frozen=True, slots=True)
class MicroScalperV1Input:
    context: ContextDecision
    sweep: LiquidityEventDecision
    absorption: AbsorptionDecision
    confirmation: BuyerConfirmationDecision
    reclaim: ReclaimDecision
    reclaimed_level: float


@dataclass(frozen=True, slots=True)
class MicroScalperV1Result:
    state: SetupState
    decision: StrategyDecision


class MicroScalperV1:
    """
    Sequential LONG-only orchestration for QTR Micro Scalper V1.

    Required order:
    context -> sell sweep -> bullish absorption
    -> buyer confirmation -> reclaim -> LONG trade.
    """

    name = "micro_scalper_v1"

    def __init__(self) -> None:
        self._machine = LongSetupStateMachine()

    def evaluate(
        self,
        setup: MicroScalperV1Input,
    ) -> MicroScalperV1Result:
        state = SetupState()

        state = self._machine.advance_context(
            state,
            accepted=setup.context.accepted,
        )
        if state.stage is not SetupStage.WAIT_SWEEP:
            return self._skip(state, setup.context.reason)

        state = self._machine.advance_sweep(
            state,
            detected=setup.sweep.detected,
            reclaimed_level=setup.reclaimed_level,
            sweep_price=setup.sweep.sweep_price,
        )
        if state.stage is not SetupStage.WAIT_ABSORPTION:
            return self._skip(state, setup.sweep.reason)

        state = self._machine.advance_absorption(
            state,
            accepted=setup.absorption.accepted,
        )
        if state.stage is not SetupStage.WAIT_CONFIRMATION:
            return self._skip(state, setup.absorption.reason)

        state = self._machine.advance_confirmation(
            state,
            accepted=setup.confirmation.accepted,
        )
        if state.stage is not SetupStage.WAIT_RECLAIM:
            return self._skip(state, setup.confirmation.reason)

        state = self._machine.advance_reclaim(
            state,
            accepted=setup.reclaim.accepted,
        )
        if state.stage is not SetupStage.COMPLETE:
            return self._skip(state, setup.reclaim.reason)

        trigger_price = setup.reclaim.trigger_price
        invalidation_price = setup.reclaim.invalidation_price

        if trigger_price is None or invalidation_price is None:
            return self._skip(state, "reclaim_prices_missing")

        return MicroScalperV1Result(
            state=state,
            decision=StrategyDecision.trade(
                direction=MicroDirection.LONG,
                trigger_price=trigger_price,
                invalidation_price=invalidation_price,
                reason="micro_scalper_v1_long_sequence_complete",
            ),
        )

    @staticmethod
    def _skip(
        state: SetupState,
        reason: str,
    ) -> MicroScalperV1Result:
        return MicroScalperV1Result(
            state=state,
            decision=StrategyDecision.skip(reason),
        )
