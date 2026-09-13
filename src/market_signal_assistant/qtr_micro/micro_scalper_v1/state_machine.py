from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SetupStage(StrEnum):
    WAIT_CONTEXT = "WAIT_CONTEXT"
    WAIT_SWEEP = "WAIT_SWEEP"
    WAIT_ABSORPTION = "WAIT_ABSORPTION"
    WAIT_CONFIRMATION = "WAIT_CONFIRMATION"
    WAIT_RECLAIM = "WAIT_RECLAIM"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class SetupState:
    stage: SetupStage = SetupStage.WAIT_CONTEXT
    reclaimed_level: float | None = None
    sweep_price: float | None = None


class LongSetupStateMachine:
    """
    Sequential state machine for QTR Micro Scalper V1.

    The machine enforces event order:
    context -> sweep -> absorption -> confirmation -> reclaim.
    """

    def advance_context(
        self,
        state: SetupState,
        *,
        accepted: bool,
    ) -> SetupState:
        if state.stage is not SetupStage.WAIT_CONTEXT:
            return state
        if not accepted:
            return state
        return SetupState(stage=SetupStage.WAIT_SWEEP)

    def advance_sweep(
        self,
        state: SetupState,
        *,
        detected: bool,
        reclaimed_level: float | None,
        sweep_price: float | None,
    ) -> SetupState:
        if state.stage is not SetupStage.WAIT_SWEEP:
            return state

        if not detected:
            return state

        if (
            reclaimed_level is None
            or sweep_price is None
            or reclaimed_level <= 0
            or sweep_price <= 0
            or sweep_price >= reclaimed_level
        ):
            return state

        return SetupState(
            stage=SetupStage.WAIT_ABSORPTION,
            reclaimed_level=reclaimed_level,
            sweep_price=sweep_price,
        )

    def advance_absorption(
        self,
        state: SetupState,
        *,
        accepted: bool,
    ) -> SetupState:
        if state.stage is not SetupStage.WAIT_ABSORPTION:
            return state
        if not accepted:
            return state
        return SetupState(
            stage=SetupStage.WAIT_CONFIRMATION,
            reclaimed_level=state.reclaimed_level,
            sweep_price=state.sweep_price,
        )

    def advance_confirmation(
        self,
        state: SetupState,
        *,
        accepted: bool,
    ) -> SetupState:
        if state.stage is not SetupStage.WAIT_CONFIRMATION:
            return state
        if not accepted:
            return state
        return SetupState(
            stage=SetupStage.WAIT_RECLAIM,
            reclaimed_level=state.reclaimed_level,
            sweep_price=state.sweep_price,
        )

    def advance_reclaim(
        self,
        state: SetupState,
        *,
        accepted: bool,
    ) -> SetupState:
        if state.stage is not SetupStage.WAIT_RECLAIM:
            return state
        if not accepted:
            return state
        return SetupState(
            stage=SetupStage.COMPLETE,
            reclaimed_level=state.reclaimed_level,
            sweep_price=state.sweep_price,
        )
