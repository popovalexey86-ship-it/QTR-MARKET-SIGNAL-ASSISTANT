from market_signal_assistant.qtr_micro.micro_scalper_v1.state_machine import (
    LongSetupStateMachine,
    SetupStage,
    SetupState,
)


def test_full_sequence_reaches_complete() -> None:
    machine = LongSetupStateMachine()
    state = SetupState()

    state = machine.advance_context(state, accepted=True)
    assert state.stage is SetupStage.WAIT_SWEEP

    state = machine.advance_sweep(
        state,
        detected=True,
        reclaimed_level=100.0,
        sweep_price=99.7,
    )
    assert state.stage is SetupStage.WAIT_ABSORPTION

    state = machine.advance_absorption(state, accepted=True)
    assert state.stage is SetupStage.WAIT_CONFIRMATION

    state = machine.advance_confirmation(state, accepted=True)
    assert state.stage is SetupStage.WAIT_RECLAIM

    state = machine.advance_reclaim(state, accepted=True)
    assert state.stage is SetupStage.COMPLETE
    assert state.reclaimed_level == 100.0
    assert state.sweep_price == 99.7


def test_cannot_skip_required_stages() -> None:
    machine = LongSetupStateMachine()
    state = SetupState()

    state = machine.advance_absorption(state, accepted=True)
    state = machine.advance_confirmation(state, accepted=True)
    state = machine.advance_reclaim(state, accepted=True)

    assert state.stage is SetupStage.WAIT_CONTEXT


def test_rejected_stage_does_not_advance() -> None:
    machine = LongSetupStateMachine()
    state = machine.advance_context(
        SetupState(),
        accepted=True,
    )

    state = machine.advance_sweep(
        state,
        detected=False,
        reclaimed_level=100.0,
        sweep_price=99.7,
    )

    assert state.stage is SetupStage.WAIT_SWEEP


def test_invalid_sweep_structure_does_not_advance() -> None:
    machine = LongSetupStateMachine()

    state = machine.advance_context(
        SetupState(),
        accepted=True,
    )

    state = machine.advance_sweep(
        state,
        detected=True,
        reclaimed_level=100.0,
        sweep_price=100.1,
    )

    assert state.stage is SetupStage.WAIT_SWEEP
