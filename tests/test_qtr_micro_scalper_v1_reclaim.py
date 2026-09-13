from market_signal_assistant.qtr_micro.micro_scalper_v1.reclaim import (
    LongReclaimTrigger,
    ReclaimSnapshot,
)


def test_accepts_reclaim_after_downside_sweep() -> None:
    decision = LongReclaimTrigger().evaluate(
        ReclaimSnapshot(
            reclaimed_level=100.0,
            sweep_price=99.7,
            current_price=100.1,
        )
    )

    assert decision.accepted is True
    assert decision.reason == "long_reclaim_confirmed"
    assert decision.trigger_price == 100.1
    assert decision.invalidation_price == 99.7


def test_rejects_when_level_not_reclaimed() -> None:
    decision = LongReclaimTrigger().evaluate(
        ReclaimSnapshot(
            reclaimed_level=100.0,
            sweep_price=99.7,
            current_price=99.9,
        )
    )

    assert decision.accepted is False
    assert decision.reason == "level_not_reclaimed"


def test_rejects_when_no_downside_sweep_exists() -> None:
    decision = LongReclaimTrigger().evaluate(
        ReclaimSnapshot(
            reclaimed_level=100.0,
            sweep_price=100.1,
            current_price=100.2,
        )
    )

    assert decision.accepted is False
    assert decision.reason == "no_valid_downside_sweep"


def test_rejects_invalid_prices() -> None:
    decision = LongReclaimTrigger().evaluate(
        ReclaimSnapshot(
            reclaimed_level=100.0,
            sweep_price=0.0,
            current_price=100.1,
        )
    )

    assert decision.accepted is False
    assert decision.reason == "invalid_price"
