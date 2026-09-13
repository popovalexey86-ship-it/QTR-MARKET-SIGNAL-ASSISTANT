from market_signal_assistant.qtr_micro.micro_scalper_v1.context_filter import (
    LongContextFilter,
    MarketContext,
)


def test_long_context_accepts_clean_market() -> None:
    decision = LongContextFilter().evaluate(
        MarketContext(
            spread_ok=True,
            volatility_ok=True,
            sell_pressure_blocked=False,
            market_data_healthy=True,
        )
    )

    assert decision.accepted
    assert decision.reason == "long_context_accepted"


def test_long_context_rejects_unhealthy_market_data() -> None:
    decision = LongContextFilter().evaluate(
        MarketContext(
            spread_ok=True,
            volatility_ok=True,
            sell_pressure_blocked=False,
            market_data_healthy=False,
        )
    )

    assert not decision.accepted
    assert decision.reason == "market_data_unhealthy"


def test_long_context_rejects_strong_sell_pressure() -> None:
    decision = LongContextFilter().evaluate(
        MarketContext(
            spread_ok=True,
            volatility_ok=True,
            sell_pressure_blocked=True,
            market_data_healthy=True,
        )
    )

    assert not decision.accepted
    assert decision.reason == "strong_sell_pressure"
