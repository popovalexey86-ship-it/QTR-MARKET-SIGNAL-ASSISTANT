from datetime import UTC, datetime

from market_signal_assistant.qtr_micro.micro_scalper_v1.confirmation import (
    BuyerConfirmationGate,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookMetrics
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)


def _trade_flow(
    *,
    delta_1s: float = 1000.0,
    delta_5s: float = 3000.0,
    delta_15s: float = -3000.0,
) -> TradeFlowMetrics:
    return TradeFlowMetrics(
        symbol="BTCUSDT",
        as_of=NOW,
        buy_notional_1s=2000.0,
        sell_notional_1s=1000.0,
        delta_1s=delta_1s,
        delta_5s=delta_5s,
        delta_15s=delta_15s,
        delta_60s=-10000.0,
        cvd_process=0.0,
        cvd_utc_day=0.0,
        cvd_episode=None,
        trade_count_5s=10,
        largest_trade_5s=5000.0,
        block_delta_60s=0.0,
        rpi_delta_60s=0.0,
        last_trade_at=NOW,
    )


def _orderbook(
    *,
    ready: bool = True,
    imbalance_l5: float | None = 0.15,
) -> OrderBookMetrics:
    return OrderBookMetrics(
        symbol="BTCUSDT",
        as_of=NOW,
        book_exchange_at=NOW,
        book_age_ms=10.0,
        update_id=1,
        cross_sequence=1,
        bid_levels=10,
        ask_levels=10,
        best_bid=100.0,
        best_ask=100.1,
        mid_price=100.05,
        microprice=100.05,
        spread_bps=10.0,
        bid_depth_5bps=100000.0,
        ask_depth_5bps=90000.0,
        bid_depth_10bps=200000.0,
        ask_depth_10bps=180000.0,
        bid_depth_25bps=400000.0,
        ask_depth_25bps=380000.0,
        imbalance_l1=0.10,
        imbalance_l5=imbalance_l5,
        imbalance_l10=0.10,
        imbalance_l25=0.05,
        imbalance_l50=0.00,
        ready=ready,
        health_reasons=(),
    )


def test_accepts_buyer_confirmation() -> None:
    decision = BuyerConfirmationGate().evaluate(
        _trade_flow(),
        _orderbook(),
    )

    assert decision.accepted is True
    assert decision.reason == "buyer_confirmed"


def test_rejects_when_immediate_buyer_flow_missing() -> None:
    decision = BuyerConfirmationGate().evaluate(
        _trade_flow(delta_1s=-100.0),
        _orderbook(),
    )

    assert decision.accepted is False
    assert decision.reason == "immediate_buyer_flow_missing"


def test_rejects_when_short_term_flow_not_improving() -> None:
    decision = BuyerConfirmationGate().evaluate(
        _trade_flow(
            delta_1s=1000.0,
            delta_5s=-2000.0,
            delta_15s=-3000.0,
        ),
        _orderbook(),
    )

    assert decision.accepted is False
    assert decision.reason == "short_term_flow_not_improving"


def test_rejects_sell_heavy_orderbook() -> None:
    decision = BuyerConfirmationGate().evaluate(
        _trade_flow(),
        _orderbook(imbalance_l5=-0.10),
    )

    assert decision.accepted is False
    assert decision.reason == "orderbook_still_sell_heavy"


def test_rejects_unhealthy_orderbook() -> None:
    decision = BuyerConfirmationGate().evaluate(
        _trade_flow(),
        _orderbook(ready=False),
    )

    assert decision.accepted is False
    assert decision.reason == "orderbook_not_ready"
