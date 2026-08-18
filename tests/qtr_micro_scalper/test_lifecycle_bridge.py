from __future__ import annotations

from datetime import timedelta

import pytest
from test_shadow_decision import NOW, opportunity

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.lifecycle_bridge import (
    LiveShadowLifecycleBridge,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowBarResult
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowDecisionConfig,
    ShadowDecisionEngine,
    ShadowPriceBar,
    ShadowTrade,
    ShadowTradeEventType,
    ShadowTradeStage,
)


class LifecycleProcessor:
    def __init__(
        self,
        trade: ShadowTrade,
        *,
        config: ShadowDecisionConfig | None = None,
    ) -> None:
        self.trade = trade
        self.engine = ShadowDecisionEngine(config)
        self.bars: list[ShadowPriceBar] = []

    def process_bars(
        self,
        bars: tuple[ShadowPriceBar, ...],
    ) -> tuple[ShadowBarResult, ...]:
        results: list[ShadowBarResult] = []
        for bar in bars:
            self.bars.append(bar)
            self.trade = self.engine.process_bar(self.trade, bar)
            results.append(
                ShadowBarResult(
                    symbol=bar.symbol,
                    trade=self.trade,
                    events=(),
                    error=None,
                )
            )
        return tuple(results)


def planned_trade(
    direction: ShadowDirection = ShadowDirection.LONG,
    *,
    trigger_price: float = 100.0,
    config: ShadowDecisionConfig | None = None,
) -> ShadowTrade:
    decision = ShadowDecisionEngine(config).create_trade(
        opportunity(direction=direction, trigger_price=trigger_price)
    )
    assert decision.trade is not None
    return decision.trade


def observed_trade(
    seconds: float,
    price: float,
    *,
    trade_id: str,
) -> PublicTradeEvent:
    observed_at = NOW + timedelta(seconds=seconds)
    return PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id=trade_id,
        exchange_at=observed_at,
        received_at=observed_at,
        side=TradeSide.BUY,
        price=price,
        quantity=1.0,
        quote_notional=price,
    )


def market_clock(seconds: float, *, update_id: int) -> OrderBookEvent:
    observed_at = NOW + timedelta(seconds=seconds)
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=OrderBookEventType.DELTA,
        exchange_at=observed_at,
        received_at=observed_at,
        update_id=update_id,
    )


def live_bridge(
    direction: ShadowDirection = ShadowDirection.LONG,
    *,
    trigger_price: float = 100.0,
    config: ShadowDecisionConfig | None = None,
) -> tuple[LiveShadowLifecycleBridge, LifecycleProcessor]:
    trade = planned_trade(direction, trigger_price=trigger_price, config=config)
    processor = LifecycleProcessor(trade, config=config)
    bridge = LiveShadowLifecycleBridge(processor)
    assert bridge.activate(trade)
    return bridge, processor


@pytest.mark.parametrize(
    ("direction", "first_price", "touch_price"),
    (
        (ShadowDirection.LONG, 99.0, 100.0),
        (ShadowDirection.SHORT, 101.0, 100.0),
    ),
)
def test_waiting_entry_opens_for_long_and_short_touch(
    direction: ShadowDirection,
    first_price: float,
    touch_price: float,
) -> None:
    bridge, processor = live_bridge(direction)
    bridge.process_event(observed_trade(0.1, first_price, trade_id="first"))
    bridge.process_event(observed_trade(0.8, touch_price, trade_id="touch"))

    result = bridge.process_event(market_clock(1.1, update_id=1))

    assert result[-1].trade is not None
    assert result[-1].trade.stage is ShadowTradeStage.OPEN
    assert processor.bars == [
        ShadowPriceBar(
            symbol="BTCUSDT",
            opened_at=NOW,
            closed_at=NOW + timedelta(seconds=1),
            open=first_price,
            high=max(first_price, touch_price),
            low=min(first_price, touch_price),
            close=touch_price,
        )
    ]


def test_waiting_entry_expires_after_completed_post_deadline_bar() -> None:
    bridge, processor = live_bridge(trigger_price=101.0)
    bridge.process_event(observed_trade(60.1, 100.0, trade_id="after-deadline"))

    result = bridge.process_event(market_clock(61.1, update_id=1))

    assert result[-1].trade is not None
    assert result[-1].trade.stage is ShadowTradeStage.EXPIRED
    assert result[-1].trade.events[-1].event_type is ShadowTradeEventType.EXPIRED
    assert processor.bars[-1].closed_at == NOW + timedelta(seconds=61)


def test_tp1_tp2_and_journal_input_bars_use_only_observed_prices() -> None:
    bridge, processor = live_bridge()
    for seconds, price, trade_id, clock_id in (
        (0.1, 100.0, "entry", 1),
        (1.1, 102.3, "tp1", 2),
        (2.1, 104.5, "tp2", 3),
    ):
        bridge.process_event(observed_trade(seconds, price, trade_id=trade_id))
        bridge.process_event(market_clock(seconds + 0.95, update_id=clock_id))

    assert processor.trade.stage is ShadowTradeStage.CLOSED
    assert [event.event_type for event in processor.trade.events] == [
        ShadowTradeEventType.ENTRY,
        ShadowTradeEventType.TP1,
        ShadowTradeEventType.TP2,
    ]
    assert [bar.close for bar in processor.bars] == [100.0, 102.3, 104.5]


def test_stop_and_breakeven_follow_existing_decision_engine() -> None:
    bridge, processor = live_bridge()
    bridge.process_event(observed_trade(0.1, 100.0, trade_id="entry"))
    bridge.process_event(market_clock(1.1, update_id=1))
    bridge.process_event(observed_trade(1.2, 102.3, trade_id="tp1"))
    bridge.process_event(market_clock(2.1, update_id=2))
    bridge.process_event(observed_trade(2.2, 99.9, trade_id="breakeven"))
    bridge.process_event(market_clock(3.1, update_id=3))

    assert processor.trade.stage is ShadowTradeStage.CLOSED
    assert processor.trade.current_stop == processor.trade.entry_price
    assert processor.trade.events[-1].event_type is ShadowTradeEventType.STOP
    assert processor.trade.events[-1].price == processor.trade.entry_price


def test_initial_stop_is_processed_after_open() -> None:
    bridge, processor = live_bridge()
    bridge.process_event(observed_trade(0.1, 100.0, trade_id="entry"))
    bridge.process_event(market_clock(1.1, update_id=1))
    bridge.process_event(observed_trade(1.2, 97.5, trade_id="stop"))
    bridge.process_event(market_clock(2.1, update_id=2))

    assert processor.trade.stage is ShadowTradeStage.CLOSED
    assert processor.trade.events[-1].event_type is ShadowTradeEventType.STOP


def test_maximum_holding_bars_produces_time_exit() -> None:
    config = ShadowDecisionConfig(maximum_holding_bars=2)
    bridge, processor = live_bridge(config=config)
    bridge.process_event(observed_trade(0.1, 100.0, trade_id="entry"))
    bridge.process_event(market_clock(1.1, update_id=1))
    bridge.process_event(observed_trade(1.2, 100.5, trade_id="held"))
    bridge.process_event(market_clock(2.1, update_id=2))

    assert processor.trade.stage is ShadowTradeStage.CLOSED
    assert processor.trade.bars_held == 2
    assert processor.trade.events[-1].event_type is ShadowTradeEventType.TIME_EXIT


def test_duplicate_and_late_trades_do_not_create_duplicate_bars() -> None:
    bridge, processor = live_bridge()
    first = observed_trade(0.2, 100.0, trade_id="same")
    bridge.process_event(first)
    bridge.process_event(first)
    bridge.process_event(market_clock(1.1, update_id=1))
    bridge.process_event(observed_trade(0.5, 101.0, trade_id="late"))
    bridge.process_event(market_clock(2.1, update_id=2))

    assert len(processor.bars) == 1


def test_trade_order_inside_bucket_is_deterministic() -> None:
    first_bridge, first = live_bridge()
    second_bridge, second = live_bridge()
    early = observed_trade(0.2, 99.0, trade_id="early")
    late = observed_trade(0.8, 101.0, trade_id="late")
    for event in (early, late):
        first_bridge.process_event(event)
    for event in (late, early):
        second_bridge.process_event(event)
    first_bridge.process_event(market_clock(1.1, update_id=1))
    second_bridge.process_event(market_clock(1.1, update_id=1))

    assert first.bars == second.bars
    assert first.bars[0].open == 99.0
    assert first.bars[0].close == 101.0


def test_stop_discards_incomplete_bar_without_synthetic_flush() -> None:
    bridge, processor = live_bridge()
    bridge.process_event(observed_trade(0.2, 100.0, trade_id="incomplete"))

    bridge.stop()
    bridge.process_event(market_clock(2.0, update_id=1))

    assert processor.bars == []
    assert not bridge.is_tracking("BTCUSDT")
