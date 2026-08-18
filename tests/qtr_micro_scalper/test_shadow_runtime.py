from dataclasses import FrozenInstanceError, replace
from datetime import timedelta

import pytest
from test_snapshot import (
    NOW,
    Components,
    build_complete,
    components,
)

from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowDecisionConfig,
    ShadowDecisionEngine,
    ShadowPriceBar,
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntime,
    ShadowRuntimeConfig,
    ShadowRuntimeEventType,
)
from market_signal_assistant.qtr_micro_scalper.snapshot import (
    MicrostructureSnapshotBundle,
    SnapshotReadiness,
    simulate_microstructure_snapshot,
)


def bar(
    index: int,
    *,
    symbol: str = "BTCUSDT",
    open_price: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.0,
) -> ShadowPriceBar:
    opened_at = NOW + timedelta(seconds=index)
    return ShadowPriceBar(
        symbol=symbol,
        opened_at=opened_at,
        closed_at=opened_at + timedelta(seconds=1),
        open=open_price,
        high=high,
        low=low,
        close=close,
    )


def for_symbol(values: Components, symbol: str) -> Components:
    market_state = replace(values.market_state, symbol=symbol)
    price_context = replace(values.setup_context.price_context, symbol=symbol)
    setup_context = replace(
        values.setup_context,
        symbol=symbol,
        market_state=market_state,
        price_context=price_context,
    )
    return replace(
        values,
        trade_flow=replace(values.trade_flow, symbol=symbol),
        orderbook=replace(values.orderbook, symbol=symbol),
        liquidity=replace(values.liquidity, symbol=symbol),
        market_state=market_state,
        setup_context=setup_context,
    )


def at_offset(values: Components, seconds: int) -> Components:
    assessed_at = NOW + timedelta(seconds=seconds)
    market_state = replace(values.market_state, assessed_at=assessed_at)
    price_context = replace(
        values.setup_context.price_context,
        assessed_at=assessed_at,
    )
    setup_context = replace(
        values.setup_context,
        assessed_at=assessed_at,
        market_state=market_state,
        price_context=price_context,
    )
    return replace(
        values,
        trade_flow=replace(
            values.trade_flow,
            as_of=assessed_at,
            last_trade_at=assessed_at - timedelta(milliseconds=10),
        ),
        orderbook=replace(
            values.orderbook,
            as_of=assessed_at,
            book_exchange_at=assessed_at - timedelta(milliseconds=10),
        ),
        market_state=market_state,
        setup_context=setup_context,
    )
def bundle_for(
    values: Components,
    *,
    generated_offset: int = 0,
) -> MicrostructureSnapshotBundle:
    return simulate_microstructure_snapshot(
        symbol=values.trade_flow.symbol,
        generated_at=NOW + timedelta(seconds=generated_offset),
        trade_flow=values.trade_flow,
        orderbook=values.orderbook,
        liquidity=values.liquidity,
        market_state=values.market_state,
        setup_context=values.setup_context,
        scalper_score=values.scalper_score,
    )


def test_ready_snapshot_is_rescored_and_creates_waiting_trade() -> None:
    bundle = build_complete()
    result = ShadowRuntime().process_snapshot(bundle)
    assert result.accepted is True
    assert result.score is not None
    assert result.score.total_score != bundle.scalper_score.total_score  # type: ignore[union-attr]
    assert result.trade is not None
    assert result.trade.stage is ShadowTradeStage.WAITING_ENTRY
    assert result.events[0].event_type is ShadowRuntimeEventType.ENTRY_CREATED
    assert result.events[0].message.startswith("🔥")


def test_not_ready_snapshot_is_rejected_before_trade_creation() -> None:
    values = components()
    partial = simulate_microstructure_snapshot(
        symbol="BTCUSDT",
        generated_at=NOW,
        trade_flow=values.trade_flow,
        orderbook=values.orderbook,
    )
    assert partial.readiness is SnapshotReadiness.PARTIAL
    result = ShadowRuntime().process_snapshot(partial)
    assert result.accepted is False
    assert result.score is None
    assert result.trade is None


def test_blocked_recomputed_score_does_not_create_trade() -> None:
    values = components()
    opposite = replace(
        values.liquidity,
        pressure=replace(
            values.liquidity.pressure,
            combined_pressure=-0.8,
            direction=values.liquidity.pressure.direction.SELL,
        ),
    )
    bundle = bundle_for(replace(values, liquidity=opposite))
    result = ShadowRuntime().process_snapshot(bundle)
    assert result.accepted is False
    assert result.score is not None
    assert result.score.decision.value == "BLOCKED"
    assert result.trade is None


def test_duplicate_active_symbol_is_suppressed() -> None:
    runtime = ShadowRuntime()
    bundle = build_complete()
    assert runtime.process_snapshot(bundle).accepted is True
    duplicate = runtime.process_snapshot(bundle)
    assert duplicate.accepted is False
    assert duplicate.reasons == ("An active shadow trade already exists for symbol.",)
    assert len(runtime.active_trades()) == 1


def test_trade_ids_are_deterministic_across_offline_runtimes() -> None:
    first = ShadowRuntime().process_snapshot(build_complete()).trade
    second = ShadowRuntime().process_snapshot(build_complete()).trade
    assert first is not None and second is not None
    assert first.trade_id == second.trade_id
    assert first.trade_id.startswith("shadow-")


def test_maximum_concurrent_shadow_trades_is_enforced() -> None:
    runtime = ShadowRuntime(ShadowRuntimeConfig(max_concurrent_trades=1))
    assert runtime.process_snapshot(build_complete()).accepted is True
    eth = for_symbol(components(), "ETHUSDT")
    rejected = runtime.process_snapshot(bundle_for(eth))
    assert rejected.accepted is False
    assert rejected.reasons == ("Maximum concurrent shadow trades reached.",)


def test_open_tp1_tp2_lifecycle_and_journal() -> None:
    runtime = ShadowRuntime()
    created = runtime.process_snapshot(build_complete())
    assert created.trade is not None

    opened = runtime.process_bar(bar(1))
    assert opened.trade is not None
    assert opened.trade.stage is ShadowTradeStage.OPEN
    assert opened.events[0].event_type is ShadowRuntimeEventType.OPEN

    tp1 = runtime.process_bar(bar(3, high=102.3, low=100.0, close=102.0))
    assert tp1.trade is not None
    assert tp1.trade.stage is ShadowTradeStage.TP1_HIT
    assert tp1.events[0].event_type is ShadowRuntimeEventType.TP1_REACHED
    assert tp1.events[0].stage is ShadowTradeStage.TP1_HIT

    tp2 = runtime.process_bar(
        bar(5, open_price=102.0, high=104.5, low=101.0, close=104.0)
    )
    assert tp2.trade is not None
    assert tp2.trade.stage is ShadowTradeStage.CLOSED
    assert tp2.events[0].event_type is ShadowRuntimeEventType.TP2_REACHED
    assert tp2.events[0].stage is ShadowTradeStage.CLOSED
    assert runtime.active_trades() == ()
    assert runtime.completed_trades() == (tp2.trade,)
    assert [event.event_type for event in runtime.journal()] == [
        ShadowRuntimeEventType.ENTRY_CREATED,
        ShadowRuntimeEventType.OPEN,
        ShadowRuntimeEventType.TP1_REACHED,
        ShadowRuntimeEventType.TP2_REACHED,
    ]


def test_stop_closes_and_emits_stopped() -> None:
    runtime = ShadowRuntime()
    runtime.process_snapshot(build_complete())
    stopped = runtime.process_bar(bar(1, high=101.0, low=97.0, close=98.0))
    assert stopped.trade is not None
    assert stopped.trade.stage is ShadowTradeStage.CLOSED
    assert [event.event_type for event in stopped.events] == [
        ShadowRuntimeEventType.OPEN,
        ShadowRuntimeEventType.STOPPED,
    ]
    assert stopped.events[-1].message.startswith("🛑")


def test_time_exit_has_distinct_runtime_event() -> None:
    runtime = ShadowRuntime(
        decision_engine=ShadowDecisionEngine(
            ShadowDecisionConfig(maximum_holding_bars=1)
        )
    )
    runtime.process_snapshot(build_complete())

    exited = runtime.process_bar(bar(1, high=101.0, low=99.0, close=100.5))

    assert exited.trade is not None
    assert exited.trade.stage is ShadowTradeStage.CLOSED
    assert exited.events[-1].event_type is ShadowRuntimeEventType.TIME_EXIT


def test_waiting_entry_expires() -> None:
    runtime = ShadowRuntime()
    runtime.process_snapshot(build_complete())
    expired = runtime.process_bar(
        bar(61, open_price=99.0, high=99.5, low=98.5, close=99.0)
    )
    assert expired.trade is not None
    assert expired.trade.stage is ShadowTradeStage.EXPIRED
    assert expired.events[0].event_type is ShadowRuntimeEventType.EXPIRED
    assert expired.events[0].message.startswith("⏱")


def test_cooldown_blocks_new_trade_after_terminal_state() -> None:
    runtime = ShadowRuntime(ShadowRuntimeConfig(cooldown_seconds=300))
    runtime.process_snapshot(build_complete())
    runtime.process_bar(bar(1, high=101.0, low=97.0, close=98.0))
    result = runtime.process_snapshot(bundle_for(components(), generated_offset=10))
    assert result.accepted is False
    assert result.reasons == ("Shadow trade cooldown is active.",)


def test_duplicate_trade_id_is_suppressed_after_close_and_zero_cooldown() -> None:
    runtime = ShadowRuntime(ShadowRuntimeConfig(cooldown_seconds=0))
    runtime.process_snapshot(build_complete())
    runtime.process_bar(bar(1, high=101.0, low=97.0, close=98.0))
    duplicate = runtime.process_snapshot(
        bundle_for(components(), generated_offset=10)
    )
    assert duplicate.accepted is False
    assert duplicate.reasons == ("Duplicate shadow trade id suppressed.",)


def test_new_trade_is_allowed_after_cooldown_expires() -> None:
    runtime = ShadowRuntime(ShadowRuntimeConfig(cooldown_seconds=300))
    runtime.process_snapshot(build_complete())
    runtime.process_bar(bar(1, high=101.0, low=97.0, close=98.0))
    later = at_offset(components(), 400)
    result = runtime.process_snapshot(bundle_for(later, generated_offset=400))
    assert result.accepted is True
    assert result.trade is not None


def test_bar_without_active_trade_is_safe() -> None:
    update = ShadowRuntime().process_bar(bar(1))
    assert update.trade is None
    assert update.outcome is None
    assert update.events == ()


def test_runtime_events_and_returned_collections_are_immutable() -> None:
    runtime = ShadowRuntime()
    result = runtime.process_snapshot(build_complete())
    event = result.events[0]
    with pytest.raises(FrozenInstanceError):
        event.message = "changed"  # type: ignore[misc]
    assert isinstance(runtime.journal(), tuple)
    assert isinstance(runtime.active_trades(), tuple)


def test_snapshots_must_be_chronological() -> None:
    runtime = ShadowRuntime()
    runtime.process_snapshot(bundle_for(components(), generated_offset=10))
    with pytest.raises(ValueError, match="chronological"):
        runtime.process_snapshot(build_complete())


def test_runtime_config_is_validated() -> None:
    with pytest.raises(ValueError, match="concurrent"):
        ShadowRuntimeConfig(max_concurrent_trades=0)
    with pytest.raises(ValueError, match="cooldown"):
        ShadowRuntimeConfig(cooldown_seconds=-1)
