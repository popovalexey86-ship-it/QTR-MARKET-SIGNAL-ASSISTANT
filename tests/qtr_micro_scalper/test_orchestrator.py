from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_inplay_bridge import target
from test_shadow_runtime import at_offset, bar, for_symbol
from test_snapshot import NOW, Components, components

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    PressureDirection,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import (
    ShadowAnalysisInput,
    ShadowOrchestrator,
    ShadowOrchestratorEventType,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
)


def analysis_input(
    values: Components | None = None,
    *,
    generated_offset: int = 0,
    symbol: str | None = None,
) -> ShadowAnalysisInput:
    selected = values or components()
    return ShadowAnalysisInput(
        symbol=symbol or selected.trade_flow.symbol,
        generated_at=NOW + timedelta(seconds=generated_offset),
        trade_flow=selected.trade_flow,
        orderbook=selected.orderbook,
        liquidity=selected.liquidity,
        market_state=selected.market_state,
        setup_context=selected.setup_context,
    )


def orchestrator(tmp_path: Path) -> ShadowOrchestrator:
    return ShadowOrchestrator(
        journal=ShadowTradeJournal(tmp_path / "shadow-orchestrator.jsonl")
    )


def activate(
    coordinator: ShadowOrchestrator,
    symbol: str = "BTCUSDT",
    *,
    at_offset_seconds: int = 0,
) -> None:
    discovered_at = NOW + timedelta(seconds=at_offset_seconds)
    discovered = coordinator.discover_target(
        target(symbol, discovered_at=discovered_at),
        observed_at=discovered_at,
    )
    assert discovered.accepted is True
    activated = coordinator.activate_target(symbol, activated_at=discovered_at)
    assert activated.accepted is True


def test_target_discovery_and_activation_emit_ordered_events(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    found = coordinator.discover_target(target(), observed_at=NOW)
    started = coordinator.activate_target("BTCUSDT", activated_at=NOW)
    assert found.events[0].event_type is ShadowOrchestratorEventType.TARGET_FOUND
    assert found.events[0].message.startswith("🔥")
    assert started.events[0].event_type is (
        ShadowOrchestratorEventType.ANALYSIS_STARTED
    )
    assert started.events[0].message.startswith("👁")
    assert [event.sequence for event in coordinator.events()] == [1, 2]


def test_full_shadow_pipeline_and_lifecycle(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    analyzed = coordinator.analyze(analysis_input())
    assert analyzed.successful is True
    assert analyzed.score is not None
    assert analyzed.trade is not None
    assert analyzed.trade.stage is ShadowTradeStage.WAITING_ENTRY
    assert [event.event_type for event in analyzed.events] == [
        ShadowOrchestratorEventType.SCORE_READY,
        ShadowOrchestratorEventType.ENTRY_CREATED,
    ]

    opened = coordinator.process_bars((bar(1),))[0]
    assert opened.trade is not None
    assert opened.trade.stage is ShadowTradeStage.OPEN
    assert opened.events[0].event_type is (
        ShadowOrchestratorEventType.POSITION_UPDATED
    )

    tp1 = coordinator.process_bars(
        (bar(3, high=102.3, low=100.0, close=102.0),)
    )[0]
    assert tp1.trade is not None
    assert tp1.trade.stage is ShadowTradeStage.TP1_HIT
    assert tp1.events[0].event_type is (
        ShadowOrchestratorEventType.POSITION_UPDATED
    )

    closed = coordinator.process_bars(
        (bar(5, open_price=102.0, high=104.5, low=101.0, close=104.0),)
    )[0]
    assert closed.trade is not None
    assert closed.trade.stage is ShadowTradeStage.CLOSED
    assert closed.events[0].event_type is ShadowOrchestratorEventType.TRADE_FINISHED
    assert closed.events[0].message.startswith("🏁")


def test_score_and_entry_events_have_required_messages(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    result = coordinator.analyze(analysis_input())
    assert result.events[0].message.startswith("🎯")
    assert result.events[1].message.startswith("⚔️")


def test_runtime_lifecycle_is_persisted_in_shadow_journal(tmp_path: Path) -> None:
    path = tmp_path / "shadow-orchestrator.jsonl"
    coordinator = ShadowOrchestrator(journal=ShadowTradeJournal(path))
    activate(coordinator)
    coordinator.analyze(analysis_input())
    coordinator.process_bars((bar(1),))
    coordinator.process_bars((bar(3, high=102.3, low=100.0, close=102.0),))
    coordinator.process_bars(
        (bar(5, open_price=102.0, high=104.5, low=101.0, close=104.0),)
    )
    recovered = ShadowTradeJournal(path).records()
    assert [record.stage for record in recovered] == [
        ShadowTradeStage.WAITING_ENTRY,
        ShadowTradeStage.OPEN,
        ShadowTradeStage.TP1_HIT,
        ShadowTradeStage.CLOSED,
    ]


def test_duplicate_target_does_not_emit_second_target_found(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    first = coordinator.discover_target(target(), observed_at=NOW)
    duplicate = coordinator.discover_target(target(), observed_at=NOW)
    assert first.events
    assert duplicate.accepted is True
    assert duplicate.events == ()
    assert coordinator.metrics().duplicates_suppressed == 1


def test_duplicate_snapshot_does_not_create_second_trade(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    first = coordinator.analyze(analysis_input())
    duplicate = coordinator.analyze(analysis_input())
    assert first.trade is not None
    assert duplicate.trade is None
    assert duplicate.events == ()
    assert coordinator.metrics().shadow_trades_created == 1
    assert coordinator.metrics().active_shadow_trades == 1


def test_blocked_score_is_reported_without_trade(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    values = components()
    opposite = replace(
        values.liquidity,
        pressure=replace(
            values.liquidity.pressure,
            combined_pressure=-0.8,
            direction=PressureDirection.SELL,
        ),
    )
    result = coordinator.analyze(
        analysis_input(replace(values, liquidity=opposite))
    )
    assert result.successful is True
    assert result.score is not None
    assert result.score.decision.value == "BLOCKED"
    assert result.trade is None
    assert [event.event_type for event in result.events] == [
        ShadowOrchestratorEventType.SCORE_READY
    ]


def test_one_symbol_error_does_not_stop_batch(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator, "BTCUSDT")
    activate(coordinator, "ETHUSDT")
    btc = components()
    eth = for_symbol(components(), "ETHUSDT")
    broken_eth = replace(
        eth,
        trade_flow=replace(eth.trade_flow, symbol="WRONGUSDT"),
    )
    results = coordinator.analyze_many(
        (analysis_input(broken_eth, symbol="ETHUSDT"), analysis_input(btc))
    )
    assert [result.symbol for result in results] == ["BTCUSDT", "ETHUSDT"]
    assert results[0].successful is True
    assert results[0].trade is not None
    assert results[1].successful is False
    assert results[1].trade is None
    assert coordinator.metrics().errors == 1


def test_symbol_isolation_keeps_other_trade_active(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    coordinator.analyze(analysis_input())
    foreign = coordinator.process_bars((bar(1, symbol="ETHUSDT"),))[0]
    assert foreign.successful is True
    assert foreign.trade is None
    assert coordinator.metrics().active_shadow_trades == 1


def test_analysis_requires_active_target(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    result = coordinator.analyze(analysis_input())
    assert result.successful is False
    assert result.error == "Target is not ACTIVE in the InPlay bridge."
    assert result.score is None
    assert coordinator.metrics().snapshots_received == 0


def test_analysis_batch_order_is_deterministic(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator, "BTCUSDT")
    activate(coordinator, "ETHUSDT")
    btc = components()
    eth = for_symbol(at_offset(components(), 1), "ETHUSDT")
    results = coordinator.analyze_many(
        (analysis_input(eth, generated_offset=1), analysis_input(btc))
    )
    assert [result.symbol for result in results] == ["BTCUSDT", "ETHUSDT"]
    sequences = [event.sequence for event in coordinator.events()]
    assert sequences == list(range(1, len(sequences) + 1))


def test_bar_batch_is_sorted_by_time_then_symbol(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator, "BTCUSDT")
    activate(coordinator, "ETHUSDT")
    coordinator.analyze_many(
        (
            analysis_input(components()),
            analysis_input(for_symbol(components(), "ETHUSDT")),
        )
    )
    results = coordinator.process_bars(
        (
            bar(3, symbol="BTCUSDT"),
            bar(1, symbol="ETHUSDT"),
            bar(1, symbol="BTCUSDT"),
        )
    )
    assert [result.symbol for result in results] == [
        "BTCUSDT",
        "ETHUSDT",
        "BTCUSDT",
    ]


def test_runtime_metrics_cover_pipeline_state(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    coordinator.analyze(analysis_input())
    coordinator.process_bars((bar(1),))
    coordinator.process_bars((bar(3, high=105.0, low=100.0, close=104.0),))
    metrics = coordinator.metrics()
    assert metrics.targets_discovered == 1
    assert metrics.active_targets == 1
    assert metrics.snapshots_received == 1
    assert metrics.scores_created == 1
    assert metrics.shadow_trades_created == 1
    assert metrics.trade_updates == 2
    assert metrics.trades_closed == 1
    assert metrics.active_shadow_trades == 0
    assert metrics.journal_records == 3
    assert metrics.events_emitted == len(coordinator.events())


def test_events_and_metrics_are_immutable(tmp_path: Path) -> None:
    coordinator = orchestrator(tmp_path)
    activate(coordinator)
    event = coordinator.events()[0]
    metrics = coordinator.metrics()
    with pytest.raises(FrozenInstanceError):
        event.sequence = 99  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        metrics.errors = 99  # type: ignore[misc]


def test_two_fresh_orchestrators_emit_identical_events(tmp_path: Path) -> None:
    first = ShadowOrchestrator(
        journal=ShadowTradeJournal(tmp_path / "first.jsonl")
    )
    second = ShadowOrchestrator(
        journal=ShadowTradeJournal(tmp_path / "second.jsonl")
    )
    for coordinator in (first, second):
        activate(coordinator)
        coordinator.analyze(analysis_input())
        coordinator.process_bars((bar(1),))
    assert first.events() == second.events()
