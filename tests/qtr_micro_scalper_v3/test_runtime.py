from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookState
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowAccumulator
from market_signal_assistant.qtr_micro_scalper_v3.engine import CashScalperEngine
from market_signal_assistant.qtr_micro_scalper_v3.models import V3PriceObservation
from market_signal_assistant.qtr_micro_scalper_v3.runtime import (
    V3FeatureBuilder,
    V3ShadowRuntime,
)
from market_signal_assistant.qtr_micro_scalper_v3.telemetry import JsonlTelemetryJournal
from qtr_micro_scalper_v3.helpers import NOW, snapshot


def test_runtime_records_entry_trade_and_independent_forward_outcomes(
    tmp_path: Path,
) -> None:
    runtime = V3ShadowRuntime(
        engine=CashScalperEngine(),
        entry_journal=JsonlTelemetryJournal(tmp_path / "entries.jsonl"),
        trade_journal=JsonlTelemetryJournal(tmp_path / "trades.jsonl"),
        outcome_journal=JsonlTelemetryJournal(tmp_path / "outcomes.jsonl"),
    )
    result = runtime.process_snapshot(snapshot())
    assert result.entry_created is True
    runtime.process_price("BTCUSDT", NOW + timedelta(seconds=10), 100.50)
    assert runtime.active_trade("BTCUSDT") is None

    runtime.process_price("BTCUSDT", NOW + timedelta(seconds=610), 100.40)
    assert len((tmp_path / "outcomes.jsonl").read_text().splitlines()) == 5
    assert len((tmp_path / "trades.jsonl").read_text().splitlines()) == 2
    entry_payload = (tmp_path / "entries.jsonl").read_text()
    assert '"estimated_round_trip_cost_bps":15.0' in entry_payload


def test_runtime_does_not_duplicate_active_symbol_or_same_episode(
    tmp_path: Path,
) -> None:
    runtime = V3ShadowRuntime(
        engine=CashScalperEngine(),
        entry_journal=JsonlTelemetryJournal(tmp_path / "entries.jsonl"),
        trade_journal=JsonlTelemetryJournal(tmp_path / "trades.jsonl"),
        outcome_journal=JsonlTelemetryJournal(tmp_path / "outcomes.jsonl"),
    )
    assert runtime.process_snapshot(snapshot()).entry_created is True
    repeated = runtime.process_snapshot(replace(snapshot(), observed_at=NOW))
    assert repeated.entry_created is False


def test_engine_uses_bounded_episode_state() -> None:
    engine = CashScalperEngine(max_remembered_symbols=2)
    for index, symbol in enumerate(("AAAUSDT", "BBBUSDT", "CCCUSDT")):
        snap = replace(
            snapshot(),
            symbol=symbol,
            impulse_id=f"{symbol}-episode",
            observed_at=NOW + timedelta(minutes=index),
            source_at=NOW + timedelta(minutes=index),
            impulse_started_at=NOW + timedelta(minutes=index, seconds=-5),
        )
        trade = engine.open_shadow_trade(engine.evaluate(snap))
        closed = engine.manage(
            trade,
            V3PriceObservation(symbol, snap.observed_at + timedelta(seconds=2), 100.50),
        )
        engine.remember_terminal(closed.trade)
    assert engine.remembered_symbol_count == 2


def test_feature_builder_reuses_public_flow_and_book_without_v2_score() -> None:
    accumulator = TradeFlowAccumulator(clock=lambda: NOW + timedelta(seconds=1))
    book = OrderBookState("BTCUSDT", require_contiguous_update_ids=False)
    book.process(
        OrderBookEvent(
            symbol="BTCUSDT",
            event_type=OrderBookEventType.SNAPSHOT,
            exchange_at=NOW,
            received_at=NOW,
            update_id=1,
            bids=(OrderBookLevel(99.99, 1_100.0),),
            asks=(OrderBookLevel(100.01, 1_100.0),),
        )
    )
    builder = V3FeatureBuilder(accumulator, lambda _symbol: book)
    first = PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id="1",
        exchange_at=NOW,
        received_at=NOW,
        side=TradeSide.BUY,
        price=100.0,
        quantity=100.0,
        quote_notional=10_000.0,
    )
    second = PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id="2",
        exchange_at=NOW + timedelta(seconds=1),
        received_at=NOW + timedelta(seconds=1),
        side=TradeSide.BUY,
        price=100.4,
        quantity=100.0,
        quote_notional=10_040.0,
    )
    accumulator.ingest(first)
    assert builder.observe_trade(first) is None
    accumulator.ingest(second)
    result = builder.observe_trade(second)

    assert result is not None
    assert result.direction.value == "LONG"
    assert result.price_displacement_5s_bps == pytest.approx(40.0)
    assert result.estimated_potential_bps == pytest.approx(40.0, rel=0.01)
    assert result.trigger_progress_atr is None
    assert not hasattr(result, "total_score")
