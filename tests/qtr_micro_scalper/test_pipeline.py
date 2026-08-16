from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.inplay_bridge import ScalperTarget
from market_signal_assistant.qtr_micro_scalper.live.collector import AsyncWebSocket
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowOrchestrator
from market_signal_assistant.qtr_micro_scalper.pipeline import (
    LiveShadowPipeline,
    LiveShadowPipelineEvent,
    PipelineEventType,
    PipelineProcessResult,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    ShadowDirection,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
)
from market_signal_assistant.qtr_micro_scalper.snapshot import SnapshotReadiness

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def price_context(
    symbol: str, assessed_at: datetime, market_price: float
) -> PriceContext:
    return PriceContext(
        symbol=symbol,
        assessed_at=assessed_at,
        direction=ShadowDirection.LONG,
        market_price=market_price,
        atr=1.0,
        trigger_price=market_price,
        invalidation_price=market_price - 1.0,
        local_range_low=market_price - 2.0,
        local_range_high=market_price + 2.0,
        confirmations=("Bullish price structure.",),
    )


def target(symbol: str = "BTCUSDT") -> ScalperTarget:
    return ScalperTarget(
        symbol=symbol,
        discovered_at=NOW,
        source="offline-test",
        reason="Active scanner target.",
        priority=90.0,
        volatility_score=80.0,
        volume_score=90.0,
        liquidity_score=90.0,
    )


def pipeline(tmp_path: Path, *symbols: str) -> LiveShadowPipeline:
    journal = ShadowTradeJournal(tmp_path / "pipeline-shadow.jsonl")
    coordinator = ShadowOrchestrator(journal=journal)
    result = LiveShadowPipeline(
        symbols=symbols or ("BTCUSDT",),
        price_context_provider=price_context,
        orchestrator=coordinator,
        clock=lambda: NOW + timedelta(seconds=5),
    )
    for symbol in symbols or ("BTCUSDT",):
        assert result.register_target(target(symbol), observed_at=NOW)
    return result


def trade(
    symbol: str = "BTCUSDT",
    *,
    trade_id: str = "trade-1",
    at: datetime = NOW + timedelta(milliseconds=100),
) -> PublicTradeEvent:
    return PublicTradeEvent(
        symbol=symbol,
        trade_id=trade_id,
        exchange_at=at,
        received_at=at,
        side=TradeSide.BUY,
        price=100.0,
        quantity=100.0,
        quote_notional=10_000.0,
    )


def book(
    event_type: OrderBookEventType,
    update_id: int,
    *,
    symbol: str = "BTCUSDT",
    at: datetime = NOW,
) -> OrderBookEvent:
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    if event_type is OrderBookEventType.SNAPSHOT:
        bids = (
            OrderBookLevel(99.99, 100.0),
            OrderBookLevel(99.98, 80.0),
            OrderBookLevel(99.97, 50.0),
        )
        asks = (
            OrderBookLevel(100.01, 20.0),
            OrderBookLevel(100.02, 20.0),
            OrderBookLevel(100.03, 20.0),
        )
    else:
        bids = (OrderBookLevel(99.99, 120.0),)
        asks = (OrderBookLevel(100.01, 0.0),)
    return OrderBookEvent(
        symbol=symbol,
        event_type=event_type,
        exchange_at=at,
        received_at=at,
        update_id=update_id,
        bids=bids,
        asks=asks,
        cross_sequence=update_id,
    )


async def ready_result(
    service: LiveShadowPipeline,
    *,
    symbol: str = "BTCUSDT",
) -> PipelineProcessResult:
    first = await service.process_event(
        book(OrderBookEventType.SNAPSHOT, 1, symbol=symbol)
    )
    flow = await service.process_event(trade(symbol))
    assert first.snapshot is None
    assert flow.snapshot is None
    return await service.process_event(
        book(
            OrderBookEventType.DELTA,
            2,
            symbol=symbol,
            at=NOW + timedelta(milliseconds=200),
        )
    )


def test_ready_market_data_reaches_shadow_journal(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        result = await ready_result(service)
        assert result.snapshot is not None
        assert result.snapshot.readiness is SnapshotReadiness.READY
        assert result.score is not None
        assert result.trade is not None
        assert [event.event_type for event in result.events] == [
            PipelineEventType.MARKET_DATA_RECEIVED,
            PipelineEventType.SNAPSHOT_READY,
            PipelineEventType.SCORE_CREATED,
            PipelineEventType.SHADOW_DECISION,
            PipelineEventType.JOURNAL_UPDATED,
        ]
        assert service.metrics().journal_updates == 1
        assert (tmp_path / "pipeline-shadow.jsonl").exists()

    asyncio.run(scenario())


def test_snapshot_is_created_only_after_book_and_flow_are_ready(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        first = await service.process_event(book(OrderBookEventType.SNAPSHOT, 1))
        second = await service.process_event(trade())
        assert first.snapshot is second.snapshot is None
        assert service.metrics().snapshots_ready == 0

    asyncio.run(scenario())


def test_duplicate_market_event_is_suppressed(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        event = trade()
        first = await service.process_event(event)
        duplicate = await service.process_event(event)
        assert first.accepted
        assert not duplicate.accepted
        assert duplicate.events == ()
        assert service.metrics().duplicate_events_suppressed == 1

    asyncio.run(scenario())


def test_stale_trade_flow_prevents_snapshot(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        await service.process_event(book(OrderBookEventType.SNAPSHOT, 1))
        await service.process_event(trade())
        result = await service.process_event(
            book(
                OrderBookEventType.DELTA,
                2,
                at=NOW + timedelta(seconds=2),
            )
        )
        assert result.snapshot is None
        assert service.metrics().stale_data_suppressed == 1

    asyncio.run(scenario())


def test_symbol_error_is_isolated_from_subscribed_symbol(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        foreign = await service.process_event(trade("ETHUSDT"))
        valid = await service.process_event(trade())
        assert foreign.error == "unsupported_symbol"
        assert valid.accepted
        assert service.metrics().market_events_received == 1
        assert service.metrics().errors == 1

    asyncio.run(scenario())


def test_configured_symbol_failure_does_not_stop_other_symbol(tmp_path: Path) -> None:
    def provider(
        symbol: str,
        assessed_at: datetime,
        market_price: float,
    ) -> PriceContext:
        if symbol == "ETHUSDT":
            raise ValueError("isolated ETH context failure")
        return price_context(symbol, assessed_at, market_price)

    async def scenario() -> None:
        coordinator = ShadowOrchestrator(
            journal=ShadowTradeJournal(tmp_path / "symbol-isolation.jsonl")
        )
        service = LiveShadowPipeline(
            symbols=("BTCUSDT", "ETHUSDT"),
            price_context_provider=provider,
            orchestrator=coordinator,
            clock=lambda: NOW + timedelta(seconds=5),
        )
        assert service.register_target(target("BTCUSDT"), observed_at=NOW)
        assert service.register_target(target("ETHUSDT"), observed_at=NOW)
        eth = await ready_result(service, symbol="ETHUSDT")
        btc = await ready_result(service, symbol="BTCUSDT")
        assert eth.error == "isolated ETH context failure"
        assert btc.error is None
        assert btc.snapshot is not None
        assert service.metrics().errors == 1

    asyncio.run(scenario())


class MockMarketCollector:
    def __init__(self) -> None:
        self.starts = 0
        self.stops = 0

    async def start(self) -> None:
        self.starts += 1

    async def stop(self) -> None:
        self.stops += 1


def test_async_market_collector_lifecycle_is_lazy_and_idempotent(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        mock = MockMarketCollector()
        service = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=price_context,
            market_collector=mock,
            orchestrator=ShadowOrchestrator(
                journal=ShadowTradeJournal(tmp_path / "lifecycle.jsonl")
            ),
            clock=lambda: NOW,
        )
        assert mock.starts == 0
        await service.start()
        await service.start()
        await service.stop()
        assert mock.starts == 1
        assert mock.stops == 1

    asyncio.run(scenario())


def test_queue_processes_mock_stream_events(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        await service.start()
        service.enqueue_event(book(OrderBookEventType.SNAPSHOT, 1))
        service.enqueue_event(trade())
        service.enqueue_event(
            book(
                OrderBookEventType.DELTA,
                2,
                at=NOW + timedelta(milliseconds=200),
            )
        )
        await service.stop()
        assert service.metrics().market_events_received == 3
        assert service.metrics().snapshots_ready == 1

    asyncio.run(scenario())


def test_pipeline_events_and_metrics_are_immutable(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        await service.process_event(trade())
        event = service.events()[0]
        with pytest.raises(FrozenInstanceError):
            event.sequence = 999  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            service.metrics().errors = 999  # type: ignore[misc]

    asyncio.run(scenario())


def test_pipeline_event_model_contains_requested_semantics() -> None:
    event = LiveShadowPipelineEvent(
        event_id="event-1",
        sequence=1,
        event_type=PipelineEventType.MARKET_DATA_RECEIVED,
        occurred_at=NOW,
        symbol="BTCUSDT",
        message="📡 MARKET_DATA_RECEIVED",
    )
    assert event.event_type.value == "MARKET_DATA_RECEIVED"


def test_live_composition_is_lazy_and_does_not_open_websocket(tmp_path: Path) -> None:
    calls = 0

    async def factory(_url: str) -> AsyncWebSocket:
        nonlocal calls
        calls += 1
        raise AssertionError("WebSocket must remain lazy during composition")

    coordinator = ShadowOrchestrator(
        journal=ShadowTradeJournal(tmp_path / "lazy-live.jsonl")
    )
    LiveShadowPipeline.with_live_collectors(
        symbols=("BTCUSDT",),
        price_context_provider=price_context,
        websocket_factory=factory,
        orchestrator=coordinator,
        clock=lambda: NOW,
    )
    assert calls == 0
