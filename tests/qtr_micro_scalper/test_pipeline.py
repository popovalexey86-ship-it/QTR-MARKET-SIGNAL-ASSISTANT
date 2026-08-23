from __future__ import annotations

import asyncio
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowAccumulator
from market_signal_assistant.qtr_micro_scalper.dynamic_targets import (
    DynamicTargetSettings,
    DynamicVerifiedTargetManager,
)
from market_signal_assistant.qtr_micro_scalper.holding_experiment import (
    HoldingExperimentConfig,
    HoldingExperimentJournal,
    HoldingExperimentRecordType,
    HoldingExperimentRuntime,
    HoldingVariant,
    iter_holding_experiment_records,
)
from market_signal_assistant.qtr_micro_scalper.inplay_bridge import ScalperTarget
from market_signal_assistant.qtr_micro_scalper.live.collector import (
    AsyncWebSocket,
    UnifiedMarketDataCollector,
    UnifiedSubscriptionMetrics,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowOrchestrator
from market_signal_assistant.qtr_micro_scalper.pipeline import (
    LiveShadowPipeline,
    LiveShadowPipelineConfig,
    LiveShadowPipelineEvent,
    PipelineEventType,
    PipelineProcessResult,
)
from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
    JsonlVerifiedSetupProvider,
    VerifiedPriceContextAdapter,
    VerifiedSetupRecord,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    ShadowDirection,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEventType,
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
    price: float = 100.0,
) -> PublicTradeEvent:
    return PublicTradeEvent(
        symbol=symbol,
        trade_id=trade_id,
        exchange_at=at,
        received_at=at,
        side=TradeSide.BUY,
        price=price,
        quantity=100.0,
        quote_notional=price * 100.0,
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


def test_ready_baseline_entry_creates_parallel_holding_group(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        experiment_journal = HoldingExperimentJournal(
            tmp_path / "pipeline-holding.jsonl"
        )
        experiment = HoldingExperimentRuntime(
            experiment_journal,
            HoldingExperimentConfig(enabled=True),
        )
        baseline_journal = ShadowTradeJournal(tmp_path / "pipeline-baseline.jsonl")
        service = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=price_context,
            orchestrator=ShadowOrchestrator(journal=baseline_journal),
            holding_experiment=experiment,
            clock=lambda: NOW + timedelta(seconds=5),
        )
        assert service.register_target(target(), observed_at=NOW)

        result = await ready_result(service)

        assert result.trade is not None
        assert experiment.metrics().active_groups == 1
        assert experiment.metrics().active_variants == 4
        created = [
            record
            for record in iter_holding_experiment_records(
                experiment_journal.path
            )
            if record.record_type is HoldingExperimentRecordType.CREATED
        ]
        assert [record.variant for record in created] == list(HoldingVariant)
        assert len(baseline_journal.records()) == 1
        assert baseline_journal.records()[0].trade_id == result.trade.trade_id

    asyncio.run(scenario())


def test_holding_experiment_does_not_change_baseline_trade_result(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        control_journal = ShadowTradeJournal(tmp_path / "control.jsonl")
        control = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=price_context,
            orchestrator=ShadowOrchestrator(journal=control_journal),
            clock=lambda: NOW + timedelta(seconds=5),
        )
        assert control.register_target(target(), observed_at=NOW)
        experiment_journal = HoldingExperimentJournal(tmp_path / "experiment.jsonl")
        experiment = HoldingExperimentRuntime(
            experiment_journal,
            HoldingExperimentConfig(enabled=True),
        )
        observed_journal = ShadowTradeJournal(tmp_path / "observed.jsonl")
        observed = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=price_context,
            orchestrator=ShadowOrchestrator(journal=observed_journal),
            holding_experiment=experiment,
            clock=lambda: NOW + timedelta(seconds=5),
        )
        assert observed.register_target(target(), observed_at=NOW)

        control_created = await ready_result(control)
        observed_created = await ready_result(observed)
        assert control_created.trade == observed_created.trade
        assert control_created.trade is not None
        for service in (control, observed):
            await service.process_event(
                trade(
                    trade_id="same-entry",
                    at=NOW + timedelta(milliseconds=300),
                    price=control_created.trade.entry_price,
                )
            )
            await service.process_event(
                book(
                    OrderBookEventType.SNAPSHOT,
                    3,
                    at=NOW + timedelta(seconds=1, milliseconds=300),
                )
            )

        assert control_journal.records() == observed_journal.records()

    asyncio.run(scenario())


def test_pipeline_retains_only_bounded_recent_runtime_events(tmp_path: Path) -> None:
    async def scenario() -> None:
        coordinator = ShadowOrchestrator(
            journal=ShadowTradeJournal(tmp_path / "bounded-events.jsonl")
        )
        service = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=price_context,
            orchestrator=coordinator,
            config=LiveShadowPipelineConfig(event_retention_capacity=5),
            clock=lambda: NOW + timedelta(seconds=5),
        )
        assert service.register_target(target(), observed_at=NOW)

        for value in range(20):
            await service.process_event(
                trade(
                    trade_id=f"bounded-{value}",
                    at=NOW + timedelta(milliseconds=value),
                )
            )

        assert len(service.events()) == 5
        assert service.metrics().retained_events == 5
        assert service.metrics().market_events_received == 20

    asyncio.run(scenario())


def test_pipeline_worker_exception_is_exposed_and_stop_does_not_hang(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)

        async def fail_processing(
            event: PublicTradeEvent,
            *,
            already_applied: bool = False,
        ) -> PipelineProcessResult:
            del event, already_applied
            raise MemoryError("synthetic critical worker failure")

        monkeypatch.setattr(service, "process_event", fail_processing)
        await service.start()
        service.enqueue_event(trade())
        for _ in range(100):
            if service.background_error() is not None:
                break
            await asyncio.sleep(0)

        assert "synthetic critical worker failure" in (
            service.background_error() or ""
        )
        await asyncio.wait_for(service.stop(), timeout=1.0)

    asyncio.run(scenario())


def test_persistence_oserror_is_exposed_as_critical_background_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        coordinator = ShadowOrchestrator(
            journal=ShadowTradeJournal(tmp_path / "persistence-error.jsonl")
        )

        def fail_persistence(analysis: object) -> object:
            del analysis
            raise OSError("synthetic disk full")

        monkeypatch.setattr(coordinator, "analyze", fail_persistence)
        service = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=price_context,
            orchestrator=coordinator,
            clock=lambda: NOW + timedelta(seconds=5),
        )
        assert service.register_target(target(), observed_at=NOW)

        result = await ready_result(service)

        assert not result.accepted
        assert "synthetic disk full" in (result.error or "")
        assert "Critical shadow persistence failure" in (
            service.background_error() or ""
        )

    asyncio.run(scenario())


def test_live_trade_bar_opens_trade_and_persists_journal(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        created = await ready_result(service)
        assert created.trade is not None
        assert created.trade.stage is ShadowTradeStage.WAITING_ENTRY

        await service.process_event(
            trade(
                trade_id="lifecycle-entry",
                at=NOW + timedelta(milliseconds=300),
                price=created.trade.entry_price,
            )
        )
        opened = await service.process_event(
            book(
                OrderBookEventType.SNAPSHOT,
                3,
                at=NOW + timedelta(seconds=1, milliseconds=300),
            )
        )

        assert opened.trade is not None
        assert opened.trade.stage is ShadowTradeStage.OPEN
        records = ShadowTradeJournal(tmp_path / "pipeline-shadow.jsonl").records()
        assert [record.stage for record in records] == [
            ShadowTradeStage.WAITING_ENTRY,
            ShadowTradeStage.OPEN,
        ]
        assert records[-1].events[-1].event_type is ShadowRuntimeEventType.OPEN

    asyncio.run(scenario())


def test_active_trade_lifecycle_survives_unavailable_new_setup(tmp_path: Path) -> None:
    async def scenario() -> None:
        available = True

        def changing_context(
            symbol: str,
            assessed_at: datetime,
            market_price: float,
        ) -> PriceContext | None:
            if not available:
                return None
            return price_context(symbol, assessed_at, market_price)

        journal = ShadowTradeJournal(tmp_path / "conflicted-shadow.jsonl")
        coordinator = ShadowOrchestrator(journal=journal)
        service = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=changing_context,
            orchestrator=coordinator,
            clock=lambda: NOW,
        )
        assert service.register_target(target(), observed_at=NOW)
        created = await ready_result(service)
        assert created.trade is not None
        available = False

        await service.process_event(
            trade(
                trade_id="conflicted-entry",
                at=NOW + timedelta(milliseconds=300),
                price=created.trade.entry_price,
            )
        )
        opened = await service.process_event(
            book(
                OrderBookEventType.SNAPSHOT,
                3,
                at=NOW + timedelta(seconds=1, milliseconds=300),
            )
        )

        assert opened.snapshot is None
        assert opened.trade is not None
        assert opened.trade.trade_id == created.trade.trade_id
        assert opened.trade.stage is ShadowTradeStage.OPEN
        assert len({record.trade_id for record in journal.records()}) == 1

    asyncio.run(scenario())


def test_live_waiting_entry_expires_without_synthetic_fill(tmp_path: Path) -> None:
    async def scenario() -> None:
        service = pipeline(tmp_path)
        created = await ready_result(service)
        assert created.trade is not None

        await service.process_event(
            trade(
                trade_id="post-deadline",
                at=NOW + timedelta(seconds=60, milliseconds=300),
                price=99.0,
            )
        )
        expired = await service.process_event(
            book(
                OrderBookEventType.SNAPSHOT,
                3,
                at=NOW + timedelta(seconds=61, milliseconds=300),
            )
        )

        assert expired.trade is not None
        assert expired.trade.stage is ShadowTradeStage.EXPIRED
        assert expired.trade.entry_at is None
        final_record = ShadowTradeJournal(tmp_path / "pipeline-shadow.jsonl").records()[
            -1
        ]
        assert final_record.outcome.value == "NOT_TRIGGERED"
        assert final_record.events[-1].event_type is ShadowRuntimeEventType.EXPIRED

    asyncio.run(scenario())


def test_external_production_record_can_create_ready_snapshot_read_only(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "scanner" / "qtr_setup_audit.jsonl"
    audit_path.parent.mkdir()
    payload = {
        "symbol": "BTCUSDT",
        "price_context": {
            "observed_at": NOW.isoformat(),
            "source_direction": "UP",
            "setup_direction": "UP",
            "market_price": 100.0,
            "atr": 1.0,
            "trigger_price": 100.0,
            "invalidation_price": 99.0,
            "local_range_low": 98.0,
            "local_range_high": 100.0,
            "setup_state": "CONFIRMING",
            "setup_confidence": 90.0,
            "volume_confirmation": True,
            "volatility_confirmation": True,
            "liquidity_ok": True,
            "confirmations": ["Verified breakout."],
            "warnings": [],
        },
    }
    audit_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    original = audit_path.read_bytes()
    provider = JsonlVerifiedSetupProvider(audit_path)
    adapter = VerifiedPriceContextAdapter(provider)
    service = LiveShadowPipeline(
        symbols=("BTCUSDT",),
        price_context_provider=adapter,
        target_provider=adapter.target,
        orchestrator=ShadowOrchestrator(
            journal=ShadowTradeJournal(tmp_path / "shadow" / "journal.jsonl")
        ),
        clock=lambda: NOW,
    )

    result = asyncio.run(ready_result(service))

    assert result.snapshot is not None
    assert result.snapshot.readiness is SnapshotReadiness.READY
    assert audit_path.read_bytes() == original
    assert provider.metrics.bootstrap_scans == 1
    assert provider.metrics.incremental_reads == 0


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


class MutableVerifiedProvider:
    def __init__(self, records: tuple[VerifiedSetupRecord, ...] = ()) -> None:
        self.records = records
        self.calls = 0

    def latest_records(self) -> tuple[VerifiedSetupRecord, ...]:
        self.calls += 1
        return self.records


class FakeSubscriptionController:
    def __init__(self) -> None:
        self.symbols: tuple[str, ...] = ()
        self.calls: list[tuple[str, ...]] = []
        self.subscribes = 0
        self.unsubscribes = 0

    async def update_symbols(self, symbols: tuple[str, ...]) -> None:
        normalized = tuple(sorted(symbols))
        if normalized == self.symbols:
            return
        previous = set(self.symbols)
        current = set(normalized)
        self.subscribes += int(bool(current - previous))
        self.unsubscribes += int(bool(previous - current))
        self.symbols = normalized
        self.calls.append(normalized)

    @property
    def subscription_metrics(self) -> UnifiedSubscriptionMetrics:
        return UnifiedSubscriptionMetrics(
            active_topics=len(self.symbols) * 3,
            subscribe_operations=self.subscribes,
            unsubscribe_operations=self.unsubscribes,
            subscription_errors=0,
        )


def verified_record(
    symbol: str,
    *,
    state: str = "READY_TO_CONSIDER",
    confidence: float = 90.0,
    observed_at: datetime = NOW,
) -> VerifiedSetupRecord:
    return VerifiedSetupRecord(
        symbol=symbol,
        observed_at=observed_at,
        source_direction="UP",
        setup_direction="UP",
        market_price=100.0,
        atr=1.0,
        trigger_price=100.0,
        invalidation_price=98.0,
        local_range_low=99.0,
        local_range_high=101.0,
        setup_state=state,
        setup_confidence=confidence,
        volume_confirmation=True,
        volatility_confirmation=True,
        liquidity_ok=True,
    )


def dynamic_pipeline(
    tmp_path: Path,
    provider: MutableVerifiedProvider,
    controller: FakeSubscriptionController,
    *,
    maximum: int = 5,
    trade_flow: TradeFlowAccumulator | None = None,
    config: LiveShadowPipelineConfig | None = None,
    holding_experiment: HoldingExperimentRuntime | None = None,
) -> LiveShadowPipeline:
    manager = DynamicVerifiedTargetManager(
        provider,
        DynamicTargetSettings(max_active_symbols=maximum),
    )
    return LiveShadowPipeline(
        symbols=(),
        price_context_provider=price_context,
        orchestrator=ShadowOrchestrator(
            journal=ShadowTradeJournal(tmp_path / "dynamic-shadow.jsonl")
        ),
        dynamic_target_manager=manager,
        subscription_controller=cast(UnifiedMarketDataCollector, controller),
        clock=lambda: NOW,
        trade_flow=trade_flow,
        config=config,
        holding_experiment=holding_experiment,
    )


def test_dynamic_pipeline_adds_processes_and_removes_symbol(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        accumulator = TradeFlowAccumulator(clock=lambda: NOW)
        provider = MutableVerifiedProvider((verified_record("BTCUSDT"),))
        controller = FakeSubscriptionController()
        service = dynamic_pipeline(
            tmp_path,
            provider,
            controller,
            trade_flow=accumulator,
        )

        first = await service.refresh_targets()
        assert first is not None
        assert service.active_symbols() == ("BTCUSDT",)
        assert (await service.process_event(trade())).accepted
        assert accumulator.event_count("BTCUSDT") == 1

        provider.records = ()
        removed = await service.refresh_targets()
        assert removed is not None and removed.removed == ("BTCUSDT",)
        assert service.active_symbols() == ()
        assert accumulator.event_count("BTCUSDT") == 0
        late = await service.process_event(trade(trade_id="late"))
        assert not late.accepted
        assert late.error is None

        provider.records = (verified_record("ETHUSDT"),)
        await service.refresh_targets()
        eth = await service.process_event(trade("ETHUSDT", trade_id="eth"))
        assert eth.accepted
        assert service.active_symbols() == ("ETHUSDT",)
        assert service.metrics().errors == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("producer_state", ("CANCELLED", "LATE"))
def test_waiting_entry_is_protected_from_producer_state_change(
    tmp_path: Path,
    producer_state: str,
) -> None:
    async def scenario() -> None:
        provider = MutableVerifiedProvider((verified_record("BTCUSDT"),))
        controller = FakeSubscriptionController()
        service = dynamic_pipeline(tmp_path, provider, controller)
        await service.refresh_targets()
        assert service.register_target(target(), observed_at=NOW)
        created = await ready_result(service)
        assert created.trade is not None
        assert created.trade.stage is ShadowTradeStage.WAITING_ENTRY

        provider.records = (verified_record("BTCUSDT", state=producer_state),)
        refreshed = await service.refresh_targets()
        assert refreshed is not None
        assert refreshed.desired_symbols == ()
        assert refreshed.protected_trade_symbols == ("BTCUSDT",)
        assert service.active_symbols() == ("BTCUSDT",)
        assert controller.unsubscribes == 0

    asyncio.run(scenario())


def test_open_trade_stays_subscribed_until_terminal_state(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = MutableVerifiedProvider((verified_record("BTCUSDT"),))
        controller = FakeSubscriptionController()
        service = dynamic_pipeline(tmp_path, provider, controller)
        await service.refresh_targets()
        assert service.register_target(target(), observed_at=NOW)
        created = await ready_result(service)
        assert created.trade is not None

        await service.process_event(
            trade(
                trade_id="dynamic-entry",
                at=NOW + timedelta(milliseconds=300),
                price=created.trade.entry_price,
            )
        )
        opened = await service.process_event(
            book(
                OrderBookEventType.SNAPSHOT,
                3,
                at=NOW + timedelta(seconds=1, milliseconds=300),
            )
        )
        assert opened.trade is not None
        assert opened.trade.stage is ShadowTradeStage.OPEN

        provider.records = ()
        protected = await service.refresh_targets()
        assert protected is not None
        assert protected.protected_trade_symbols == ("BTCUSDT",)
        assert service.active_symbols() == ("BTCUSDT",)

    asyncio.run(scenario())


def test_terminal_trade_allows_dynamic_unsubscribe(tmp_path: Path) -> None:
    async def scenario() -> None:
        provider = MutableVerifiedProvider((verified_record("BTCUSDT"),))
        controller = FakeSubscriptionController()
        service = dynamic_pipeline(tmp_path, provider, controller)
        await service.refresh_targets()
        assert service.register_target(target(), observed_at=NOW)
        created = await ready_result(service)
        assert created.trade is not None

        provider.records = ()
        await service.process_event(
            trade(
                trade_id="dynamic-expire",
                at=NOW + timedelta(seconds=60, milliseconds=300),
                price=99.0,
            )
        )
        expired = await service.process_event(
            book(
                OrderBookEventType.SNAPSHOT,
                3,
                at=NOW + timedelta(seconds=61, milliseconds=300),
            )
        )
        assert expired.trade is not None
        assert expired.trade.stage is ShadowTradeStage.EXPIRED

        refreshed = await service.refresh_targets()
        assert refreshed is not None
        assert refreshed.protected_trade_symbols == ()
        assert refreshed.removed == ("BTCUSDT",)
        assert service.active_symbols() == ()

    asyncio.run(scenario())


def test_experiment_protects_subscription_after_baseline_a30_terminal(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        provider = MutableVerifiedProvider((verified_record("BTCUSDT"),))
        controller = FakeSubscriptionController()
        experiment = HoldingExperimentRuntime(
            HoldingExperimentJournal(tmp_path / "dynamic-holding.jsonl"),
            HoldingExperimentConfig(enabled=True),
        )
        service = dynamic_pipeline(
            tmp_path,
            provider,
            controller,
            holding_experiment=experiment,
        )
        await service.refresh_targets()
        assert service.register_target(target(), observed_at=NOW)
        created = await ready_result(service)
        assert created.trade is not None

        for index in range(31):
            await service.process_event(
                trade(
                    trade_id=f"experiment-{index}",
                    at=NOW
                    + timedelta(seconds=index, milliseconds=300),
                    price=created.trade.entry_price,
                )
            )
        assert experiment.metrics().active_variants == 3
        provider.records = ()
        protected = await service.refresh_targets()
        assert protected is not None
        assert protected.desired_symbols == ()
        assert protected.protected_trade_symbols == ("BTCUSDT",)
        assert service.active_symbols() == ("BTCUSDT",)

        for index in range(31, 301):
            await service.process_event(
                trade(
                    trade_id=f"experiment-{index}",
                    at=NOW
                    + timedelta(seconds=index, milliseconds=300),
                    price=created.trade.entry_price,
                )
            )
        assert experiment.metrics().active_groups == 0
        released = await service.refresh_targets()
        assert released is not None
        assert released.protected_trade_symbols == ()
        assert released.removed == ("BTCUSDT",)
        assert service.active_symbols() == ()

    asyncio.run(scenario())


def test_incremental_audit_to_top_n_to_shadow_decision(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        audit_path = tmp_path / "setup-audit.jsonl"
        records = [
            {
                "symbol": f"S{index:03d}USDT",
                "price_context": {
                    "observed_at": NOW.isoformat(),
                    "source_direction": "UP",
                    "setup_direction": "UP",
                    "market_price": 100.0,
                    "atr": 1.0,
                    "trigger_price": 100.0,
                    "invalidation_price": 98.0,
                    "local_range_low": 99.0,
                    "local_range_high": 101.0,
                    "setup_state": "READY_TO_CONSIDER",
                    "setup_confidence": 100.0 - index,
                    "volume_confirmation": True,
                    "volatility_confirmation": True,
                    "liquidity_ok": True,
                    "confirmations": ["Verified breakout."],
                    "warnings": [],
                },
            }
            for index in range(105)
        ]
        audit_path.write_text(
            "".join(json.dumps(item) + "\n" for item in records),
            encoding="utf-8",
        )
        verified = JsonlVerifiedSetupProvider(audit_path)
        adapter = VerifiedPriceContextAdapter(verified)
        manager = DynamicVerifiedTargetManager(
            verified,
            DynamicTargetSettings(max_active_symbols=5),
        )
        controller = FakeSubscriptionController()
        service = LiveShadowPipeline(
            symbols=(),
            price_context_provider=adapter,
            target_provider=adapter.target,
            orchestrator=ShadowOrchestrator(
                journal=ShadowTradeJournal(tmp_path / "integration-shadow.jsonl")
            ),
            dynamic_target_manager=manager,
            subscription_controller=cast(UnifiedMarketDataCollector, controller),
            clock=lambda: NOW,
        )

        first = await service.refresh_targets()
        assert first is not None
        assert first.desired_symbols == tuple(f"S{index:03d}USDT" for index in range(5))
        for _ in range(100):
            unchanged = await service.refresh_targets()
            assert unchanged is not None
            assert unchanged.added == unchanged.removed == ()
        assert verified.metrics.bootstrap_scans == 1
        assert verified.metrics.incremental_reads == 0
        assert controller.calls == [first.active_symbols]

        appended = {
            "symbol": "A000USDT",
            "price_context": {
                "observed_at": NOW.isoformat(),
                "source_direction": "UP",
                "setup_direction": "UP",
                "market_price": 100.0,
                "atr": 1.0,
                "trigger_price": 100.0,
                "invalidation_price": 98.0,
                "local_range_low": 99.0,
                "local_range_high": 101.0,
                "setup_state": "READY_TO_CONSIDER",
                "setup_confidence": 100.0,
                "volume_confirmation": True,
                "volatility_confirmation": True,
                "liquidity_ok": True,
                "confirmations": ["Verified breakout."],
                "warnings": [],
            },
        }
        with audit_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(appended) + "\n")
        changed = await service.refresh_targets()
        assert changed is not None
        assert changed.added == ("A000USDT",)
        assert changed.removed == ("S004USDT",)
        assert verified.metrics.bootstrap_scans == 1
        assert verified.metrics.incremental_reads == 1
        assert controller.calls == [first.active_symbols, changed.active_symbols]

        result = await ready_result(service, symbol="A000USDT")
        assert result.snapshot is not None
        assert result.score is not None
        assert result.trade is not None
        assert service.metrics().active_symbols == 5
        assert service.metrics().active_topics == 15

    asyncio.run(scenario())


def test_static_pipeline_keeps_fixed_symbols_without_dynamic_manager(
    tmp_path: Path,
) -> None:
    service = pipeline(tmp_path, "BTCUSDT", "ETHUSDT")
    assert service.active_symbols() == ("BTCUSDT", "ETHUSDT")
    assert service.dynamic_universe_snapshot() is None
    assert service.metrics().target_refreshes == 0


def test_repeated_dynamic_add_remove_cycles_keep_runtime_state_bounded(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        provider = MutableVerifiedProvider()
        controller = FakeSubscriptionController()
        service = dynamic_pipeline(
            tmp_path,
            provider,
            controller,
            maximum=1,
            config=LiveShadowPipelineConfig(retired_symbol_capacity=3),
        )

        for index in range(100):
            symbol = f"S{index:03d}USDT"
            provider.records = (verified_record(symbol),)
            refreshed = await service.refresh_targets()
            assert refreshed is not None
            assert service.active_symbols() == (symbol,)
            assert len(controller.symbols) == 1

        metrics = service.metrics()
        assert metrics.active_symbols == 1
        assert metrics.desired_symbols == 1
        assert metrics.target_refreshes == 100
        assert metrics.symbols_added == 100
        assert metrics.symbols_removed == 99

        assert metrics.retired_symbol_tombstones == 3

    asyncio.run(scenario())
