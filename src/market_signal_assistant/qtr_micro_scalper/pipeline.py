from __future__ import annotations

import asyncio
import hashlib
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TypeAlias

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    LiquidityBookFrame,
    LiquidityIntelligenceLayer,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    MarketStateEngine,
)
from market_signal_assistant.qtr_micro_scalper.data.models import (
    LiquidationEvent,
    OrderBookEvent,
    PublicTradeEvent,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookState
from market_signal_assistant.qtr_micro_scalper.data.trades import (
    IngestStatus,
    TradeFlowAccumulator,
)
from market_signal_assistant.qtr_micro_scalper.dynamic_targets import (
    DynamicUniverseSnapshot,
    DynamicVerifiedTargetManager,
)
from market_signal_assistant.qtr_micro_scalper.inplay_bridge import ScalperTarget
from market_signal_assistant.qtr_micro_scalper.lifecycle_bridge import (
    LiveShadowLifecycleBridge,
)
from market_signal_assistant.qtr_micro_scalper.live.collector import (
    ManagedMarketStream,
    UnifiedMarketDataCollector,
    UnifiedSubscriptionMetrics,
    WebSocketFactory,
    default_websocket_factory,
)
from market_signal_assistant.qtr_micro_scalper.live.liquidations_ws import (
    LiquidationCollector,
)
from market_signal_assistant.qtr_micro_scalper.live.orderbook_ws import (
    OrderBookCollector,
)
from market_signal_assistant.qtr_micro_scalper.live.trades_ws import (
    PublicTradeCollector,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import (
    ShadowAnalysisInput,
    ShadowOrchestrator,
)
from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperScore,
    ScalperScoringEngine,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    SetupContextEngine,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import ShadowTrade
from market_signal_assistant.qtr_micro_scalper.snapshot import (
    MicrostructureSnapshotBuilder,
    MicrostructureSnapshotBundle,
    SnapshotReadiness,
)

MarketDataEvent: TypeAlias = PublicTradeEvent | OrderBookEvent | LiquidationEvent
PriceContextProvider: TypeAlias = Callable[[str, datetime, float], PriceContext | None]
TargetProvider: TypeAlias = Callable[[str, datetime], ScalperTarget | None]
OrderBookStateProvider: TypeAlias = Callable[[str], OrderBookState]


class PipelineEventType(StrEnum):
    MARKET_DATA_RECEIVED = "MARKET_DATA_RECEIVED"
    SNAPSHOT_READY = "SNAPSHOT_READY"
    SCORE_CREATED = "SCORE_CREATED"
    SHADOW_DECISION = "SHADOW_DECISION"
    JOURNAL_UPDATED = "JOURNAL_UPDATED"


@dataclass(frozen=True, slots=True)
class LiveShadowPipelineConfig:
    aggressive_notional_baseline: float = 1_000.0
    maximum_book_age_ms: float = 1_000.0
    maximum_trade_age_ms: float = 750.0
    liquidation_retention_count: int = 500
    event_deduplication_capacity: int = 100_000
    event_retention_capacity: int = 10_000
    queue_capacity: int = 10_000
    retired_symbol_capacity: int = 1_000

    def __post_init__(self) -> None:
        for name, value in (
            ("aggressive notional baseline", self.aggressive_notional_baseline),
            ("maximum book age", self.maximum_book_age_ms),
            ("maximum trade age", self.maximum_trade_age_ms),
        ):
            if value <= 0:
                raise ValueError(f"Pipeline {name} must be positive.")
        for name, value in (
            ("liquidation retention", self.liquidation_retention_count),
            ("event deduplication capacity", self.event_deduplication_capacity),
            ("event retention capacity", self.event_retention_capacity),
            ("queue capacity", self.queue_capacity),
            ("retired symbol capacity", self.retired_symbol_capacity),
        ):
            if isinstance(value, bool) or value < 1:
                raise ValueError(f"Pipeline {name} must be positive.")


@dataclass(frozen=True, slots=True)
class LiveShadowPipelineEvent:
    event_id: str
    sequence: int
    event_type: PipelineEventType
    occurred_at: datetime
    symbol: str
    message: str


@dataclass(frozen=True, slots=True)
class PipelineProcessResult:
    symbol: str
    accepted: bool
    snapshot: MicrostructureSnapshotBundle | None
    score: ScalperScore | None
    trade: ShadowTrade | None
    events: tuple[LiveShadowPipelineEvent, ...]
    reason: str
    error: str | None = None


@dataclass(frozen=True, slots=True)
class LiveShadowPipelineMetrics:
    market_events_received: int
    duplicate_events_suppressed: int
    snapshots_ready: int
    scores_created: int
    shadow_decisions: int
    journal_updates: int
    stale_data_suppressed: int
    errors: int
    active_symbols: int

    target_refreshes: int = 0
    eligible_verified_symbols: int = 0
    desired_symbols: int = 0
    protected_trade_symbols: int = 0
    symbols_added: int = 0
    symbols_removed: int = 0
    subscribe_operations: int = 0
    unsubscribe_operations: int = 0
    subscription_errors: int = 0
    active_topics: int = 0
    last_target_refresh_at: datetime | None = None
    retired_symbol_tombstones: int = 0
    queue_depth: int = 0
    retained_events: int = 0
    background_error: str | None = None


@dataclass(frozen=True, slots=True)
class _QueuedEvent:
    event: MarketDataEvent
    already_applied: bool


class LiveShadowPipeline:
    """Async, public-data-only bridge into the V2 shadow orchestrator."""

    def __init__(
        self,
        *,
        symbols: Iterable[str],
        price_context_provider: PriceContextProvider,
        target_provider: TargetProvider | None = None,
        config: LiveShadowPipelineConfig | None = None,
        trade_flow: TradeFlowAccumulator | None = None,
        orderbook_state_provider: OrderBookStateProvider | None = None,
        liquidity_layer: LiquidityIntelligenceLayer | None = None,
        market_state_engine: MarketStateEngine | None = None,
        setup_context_engine: SetupContextEngine | None = None,
        snapshot_builder: MicrostructureSnapshotBuilder | None = None,
        scoring_engine: ScalperScoringEngine | None = None,
        orchestrator: ShadowOrchestrator | None = None,
        lifecycle_bridge: LiveShadowLifecycleBridge | None = None,
        market_collector: ManagedMarketStream | None = None,
        dynamic_target_manager: DynamicVerifiedTargetManager | None = None,
        subscription_controller: UnifiedMarketDataCollector | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        normalized = tuple(
            dict.fromkeys(
                symbol.strip().upper() for symbol in symbols if symbol.strip()
            )
        )
        if not normalized and dynamic_target_manager is None:
            raise ValueError("Live shadow pipeline requires at least one symbol.")
        self._symbols = set(normalized)
        self._price_context = price_context_provider
        self._target_provider = target_provider
        self._config = config or LiveShadowPipelineConfig()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._trade_flow = trade_flow or TradeFlowAccumulator(clock=self._clock)
        self._owned_books = {
            symbol: OrderBookState(
                symbol,
                depth=50,
                require_contiguous_update_ids=False,
            )
            for symbol in normalized
        }
        self._book_provider = orderbook_state_provider or self._owned_books.__getitem__
        self._liquidity = liquidity_layer or LiquidityIntelligenceLayer()
        self._market_state = market_state_engine or MarketStateEngine()
        self._setup_context = setup_context_engine or SetupContextEngine()
        self._snapshots = snapshot_builder or MicrostructureSnapshotBuilder()
        self._scoring = scoring_engine or ScalperScoringEngine()
        self._orchestrator = orchestrator or ShadowOrchestrator()
        self._lifecycle = lifecycle_bridge or LiveShadowLifecycleBridge(
            self._orchestrator
        )
        self._market_collector = market_collector
        self._dynamic_targets = dynamic_target_manager
        self._subscription_controller = subscription_controller
        self._previous_frames: dict[str, LiquidityBookFrame] = {}
        self._current_frames: dict[str, LiquidityBookFrame] = {}
        self._liquidations: dict[str, list[LiquidationEvent]] = {}
        self._seen_events: dict[str, None] = {}
        self._registered_provider_targets: set[str] = set()
        self._symbol_locks = {symbol: asyncio.Lock() for symbol in normalized}
        self._retired_symbols: dict[str, None] = {}
        self._events: deque[LiveShadowPipelineEvent] = deque(
            maxlen=self._config.event_retention_capacity
        )
        self._sequence = 0
        self._received = 0
        self._duplicates = 0
        self._snapshots_ready = 0
        self._scores_created = 0
        self._shadow_decisions = 0
        self._journal_updates = 0
        self._stale = 0
        self._errors = 0
        self._queue: asyncio.Queue[_QueuedEvent] | None = None
        self._worker: asyncio.Task[None] | None = None
        self._target_refresh_task: asyncio.Task[None] | None = None
        self._target_refresh_stop = asyncio.Event()
        self._universe_lock = asyncio.Lock()
        self._background_error: str | None = None

    @classmethod
    def with_live_collectors(
        cls,
        *,
        symbols: Iterable[str],
        price_context_provider: PriceContextProvider,
        target_provider: TargetProvider | None = None,
        websocket_factory: WebSocketFactory = default_websocket_factory,
        orchestrator: ShadowOrchestrator | None = None,
        config: LiveShadowPipelineConfig | None = None,
        clock: Callable[[], datetime] | None = None,
        dynamic_target_manager: DynamicVerifiedTargetManager | None = None,
    ) -> LiveShadowPipeline:
        """Compose lazy Bybit public streams without opening a connection."""

        normalized = tuple(
            dict.fromkeys(
                symbol.strip().upper() for symbol in symbols if symbol.strip()
            )
        )
        selected_clock = clock or (lambda: datetime.now(UTC))
        accumulator = TradeFlowAccumulator(clock=selected_clock)
        holder: list[LiveShadowPipeline] = []

        def applied_sink(event: MarketDataEvent) -> None:
            holder[0].enqueue_event(event, already_applied=True)

        def liquidation_sink(event: LiquidationEvent) -> None:
            holder[0].enqueue_event(event)

        trades = PublicTradeCollector(
            normalized,
            accumulator,
            websocket_factory=websocket_factory,
            event_sink=applied_sink,
        )
        books = OrderBookCollector(
            normalized,
            websocket_factory=websocket_factory,
            event_sink=applied_sink,
        )
        liquidations = LiquidationCollector(
            normalized,
            liquidation_sink,
            websocket_factory=websocket_factory,
        )
        unified = UnifiedMarketDataCollector(
            (trades, books, liquidations),
            dynamic_streams=(trades, books, liquidations),
        )
        pipeline = cls(
            symbols=normalized,
            price_context_provider=price_context_provider,
            target_provider=target_provider,
            config=config,
            trade_flow=accumulator,
            orderbook_state_provider=books.state,
            orchestrator=orchestrator,
            market_collector=unified,
            clock=selected_clock,
            dynamic_target_manager=dynamic_target_manager,
            subscription_controller=unified,
        )
        holder.append(pipeline)
        return pipeline

    def register_target(self, target: ScalperTarget, *, observed_at: datetime) -> bool:
        discovered = self._orchestrator.discover_target(
            target,
            observed_at=observed_at,
        )
        if not discovered.accepted:
            return False
        return self._orchestrator.activate_target(
            target.symbol,
            activated_at=observed_at,
        ).accepted

    async def refresh_targets(self) -> DynamicUniverseSnapshot | None:
        manager = self._dynamic_targets
        if manager is None:
            return None
        snapshot = manager.refresh(
            at=self._clock(),
            protected_symbols=self._lifecycle.tracked_symbols(),
        )
        await self.update_symbols(snapshot.active_symbols)
        return snapshot

    async def update_symbols(self, symbols: Iterable[str]) -> None:
        normalized = set(item.strip().upper() for item in symbols if item.strip())
        async with self._universe_lock:
            current = set(self._symbols)
            added = tuple(sorted(normalized - current))
            removed = tuple(sorted(current - normalized))
            if not added and not removed:
                return
            for symbol in added:
                self._owned_books[symbol] = OrderBookState(
                    symbol,
                    depth=50,
                    require_contiguous_update_ids=False,
                )
                self._symbol_locks[symbol] = asyncio.Lock()
                self._retired_symbols.pop(symbol, None)
            try:
                if self._subscription_controller is not None:
                    await self._subscription_controller.update_symbols(
                        tuple(sorted(normalized))
                    )
            except Exception:
                for symbol in added:
                    self._owned_books.pop(symbol, None)
                    self._symbol_locks.pop(symbol, None)
                raise
            for symbol in removed:
                lock = self._symbol_locks.get(symbol)
                if lock is not None:
                    async with lock:
                        self._cleanup_symbol(symbol)
                else:
                    self._cleanup_symbol(symbol)
            self._symbols = normalized

    def active_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._symbols))

    def dynamic_universe_snapshot(self) -> DynamicUniverseSnapshot | None:
        manager = self._dynamic_targets
        return manager.snapshot() if manager is not None else None

    def _cleanup_symbol(self, symbol: str) -> None:
        self._symbol_locks.pop(symbol, None)
        self._owned_books.pop(symbol, None)
        self._previous_frames.pop(symbol, None)
        self._current_frames.pop(symbol, None)
        self._liquidations.pop(symbol, None)
        self._registered_provider_targets.discard(symbol)
        self._trade_flow.remove_symbol(symbol)
        self._retired_symbols.pop(symbol, None)
        self._retired_symbols[symbol] = None
        while len(self._retired_symbols) > self._config.retired_symbol_capacity:
            self._retired_symbols.pop(next(iter(self._retired_symbols)))

    async def _target_refresh_loop(self) -> None:
        manager = self._dynamic_targets
        if manager is None:
            return
        while not self._target_refresh_stop.is_set():
            try:
                await asyncio.wait_for(
                    self._target_refresh_stop.wait(),
                    timeout=manager.settings.refresh_seconds,
                )
            except TimeoutError:
                try:
                    await self.refresh_targets()
                except (OSError, RuntimeError, TypeError, ValueError):
                    self._errors += 1
            else:
                return

    async def start(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        self._background_error = None
        self._queue = asyncio.Queue(maxsize=self._config.queue_capacity)
        if self._dynamic_targets is not None:
            self._target_refresh_stop.clear()
            await self.refresh_targets()
        self._worker = asyncio.create_task(self._consume())
        self._worker.add_done_callback(
            lambda task: self._capture_task_failure("pipeline worker", task)
        )
        if self._market_collector is not None:
            await self._market_collector.start()
        if self._dynamic_targets is not None:
            self._target_refresh_task = asyncio.create_task(self._target_refresh_loop())
            self._target_refresh_task.add_done_callback(
                lambda task: self._capture_task_failure("target refresh", task)
            )

    async def stop(self) -> None:
        self._target_refresh_stop.set()
        refresh_task = self._target_refresh_task
        self._target_refresh_task = None
        if refresh_task is not None:
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)
        if self._market_collector is not None:
            await self._market_collector.stop()
        queue = self._queue
        worker = self._worker
        if queue is not None and worker is not None:
            if worker.done():
                self._discard_queued_events(queue)
            else:
                joined = asyncio.create_task(queue.join())
                done, _ = await asyncio.wait(
                    (joined, worker), return_when=asyncio.FIRST_COMPLETED
                )
                if worker in done and not joined.done():
                    joined.cancel()
                    await asyncio.gather(joined, return_exceptions=True)
                    self._discard_queued_events(queue)
        worker = self._worker
        self._worker = None
        self._queue = None
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        self._lifecycle.stop()

    def enqueue_event(
        self,
        event: MarketDataEvent,
        *,
        already_applied: bool = False,
    ) -> None:
        queue = self._queue
        if queue is None:
            self._errors += 1
            return
        if self._background_error is not None:
            raise RuntimeError(self._background_error)
        try:
            queue.put_nowait(_QueuedEvent(event, already_applied))
        except asyncio.QueueFull as exc:
            self._errors += 1
            self._background_error = "Live shadow market-data queue is full."
            raise RuntimeError(self._background_error) from exc

    async def process_event(
        self,
        event: MarketDataEvent,
        *,
        already_applied: bool = False,
    ) -> PipelineProcessResult:
        symbol = event.symbol
        async with self._universe_lock:
            symbol_lock = self._symbol_locks.get(symbol)
            if symbol_lock is None:
                retired = symbol in self._retired_symbols
                self._errors += int(not retired)
                return PipelineProcessResult(
                    symbol=symbol,
                    accepted=False,
                    snapshot=None,
                    score=None,
                    trade=None,
                    events=(),
                    reason=(
                        "Late event ignored after unsubscribe."
                        if retired
                        else "Symbol is not subscribed by the pipeline."
                    ),
                    error=None if retired else "unsupported_symbol",
                )
            await symbol_lock.acquire()
        try:
            try:
                return self._process_locked(event, already_applied=already_applied)
            except OSError as exc:
                self._errors += 1
                self._background_error = (
                    "Critical shadow persistence failure: "
                    f"{type(exc).__name__}: {exc}"
                )
                return PipelineProcessResult(
                    symbol=symbol,
                    accepted=False,
                    snapshot=None,
                    score=None,
                    trade=None,
                    events=(),
                    reason="Critical shadow persistence failed closed.",
                    error=str(exc),
                )
            except (KeyError, RuntimeError, TypeError, ValueError) as exc:
                self._errors += 1
                return PipelineProcessResult(
                    symbol=symbol,
                    accepted=False,
                    snapshot=None,
                    score=None,
                    trade=None,
                    events=(),
                    reason="Market-data event failed in isolated symbol processing.",
                    error=str(exc),
                )
        finally:
            symbol_lock.release()

    def metrics(self) -> LiveShadowPipelineMetrics:
        subscriptions = (
            self._subscription_controller.subscription_metrics
            if self._subscription_controller is not None
            else UnifiedSubscriptionMetrics(0, 0, 0, 0)
        )
        targets = (
            self._dynamic_targets.metrics()
            if self._dynamic_targets is not None
            else None
        )
        return LiveShadowPipelineMetrics(
            market_events_received=self._received,
            duplicate_events_suppressed=self._duplicates,
            snapshots_ready=self._snapshots_ready,
            scores_created=self._scores_created,
            shadow_decisions=self._shadow_decisions,
            journal_updates=self._journal_updates,
            stale_data_suppressed=self._stale,
            errors=self._errors,
            active_symbols=len(self._symbols),
            target_refreshes=targets.target_refreshes if targets else 0,
            eligible_verified_symbols=(
                targets.eligible_verified_symbols if targets else 0
            ),
            desired_symbols=targets.desired_symbols if targets else len(self._symbols),
            protected_trade_symbols=(targets.protected_trade_symbols if targets else 0),
            symbols_added=targets.symbols_added if targets else 0,
            symbols_removed=targets.symbols_removed if targets else 0,
            subscribe_operations=subscriptions.subscribe_operations,
            unsubscribe_operations=subscriptions.unsubscribe_operations,
            subscription_errors=subscriptions.subscription_errors,
            retired_symbol_tombstones=len(self._retired_symbols),
            active_topics=subscriptions.active_topics,
            last_target_refresh_at=(
                targets.last_target_refresh_at if targets else None
            ),
            queue_depth=self._queue.qsize() if self._queue is not None else 0,
            retained_events=len(self._events),
            background_error=self._background_error,
        )

    def events(self) -> tuple[LiveShadowPipelineEvent, ...]:
        return tuple(self._events)

    async def _consume(self) -> None:
        queue = self._queue
        if queue is None:
            return
        while True:
            queued = await queue.get()
            try:
                await self.process_event(
                    queued.event,
                    already_applied=queued.already_applied,
                )
            finally:
                queue.task_done()

    def background_error(self) -> str | None:
        return self._background_error

    def _capture_task_failure(
        self,
        name: str,
        task: asyncio.Task[None],
    ) -> None:
        if task.cancelled():
            return
        exception = task.exception()
        if exception is None:
            if name == "target refresh" and self._target_refresh_stop.is_set():
                return
            message = f"Critical {name} stopped unexpectedly."
        else:
            message = f"Critical {name} failed: {type(exception).__name__}: {exception}"
        self._errors += 1
        self._background_error = message

    @staticmethod
    def _discard_queued_events(queue: asyncio.Queue[_QueuedEvent]) -> None:
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            queue.task_done()

    def _process_locked(
        self,
        event: MarketDataEvent,
        *,
        already_applied: bool,
    ) -> PipelineProcessResult:
        fingerprint = _event_fingerprint(event)
        if fingerprint in self._seen_events:
            self._duplicates += 1
            return PipelineProcessResult(
                symbol=event.symbol,
                accepted=False,
                snapshot=None,
                score=None,
                trade=None,
                events=(),
                reason="Duplicate market-data event suppressed.",
            )
        self._seen_events[fingerprint] = None
        if len(self._seen_events) > self._config.event_deduplication_capacity:
            del self._seen_events[next(iter(self._seen_events))]
        as_of = max(event.exchange_at, event.received_at)
        if not already_applied and not self._apply(event, as_of=as_of):
            self._duplicates += 1
            return PipelineProcessResult(
                symbol=event.symbol,
                accepted=False,
                snapshot=None,
                score=None,
                trade=None,
                events=(),
                reason="Underlying collector rejected a duplicate or stale event.",
            )
        if already_applied and isinstance(event, OrderBookEvent):
            self._capture_book_frame(event.symbol, as_of=as_of)

        self._received += 1
        journal_before = self._orchestrator.metrics().journal_records
        lifecycle_results = self._lifecycle.process_event(event)
        lifecycle_error = next(
            (item.error for item in lifecycle_results if item.error is not None),
            None,
        )
        if lifecycle_error is not None:
            raise RuntimeError(lifecycle_error)
        lifecycle_trade = next(
            (
                item.trade
                for item in reversed(lifecycle_results)
                if item.trade is not None
            ),
            None,
        )
        emitted = [
            self._emit(
                PipelineEventType.MARKET_DATA_RECEIVED,
                occurred_at=as_of,
                symbol=event.symbol,
                semantic_key=fingerprint,
                message="📡 MARKET_DATA_RECEIVED: normalized public event accepted.",
            )
        ]
        prepared = self._prepare_analysis(event.symbol, as_of=as_of)
        if prepared is None:
            self._append_journal_update(
                emitted,
                journal_before=journal_before,
                occurred_at=as_of,
                symbol=event.symbol,
                semantic_key=fingerprint,
            )
            return PipelineProcessResult(
                symbol=event.symbol,
                accepted=True,
                snapshot=None,
                score=None,
                trade=lifecycle_trade,
                events=tuple(emitted),
                reason="Market data accepted; analysis inputs are not ready.",
            )
        analysis, snapshot, score = prepared
        emitted.extend(
            (
                self._emit(
                    PipelineEventType.SNAPSHOT_READY,
                    occurred_at=as_of,
                    symbol=event.symbol,
                    semantic_key=fingerprint,
                    message="💎 SNAPSHOT_READY: all shadow components are fresh.",
                ),
                self._emit(
                    PipelineEventType.SCORE_CREATED,
                    occurred_at=as_of,
                    symbol=event.symbol,
                    semantic_key=f"{fingerprint}|{score.total_score:.12g}",
                    message="🎯 SCORE_CREATED: explainable shadow score created.",
                ),
            )
        )
        decision = self._orchestrator.analyze(analysis)
        if decision.error is not None:
            raise RuntimeError(decision.error)
        self._shadow_decisions += 1
        decision_trade_id = (
            decision.trade.trade_id if decision.trade is not None else "NO_TRADE"
        )
        emitted.append(
            self._emit(
                PipelineEventType.SHADOW_DECISION,
                occurred_at=as_of,
                symbol=event.symbol,
                semantic_key=f"{fingerprint}|{decision_trade_id}",
                message="⚔️ SHADOW_DECISION: no real order authority is present.",
            )
        )
        after_journal = self._orchestrator.metrics().journal_records
        if decision.trade is not None:
            self._lifecycle.activate(decision.trade)
        self._append_journal_update(
            emitted,
            journal_before=journal_before,
            occurred_at=as_of,
            symbol=event.symbol,
            semantic_key=fingerprint,
            journal_after=after_journal,
        )
        return PipelineProcessResult(
            symbol=event.symbol,
            accepted=True,
            snapshot=snapshot,
            score=decision.score or score,
            trade=decision.trade or lifecycle_trade,
            events=tuple(emitted),
            reason="Fresh market snapshot processed by the shadow orchestrator.",
        )

    def _append_journal_update(
        self,
        emitted: list[LiveShadowPipelineEvent],
        *,
        journal_before: int,
        occurred_at: datetime,
        symbol: str,
        semantic_key: str,
        journal_after: int | None = None,
    ) -> None:
        resolved_after = (
            self._orchestrator.metrics().journal_records
            if journal_after is None
            else journal_after
        )
        if resolved_after <= journal_before:
            return
        self._journal_updates += 1
        emitted.append(
            self._emit(
                PipelineEventType.JOURNAL_UPDATED,
                occurred_at=occurred_at,
                symbol=symbol,
                semantic_key=f"{semantic_key}|{resolved_after}",
                message="📝 JOURNAL_UPDATED: shadow lifecycle persisted.",
            )
        )

    def _apply(self, event: MarketDataEvent, *, as_of: datetime) -> bool:
        if isinstance(event, PublicTradeEvent):
            return self._trade_flow.ingest(event).status is IngestStatus.ACCEPTED
        if isinstance(event, OrderBookEvent):
            result = self._book_provider(event.symbol).process(event)
            if result.status.value.startswith("ignored"):
                return False
            self._capture_book_frame(event.symbol, as_of=as_of)
            return True
        retained = self._liquidations.setdefault(event.symbol, [])
        retained.append(event)
        del retained[: -self._config.liquidation_retention_count]
        return True

    def _capture_book_frame(self, symbol: str, *, as_of: datetime) -> None:
        state = self._book_provider(symbol)
        if not state.ready:
            return
        frame = LiquidityBookFrame.from_state(state, as_of=as_of)
        current = self._current_frames.get(symbol)
        if current is not None:
            self._previous_frames[symbol] = current
        self._current_frames[symbol] = frame

    def _prepare_analysis(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> (
        tuple[
            ShadowAnalysisInput,
            MicrostructureSnapshotBundle,
            ScalperScore,
        ]
        | None
    ):
        previous = self._previous_frames.get(symbol)
        current = self._current_frames.get(symbol)
        if previous is None or current is None:
            return None
        trade_flow = self._trade_flow.metrics(symbol, as_of=as_of)
        current = LiquidityBookFrame.from_state(
            self._book_provider(symbol),
            as_of=as_of,
        )
        if _stale(
            current,
            trade_flow.last_trade_at,
            as_of,
            self._config.maximum_book_age_ms,
            self._config.maximum_trade_age_ms,
        ):
            self._stale += 1
            return None
        liquidity = self._liquidity.analyze(
            previous,
            current,
            trade_flow,
            aggressive_notional_baseline=self._config.aggressive_notional_baseline,
        )
        market_state = self._market_state.assess(
            current,
            trade_flow,
            liquidity,
            assessed_at=as_of,
        )
        if not market_state.ready or current.metrics.mid_price is None:
            return None
        price_context = self._price_context(
            symbol,
            as_of,
            current.metrics.mid_price,
        )
        if price_context is None or not price_context.ready:
            return None
        if (
            self._target_provider is not None
            and symbol not in self._registered_provider_targets
        ):
            target = self._target_provider(symbol, as_of)
            if target is None or not self.register_target(target, observed_at=as_of):
                return None
            self._registered_provider_targets.add(symbol)
        setup_context = self._setup_context.analyze(market_state, price_context)
        score = self._scoring.score(
            market_state,
            liquidity,
            trade_flow,
            current.metrics,
            setup_context,
            setup_context.risk,
        )
        snapshot = self._snapshots.build(
            symbol=symbol,
            generated_at=as_of,
            trade_flow=trade_flow,
            orderbook=current.metrics,
            liquidity=liquidity,
            market_state=market_state,
            setup_context=setup_context,
            scalper_score=score,
        )
        if snapshot.readiness is not SnapshotReadiness.READY:
            return None
        self._snapshots_ready += 1
        self._scores_created += 1
        return (
            ShadowAnalysisInput(
                symbol=symbol,
                generated_at=as_of,
                trade_flow=trade_flow,
                orderbook=current.metrics,
                liquidity=liquidity,
                market_state=market_state,
                setup_context=setup_context,
            ),
            snapshot,
            score,
        )

    def _emit(
        self,
        event_type: PipelineEventType,
        *,
        occurred_at: datetime,
        symbol: str,
        semantic_key: str,
        message: str,
    ) -> LiveShadowPipelineEvent:
        self._sequence += 1
        identity = f"{event_type.value}|{symbol}|{semantic_key}"
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
        event = LiveShadowPipelineEvent(
            event_id=f"live-shadow-{digest}",
            sequence=self._sequence,
            event_type=event_type,
            occurred_at=_utc(occurred_at),
            symbol=symbol,
            message=message,
        )
        self._events.append(event)
        return event


def _event_fingerprint(event: MarketDataEvent) -> str:
    if isinstance(event, PublicTradeEvent):
        identity = f"trade|{event.symbol}|{event.trade_id}"
    elif isinstance(event, OrderBookEvent):
        identity = (
            f"book|{event.symbol}|{event.event_type.value}|{event.update_id}|"
            f"{event.cross_sequence}"
        )
    else:
        identity = (
            f"liquidation|{event.symbol}|{event.exchange_at.isoformat()}|"
            f"{event.side.value}|{event.bankruptcy_price:.12g}|{event.quantity:.12g}"
        )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _stale(
    book: LiquidityBookFrame,
    trade_at: datetime | None,
    as_of: datetime,
    maximum_book_age_ms: float,
    maximum_trade_age_ms: float,
) -> bool:
    book_at = book.metrics.book_exchange_at
    if book_at is None or trade_at is None:
        return True
    book_age = (as_of - book_at).total_seconds() * 1_000
    trade_age = (as_of - trade_at).total_seconds() * 1_000
    return (
        book_age < 0
        or trade_age < 0
        or book_age > maximum_book_age_ms
        or trade_age > maximum_trade_age_ms
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Pipeline timestamps must be timezone-aware.")
    return value.astimezone(UTC)
