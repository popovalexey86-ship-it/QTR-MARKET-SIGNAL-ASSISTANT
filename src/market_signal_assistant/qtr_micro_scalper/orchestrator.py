from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from threading import Lock

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    LiquidityIntelligence,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import (
    MarketStateAssessment,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookMetrics
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics
from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    ShadowDecisionEventType,
    ShadowDecisionJournal,
    ShadowDecisionRecord,
)
from market_signal_assistant.qtr_micro_scalper.inplay_bridge import (
    InPlayTargetBridge,
    ScalperTarget,
    ScalperTargetLifecycle,
)
from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperScore,
    ScalperScoringEngine,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowOpportunity
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowDecisionEngine,
    ShadowPriceBar,
    ShadowTrade,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    DEFAULT_SHADOW_JOURNAL_PATH,
    ShadowTradeJournal,
    build_shadow_trade_record,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntime,
)
from market_signal_assistant.qtr_micro_scalper.snapshot import (
    MicrostructureSnapshotBuilder,
)


class ShadowOrchestratorEventType(StrEnum):
    TARGET_FOUND = "TARGET_FOUND"
    ANALYSIS_STARTED = "ANALYSIS_STARTED"
    SCORE_READY = "SCORE_READY"
    ENTRY_CREATED = "ENTRY_CREATED"
    POSITION_UPDATED = "POSITION_UPDATED"
    TRADE_FINISHED = "TRADE_FINISHED"


@dataclass(frozen=True, slots=True)
class ShadowOrchestratorEvent:
    event_id: str
    sequence: int
    event_type: ShadowOrchestratorEventType
    occurred_at: datetime
    symbol: str
    trade_id: str | None
    message: str

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.message.strip():
            raise ValueError("Shadow orchestrator event cannot be empty.")
        if isinstance(self.sequence, bool) or self.sequence < 1:
            raise ValueError("Shadow orchestrator event sequence must be positive.")
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Shadow orchestrator event symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at))
        if self.trade_id is not None and not self.trade_id.strip():
            raise ValueError("Shadow orchestrator trade id cannot be blank.")


@dataclass(frozen=True, slots=True)
class ShadowOrchestratorAction:
    accepted: bool
    target: ScalperTarget | None
    events: tuple[ShadowOrchestratorEvent, ...]
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("Shadow orchestrator action requires a reason.")


@dataclass(frozen=True, slots=True)
class ShadowAnalysisInput:
    symbol: str
    generated_at: datetime
    trade_flow: TradeFlowMetrics
    orderbook: OrderBookMetrics
    liquidity: LiquidityIntelligence
    market_state: MarketStateAssessment
    setup_context: ShadowOpportunity

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Shadow analysis symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "generated_at", _utc(self.generated_at))


@dataclass(frozen=True, slots=True)
class ShadowAnalysisResult:
    symbol: str
    score: ScalperScore | None
    trade: ShadowTrade | None
    events: tuple[ShadowOrchestratorEvent, ...]
    error: str | None

    @property
    def successful(self) -> bool:
        return self.error is None


@dataclass(frozen=True, slots=True)
class ShadowBarResult:
    symbol: str
    trade: ShadowTrade | None
    events: tuple[ShadowOrchestratorEvent, ...]
    error: str | None

    @property
    def successful(self) -> bool:
        return self.error is None


@dataclass(frozen=True, slots=True)
class ShadowOrchestratorMetrics:
    targets_discovered: int
    active_targets: int
    snapshots_received: int
    scores_created: int
    shadow_trades_created: int
    trade_updates: int
    trades_closed: int
    active_shadow_trades: int
    journal_records: int
    events_emitted: int
    duplicates_suppressed: int
    errors: int


class ShadowOrchestrator:
    """Main offline coordinator for the isolated Scalper V2 shadow pipeline."""

    def __init__(
        self,
        *,
        bridge: InPlayTargetBridge | None = None,
        snapshot_builder: MicrostructureSnapshotBuilder | None = None,
        scoring_engine: ScalperScoringEngine | None = None,
        decision_engine: ShadowDecisionEngine | None = None,
        runtime: ShadowRuntime | None = None,
        journal: ShadowTradeJournal | None = None,
        decision_journal: ShadowDecisionJournal | None = None,
        journal_path: Path = DEFAULT_SHADOW_JOURNAL_PATH,
    ) -> None:
        self._bridge = bridge or InPlayTargetBridge()
        self._snapshots = snapshot_builder or MicrostructureSnapshotBuilder()
        self._scoring = scoring_engine or ScalperScoringEngine()
        self._decisions = decision_engine or ShadowDecisionEngine()
        self._runtime = runtime or ShadowRuntime(
            scoring_engine=self._scoring,
            decision_engine=self._decisions,
        )
        self._journal = journal or ShadowTradeJournal(journal_path)
        self._decision_journal = decision_journal
        self._scores_by_trade: dict[str, ScalperScore] = {}
        self._context_by_trade: dict[str, tuple[str, str]] = {}
        self._events: list[ShadowOrchestratorEvent] = []
        self._event_fingerprints: set[str] = set()
        self._sequence = 0
        self._targets_discovered = 0
        self._snapshots_received = 0
        self._scores_created = 0
        self._shadow_trades_created = 0
        self._trade_updates = 0
        self._trades_closed = 0
        self._duplicates_suppressed = 0
        self._errors = 0
        self._lock = Lock()

    def discover_target(
        self,
        target: ScalperTarget,
        *,
        observed_at: datetime,
    ) -> ShadowOrchestratorAction:
        normalized_at = _utc(observed_at)
        with self._lock:
            decision = self._bridge.discover(target, observed_at=normalized_at)
            if not decision.accepted or decision.target is None:
                return ShadowOrchestratorAction(
                    accepted=False,
                    target=None,
                    events=(),
                    reason=decision.reason,
                )
            event = self._emit(
                ShadowOrchestratorEventType.TARGET_FOUND,
                occurred_at=normalized_at,
                symbol=decision.target.symbol,
                trade_id=None,
                semantic_key=decision.target.discovered_at.isoformat(),
                message="🔥 TARGET_FOUND: scanner target entered the V2 bridge.",
            )
            events = (event,) if event is not None else ()
            if event is None:
                self._duplicates_suppressed += 1
            else:
                self._targets_discovered += 1
                self._persist_decision(
                    ShadowDecisionEventType.TARGET_FOUND,
                    timestamp=normalized_at,
                    symbol=decision.target.symbol,
                    reasons=(decision.target.reason,),
                )
            return ShadowOrchestratorAction(
                accepted=True,
                target=decision.target,
                events=events,
                reason=decision.reason,
            )

    def activate_target(
        self,
        symbol: str,
        *,
        activated_at: datetime,
    ) -> ShadowOrchestratorAction:
        normalized_symbol = _symbol(symbol)
        normalized_at = _utc(activated_at)
        with self._lock:
            target = self._target(normalized_symbol)
            if target is None or target.lifecycle is ScalperTargetLifecycle.REMOVED:
                return ShadowOrchestratorAction(
                    accepted=False,
                    target=None,
                    events=(),
                    reason="Target is not available for activation.",
                )
            if target.lifecycle is ScalperTargetLifecycle.ACTIVE:
                self._duplicates_suppressed += 1
                return ShadowOrchestratorAction(
                    accepted=True,
                    target=target,
                    events=(),
                    reason="Target is already ACTIVE.",
                )
            if target.lifecycle is ScalperTargetLifecycle.DISCOVERED:
                target = self._bridge.begin_watching(
                    normalized_symbol,
                    changed_at=normalized_at,
                )
            if target.lifecycle is ScalperTargetLifecycle.WATCHING:
                target = self._bridge.activate(
                    normalized_symbol,
                    changed_at=normalized_at,
                )
            event = self._emit(
                ShadowOrchestratorEventType.ANALYSIS_STARTED,
                occurred_at=normalized_at,
                symbol=normalized_symbol,
                trade_id=None,
                semantic_key=target.discovered_at.isoformat(),
                message="👁 ANALYSIS_STARTED: deep offline analysis activated.",
            )
            if event is not None:
                self._persist_decision(
                    ShadowDecisionEventType.ANALYSIS_STARTED,
                    timestamp=normalized_at,
                    symbol=normalized_symbol,
                    reasons=("Target activated for Scalper V2 shadow analysis.",),
                )
            return ShadowOrchestratorAction(
                accepted=True,
                target=target,
                events=(event,) if event is not None else (),
                reason="Target activated for Scalper V2 shadow analysis.",
            )

    def analyze(
        self,
        analysis: ShadowAnalysisInput,
    ) -> ShadowAnalysisResult:
        with self._lock:
            return self._analyze_locked(analysis)

    def analyze_many(
        self,
        inputs: tuple[ShadowAnalysisInput, ...],
    ) -> tuple[ShadowAnalysisResult, ...]:
        ordered = tuple(
            sorted(inputs, key=lambda item: (item.generated_at, item.symbol))
        )
        with self._lock:
            return tuple(self._analyze_locked(item) for item in ordered)

    def process_bars(
        self,
        bars: tuple[ShadowPriceBar, ...],
    ) -> tuple[ShadowBarResult, ...]:
        ordered = tuple(
            sorted(
                bars,
                key=lambda item: (item.opened_at, item.closed_at, item.symbol),
            )
        )
        with self._lock:
            return tuple(self._process_bar_locked(bar) for bar in ordered)

    def events(self) -> tuple[ShadowOrchestratorEvent, ...]:
        with self._lock:
            return tuple(self._events)

    def metrics(self) -> ShadowOrchestratorMetrics:
        with self._lock:
            active_targets = sum(
                target.lifecycle is ScalperTargetLifecycle.ACTIVE
                for target in self._bridge.watched_targets()
            )
            return ShadowOrchestratorMetrics(
                targets_discovered=self._targets_discovered,
                active_targets=active_targets,
                snapshots_received=self._snapshots_received,
                scores_created=self._scores_created,
                shadow_trades_created=self._shadow_trades_created,
                trade_updates=self._trade_updates,
                trades_closed=self._trades_closed,
                active_shadow_trades=len(self._runtime.active_trades()),
                journal_records=len(self._journal.records()),
                events_emitted=len(self._events),
                duplicates_suppressed=self._duplicates_suppressed,
                errors=self._errors,
            )

    def _analyze_locked(
        self,
        analysis: ShadowAnalysisInput,
    ) -> ShadowAnalysisResult:
        target = self._target(analysis.symbol)
        if target is None or target.lifecycle is not ScalperTargetLifecycle.ACTIVE:
            return ShadowAnalysisResult(
                symbol=analysis.symbol,
                score=None,
                trade=None,
                events=(),
                error="Target is not ACTIVE in the InPlay bridge.",
            )
        self._snapshots_received += 1
        try:
            self._snapshots.build(
                symbol=analysis.symbol,
                generated_at=analysis.generated_at,
                trade_flow=analysis.trade_flow,
                orderbook=analysis.orderbook,
                liquidity=analysis.liquidity,
                market_state=analysis.market_state,
                setup_context=analysis.setup_context,
            )
            score = self._scoring.score(
                analysis.market_state,
                analysis.liquidity,
                analysis.trade_flow,
                analysis.orderbook,
                analysis.setup_context,
                analysis.setup_context.risk,
            )
            self._scores_created += 1
            score_event = self._emit(
                ShadowOrchestratorEventType.SCORE_READY,
                occurred_at=analysis.generated_at,
                symbol=analysis.symbol,
                trade_id=None,
                semantic_key=f"{analysis.generated_at.isoformat()}|{score.total_score:.12g}",
                message="🎯 SCORE_READY: explainable scalper score created.",
            )
            bundle = self._snapshots.build(
                symbol=analysis.symbol,
                generated_at=analysis.generated_at,
                trade_flow=analysis.trade_flow,
                orderbook=analysis.orderbook,
                liquidity=analysis.liquidity,
                market_state=analysis.market_state,
                setup_context=analysis.setup_context,
                scalper_score=score,
            )
            market_state = analysis.market_state.state.value
            setup_context = analysis.setup_context.decision.value
            self._persist_decision(
                ShadowDecisionEventType.SNAPSHOT_READY,
                timestamp=analysis.generated_at,
                symbol=analysis.symbol,
                market_state=market_state,
                setup_context=setup_context,
                reasons=bundle.reasons,
                warnings=bundle.warnings,
            )
            self._persist_decision(
                ShadowDecisionEventType.SCORE_CREATED,
                timestamp=analysis.generated_at,
                symbol=analysis.symbol,
                score=score,
                market_state=market_state,
                setup_context=setup_context,
                reasons=score.reasons,
                warnings=score.warnings,
            )
            runtime_decision = self._runtime.process_snapshot(bundle)
            events = [score_event] if score_event is not None else []
            if not runtime_decision.accepted or runtime_decision.trade is None:
                if any(
                    "already exists" in reason
                    for reason in runtime_decision.reasons
                ):
                    self._duplicates_suppressed += 1
                decision_score = runtime_decision.score or score
                self._persist_decision(
                    ShadowDecisionEventType.DECISION_BLOCKED,
                    timestamp=analysis.generated_at,
                    symbol=analysis.symbol,
                    score=decision_score,
                    market_state=market_state,
                    setup_context=setup_context,
                    reasons=runtime_decision.reasons,
                    warnings=decision_score.warnings,
                )
                return ShadowAnalysisResult(
                    symbol=analysis.symbol,
                    score=runtime_decision.score or score,
                    trade=None,
                    events=tuple(events),
                    error=None,
                )
            runtime_score = runtime_decision.score or score
            trade = runtime_decision.trade
            entry_event = self._emit(
                ShadowOrchestratorEventType.ENTRY_CREATED,
                occurred_at=trade.planned_at,
                symbol=trade.symbol,
                trade_id=trade.trade_id,
                semantic_key=trade.trade_id,
                message="⚔️ ENTRY_CREATED: virtual trade plan created.",
            )
            if entry_event is not None:
                events.append(entry_event)
            self._scores_by_trade[trade.trade_id] = runtime_score
            self._context_by_trade[trade.trade_id] = (
                market_state,
                setup_context,
            )
            self._persist_decision(
                ShadowDecisionEventType.SHADOW_ENTRY_CREATED,
                timestamp=trade.planned_at,
                symbol=trade.symbol,
                score=runtime_score,
                market_state=market_state,
                setup_context=setup_context,
                reasons=runtime_decision.reasons,
                warnings=runtime_score.warnings,
            )
            self._shadow_trades_created += 1
            record = build_shadow_trade_record(
                trade,
                runtime_score,
                recorded_at=analysis.generated_at,
                events=runtime_decision.events,
                reasons=runtime_decision.reasons,
            )
            if not self._journal.append(record):
                self._duplicates_suppressed += 1
            return ShadowAnalysisResult(
                symbol=analysis.symbol,
                score=runtime_score,
                trade=trade,
                events=tuple(events),
                error=None,
            )
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            self._errors += 1
            return ShadowAnalysisResult(
                symbol=analysis.symbol,
                score=None,
                trade=None,
                events=(),
                error=str(exc),
            )

    def _process_bar_locked(self, bar: ShadowPriceBar) -> ShadowBarResult:
        try:
            update = self._runtime.process_bar(bar)
            if update.trade is None:
                return ShadowBarResult(
                    symbol=bar.symbol,
                    trade=None,
                    events=(),
                    error=None,
                )
            trade = update.trade
            score = self._scores_by_trade.get(trade.trade_id)
            if score is None:
                raise RuntimeError("Scalper score is missing for active shadow trade.")
            self._trade_updates += 1
            finished = trade.terminal
            event_type = (
                ShadowOrchestratorEventType.TRADE_FINISHED
                if finished
                else ShadowOrchestratorEventType.POSITION_UPDATED
            )
            message = (
                "🏁 TRADE_FINISHED: virtual trade reached a terminal state."
                if finished
                else "📈 POSITION_UPDATED: virtual position state advanced."
            )
            event = self._emit(
                event_type,
                occurred_at=bar.closed_at,
                symbol=trade.symbol,
                trade_id=trade.trade_id,
                semantic_key=(
                    f"{trade.stage.value}|{trade.bars_held}|"
                    f"{trade.realized_r:.12g}|{bar.closed_at.isoformat()}"
                ),
                message=message,
            )
            record = build_shadow_trade_record(
                trade,
                score,
                recorded_at=bar.closed_at,
                events=update.events,
                reasons=update.reasons,
            )
            if not self._journal.append(record):
                self._duplicates_suppressed += 1
            market_state, setup_context = self._context_by_trade.get(
                trade.trade_id,
                (None, None),
            )
            self._persist_decision(
                (
                    ShadowDecisionEventType.TRADE_FINISHED
                    if finished
                    else ShadowDecisionEventType.TRADE_UPDATED
                ),
                timestamp=bar.closed_at,
                symbol=trade.symbol,
                score=score,
                market_state=market_state,
                setup_context=setup_context,
                reasons=update.reasons,
                warnings=score.warnings,
            )
            if finished:
                self._trades_closed += 1
                self._scores_by_trade.pop(trade.trade_id, None)
                self._context_by_trade.pop(trade.trade_id, None)
            return ShadowBarResult(
                symbol=bar.symbol,
                trade=trade,
                events=(event,) if event is not None else (),
                error=None,
            )
        except (KeyError, OSError, RuntimeError, ValueError) as exc:
            self._errors += 1
            return ShadowBarResult(
                symbol=bar.symbol,
                trade=None,
                events=(),
                error=str(exc),
            )

    def _target(self, symbol: str) -> ScalperTarget | None:
        normalized = _symbol(symbol)
        return next(
            (
                target
                for target in self._bridge.all_targets()
                if target.symbol == normalized
            ),
            None,
        )

    def _persist_decision(
        self,
        event_type: ShadowDecisionEventType,
        *,
        timestamp: datetime,
        symbol: str,
        reasons: tuple[str, ...],
        score: ScalperScore | None = None,
        market_state: str | None = None,
        setup_context: str | None = None,
        warnings: tuple[str, ...] = (),
    ) -> None:
        if self._decision_journal is None:
            return
        self._decision_journal.append(
            ShadowDecisionRecord(
                timestamp=timestamp,
                symbol=symbol,
                event_type=event_type,
                score=None if score is None else score.total_score,
                score_components=(
                    None if score is None else score.component_scores
                ),
                market_state=market_state,
                setup_context=setup_context,
                reasons=reasons,
                warnings=warnings,
            )
        )

    def _emit(
        self,
        event_type: ShadowOrchestratorEventType,
        *,
        occurred_at: datetime,
        symbol: str,
        trade_id: str | None,
        semantic_key: str,
        message: str,
    ) -> ShadowOrchestratorEvent | None:
        normalized_at = _utc(occurred_at)
        normalized_symbol = _symbol(symbol)
        fingerprint = "|".join(
            (
                event_type.value,
                normalized_symbol,
                trade_id or "NONE",
                semantic_key,
            )
        )
        if fingerprint in self._event_fingerprints:
            return None
        digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:20]
        self._sequence += 1
        event = ShadowOrchestratorEvent(
            event_id=f"shadow-orchestrator-{digest}",
            sequence=self._sequence,
            event_type=event_type,
            occurred_at=normalized_at,
            symbol=normalized_symbol,
            trade_id=trade_id,
            message=message,
        )
        self._event_fingerprints.add(fingerprint)
        self._events.append(event)
        return event


def _symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("Shadow orchestrator symbol cannot be empty.")
    return normalized


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Shadow orchestrator timestamp must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError("Shadow orchestrator timestamp must be timezone-aware.")
    return value.astimezone(UTC)
