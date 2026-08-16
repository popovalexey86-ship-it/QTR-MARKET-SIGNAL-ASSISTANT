from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import Lock

from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperDecision,
    ScalperScore,
    ScalperScoringEngine,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowDecisionEngine,
    ShadowPriceBar,
    ShadowTrade,
    ShadowTradeEvent,
    ShadowTradeEventType,
    ShadowTradeOutcome,
    ShadowTradeStage,
    shadow_outcome,
)
from market_signal_assistant.qtr_micro_scalper.snapshot import (
    MicrostructureSnapshotBundle,
    SnapshotReadiness,
)


class ShadowRuntimeEventType(StrEnum):
    ENTRY_CREATED = "ENTRY_CREATED"
    TP1_REACHED = "TP1_REACHED"
    TP2_REACHED = "TP2_REACHED"
    STOPPED = "STOPPED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class ShadowRuntimeConfig:
    max_concurrent_trades: int = 3
    cooldown_seconds: int = 300

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_concurrent_trades, bool)
            or self.max_concurrent_trades < 1
        ):
            raise ValueError("Maximum concurrent shadow trades must be positive.")
        if isinstance(self.cooldown_seconds, bool) or self.cooldown_seconds < 0:
            raise ValueError("Shadow runtime cooldown cannot be negative.")


@dataclass(frozen=True, slots=True)
class ShadowRuntimeEvent:
    event_id: str
    event_type: ShadowRuntimeEventType
    occurred_at: datetime
    trade_id: str
    symbol: str
    stage: ShadowTradeStage
    price: float | None
    realized_r: float
    message: str

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.trade_id.strip():
            raise ValueError("Shadow runtime event identity cannot be empty.")
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Shadow runtime event symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at))
        if self.price is not None and (
            not math.isfinite(self.price) or self.price <= 0
        ):
            raise ValueError("Shadow runtime event price must be positive.")
        if not math.isfinite(self.realized_r):
            raise ValueError("Shadow runtime event realized R must be finite.")
        if not self.message.strip():
            raise ValueError("Shadow runtime event requires a message.")


@dataclass(frozen=True, slots=True)
class ShadowRuntimeDecision:
    accepted: bool
    score: ScalperScore | None
    trade: ShadowTrade | None
    events: tuple[ShadowRuntimeEvent, ...]
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.reasons:
            raise ValueError("Shadow runtime decision requires reasons.")
        if self.accepted != (self.trade is not None):
            raise ValueError("Shadow runtime decision acceptance is inconsistent.")


@dataclass(frozen=True, slots=True)
class ShadowRuntimeUpdate:
    trade: ShadowTrade | None
    outcome: ShadowTradeOutcome | None
    events: tuple[ShadowRuntimeEvent, ...]
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.reasons:
            raise ValueError("Shadow runtime update requires reasons.")


class ShadowRuntime:
    """Offline-only orchestrator for deterministic virtual scalper trades."""

    def __init__(
        self,
        config: ShadowRuntimeConfig | None = None,
        *,
        scoring_engine: ScalperScoringEngine | None = None,
        decision_engine: ShadowDecisionEngine | None = None,
    ) -> None:
        self._config = config or ShadowRuntimeConfig()
        self._scoring = scoring_engine or ScalperScoringEngine()
        self._decisions = decision_engine or ShadowDecisionEngine()
        self._active: dict[str, ShadowTrade] = {}
        self._completed: dict[str, ShadowTrade] = {}
        self._seen_trade_ids: set[str] = set()
        self._last_closed_at: dict[str, datetime] = {}
        self._last_snapshot_at: dict[str, datetime] = {}
        self._journal: list[ShadowRuntimeEvent] = []
        self._lock = Lock()

    def process_snapshot(
        self,
        bundle: MicrostructureSnapshotBundle,
    ) -> ShadowRuntimeDecision:
        with self._lock:
            return self._process_snapshot_locked(bundle)

    def process_bar(self, bar: ShadowPriceBar) -> ShadowRuntimeUpdate:
        with self._lock:
            trade = self._active.get(bar.symbol)
            if trade is None:
                return ShadowRuntimeUpdate(
                    trade=None,
                    outcome=None,
                    events=(),
                    reasons=("No active shadow trade for symbol.",),
                )
            previous_event_count = len(trade.events)
            updated = self._decisions.process_bar(trade, bar)
            runtime_events = tuple(
                _runtime_event(updated, event)
                for event in updated.events[previous_event_count:]
                if event.event_type is not ShadowTradeEventType.ENTRY
            )
            self._journal.extend(runtime_events)
            if updated.terminal:
                self._active.pop(updated.symbol, None)
                self._completed[updated.trade_id] = updated
                if updated.closed_at is None:
                    raise RuntimeError("Terminal shadow trade has no close timestamp.")
                self._last_closed_at[updated.symbol] = updated.closed_at
                reason = "Shadow trade reached a terminal state."
            else:
                self._active[updated.symbol] = updated
                reason = f"Shadow trade advanced to {updated.stage.value}."
            return ShadowRuntimeUpdate(
                trade=updated,
                outcome=shadow_outcome(updated),
                events=runtime_events,
                reasons=(reason,),
            )

    def active_trades(self) -> tuple[ShadowTrade, ...]:
        with self._lock:
            return tuple(sorted(self._active.values(), key=lambda item: item.trade_id))

    def completed_trades(self) -> tuple[ShadowTrade, ...]:
        with self._lock:
            return tuple(
                sorted(self._completed.values(), key=lambda item: item.trade_id)
            )

    def journal(self) -> tuple[ShadowRuntimeEvent, ...]:
        with self._lock:
            return tuple(self._journal)

    def _process_snapshot_locked(
        self,
        bundle: MicrostructureSnapshotBundle,
    ) -> ShadowRuntimeDecision:
        last_snapshot = self._last_snapshot_at.get(bundle.symbol)
        if last_snapshot is not None and bundle.generated_at < last_snapshot:
            raise ValueError("Shadow runtime snapshots must be chronological.")
        self._last_snapshot_at[bundle.symbol] = bundle.generated_at

        if bundle.readiness is not SnapshotReadiness.READY:
            return _rejected(None, "Microstructure snapshot is not READY.")
        if (
            bundle.market_state is None
            or bundle.liquidity is None
            or bundle.trade_flow is None
            or bundle.orderbook is None
            or bundle.setup_context is None
        ):
            return _rejected(None, "Microstructure snapshot is incomplete.")

        score = self._scoring.score(
            bundle.market_state,
            bundle.liquidity,
            bundle.trade_flow,
            bundle.orderbook,
            bundle.setup_context,
            bundle.setup_context.risk,
        )
        if score.decision not in {
            ScalperDecision.STRONG_SCALP,
            ScalperDecision.SCALP,
        }:
            return _rejected(
                score,
                f"Scalper score decision is {score.decision.value}.",
            )
        if bundle.symbol in self._active:
            return _rejected(score, "An active shadow trade already exists for symbol.")
        closed_at = self._last_closed_at.get(bundle.symbol)
        if closed_at is not None:
            cooldown_ends = closed_at + timedelta(seconds=self._config.cooldown_seconds)
            if bundle.generated_at < cooldown_ends:
                return _rejected(score, "Shadow trade cooldown is active.")
        if len(self._active) >= self._config.max_concurrent_trades:
            return _rejected(score, "Maximum concurrent shadow trades reached.")

        decision = self._decisions.create_trade(bundle.setup_context)
        if decision.trade is None:
            return ShadowRuntimeDecision(
                accepted=False,
                score=score,
                trade=None,
                events=(),
                reasons=decision.reasons,
            )
        trade = decision.trade
        if trade.trade_id in self._seen_trade_ids:
            return _rejected(score, "Duplicate shadow trade id suppressed.")

        event = _entry_created_event(trade)
        self._seen_trade_ids.add(trade.trade_id)
        self._active[trade.symbol] = trade
        self._journal.append(event)
        return ShadowRuntimeDecision(
            accepted=True,
            score=score,
            trade=trade,
            events=(event,),
            reasons=decision.reasons,
        )


def _rejected(score: ScalperScore | None, reason: str) -> ShadowRuntimeDecision:
    return ShadowRuntimeDecision(
        accepted=False,
        score=score,
        trade=None,
        events=(),
        reasons=(reason,),
    )


def _entry_created_event(trade: ShadowTrade) -> ShadowRuntimeEvent:
    return _make_event(
        event_type=ShadowRuntimeEventType.ENTRY_CREATED,
        occurred_at=trade.planned_at,
        trade=trade,
        price=trade.entry_price,
        realized_r=0.0,
        message="🔥 Virtual entry plan created.",
    )


def _runtime_event(
    trade: ShadowTrade,
    event: ShadowTradeEvent,
) -> ShadowRuntimeEvent:
    event_type, stage, message = {
        ShadowTradeEventType.TP1: (
            ShadowRuntimeEventType.TP1_REACHED,
            ShadowTradeStage.TP1_HIT,
            "🎯 Virtual TP1 reached.",
        ),
        ShadowTradeEventType.TP2: (
            ShadowRuntimeEventType.TP2_REACHED,
            ShadowTradeStage.CLOSED,
            "🚀 Virtual TP2 reached.",
        ),
        ShadowTradeEventType.STOP: (
            ShadowRuntimeEventType.STOPPED,
            ShadowTradeStage.CLOSED,
            "🛑 Virtual stop reached.",
        ),
        ShadowTradeEventType.TIME_EXIT: (
            ShadowRuntimeEventType.EXPIRED,
            ShadowTradeStage.CLOSED,
            "⏱ Virtual holding period expired.",
        ),
        ShadowTradeEventType.EXPIRED: (
            ShadowRuntimeEventType.EXPIRED,
            ShadowTradeStage.EXPIRED,
            "⏱ Virtual entry plan expired.",
        ),
    }[event.event_type]
    return _make_event(
        event_type=event_type,
        occurred_at=event.occurred_at,
        trade=trade,
        stage=stage,
        price=event.price,
        realized_r=event.realized_r,
        message=message,
    )


def _make_event(
    *,
    event_type: ShadowRuntimeEventType,
    occurred_at: datetime,
    trade: ShadowTrade,
    stage: ShadowTradeStage | None = None,
    price: float | None,
    realized_r: float,
    message: str,
) -> ShadowRuntimeEvent:
    source = "|".join(
        (
            trade.trade_id,
            event_type.value,
            occurred_at.isoformat(),
            f"{price:.12g}" if price is not None else "NONE",
        )
    )
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()[:20]
    return ShadowRuntimeEvent(
        event_id=f"shadow-runtime-{digest}",
        event_type=event_type,
        occurred_at=occurred_at,
        trade_id=trade.trade_id,
        symbol=trade.symbol,
        stage=stage or trade.stage,
        price=price,
        realized_r=realized_r,
        message=message,
    )


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Shadow runtime timestamp must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError("Shadow runtime timestamp must be timezone-aware.")
    return value.astimezone(UTC)
