from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import Lock

from market_signal_assistant.qtr_micro_scalper.setup_context import (
    ShadowDirection,
    ShadowOpportunity,
    ShadowOpportunityDecision,
)


class ShadowTradeStage(StrEnum):
    WAITING_ENTRY = "WAITING_ENTRY"
    OPEN = "OPEN"
    TP1_HIT = "TP1_HIT"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"


class ShadowTradeEventType(StrEnum):
    ENTRY = "ENTRY"
    TP1 = "TP1"
    TP2 = "TP2"
    STOP = "STOP"
    TIME_EXIT = "TIME_EXIT"
    EXPIRED = "EXPIRED"


class ShadowOutcomeStatus(StrEnum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    WIN = "WIN"
    LOSS = "LOSS"
    BREAKEVEN = "BREAKEVEN"
    NOT_TRIGGERED = "NOT_TRIGGERED"


@dataclass(frozen=True, slots=True)
class ShadowDecisionConfig:
    stop_atr_buffer: float = 0.10
    tp1_r: float = 1.0
    tp2_r: float = 2.0
    tp1_close_fraction: float = 0.50
    entry_valid_for_seconds: int = 60
    maximum_holding_bars: int = 30
    move_stop_to_breakeven_after_tp1: bool = True

    def __post_init__(self) -> None:
        for name, value in (
            ("stop ATR buffer", self.stop_atr_buffer),
            ("TP1 R", self.tp1_r),
            ("TP2 R", self.tp2_r),
            ("TP1 close fraction", self.tp1_close_fraction),
        ):
            if not _positive(value):
                raise ValueError(f"Shadow {name} must be positive.")
        if self.tp1_r >= self.tp2_r:
            raise ValueError("Shadow TP1 must be below TP2.")
        if not 0.0 < self.tp1_close_fraction < 1.0:
            raise ValueError("Shadow TP1 close fraction must be between 0 and 1.")
        if (
            isinstance(self.entry_valid_for_seconds, bool)
            or self.entry_valid_for_seconds < 1
        ):
            raise ValueError("Shadow entry validity must be positive.")
        if (
            isinstance(self.maximum_holding_bars, bool)
            or self.maximum_holding_bars < 1
        ):
            raise ValueError("Shadow maximum holding bars must be positive.")
        if not isinstance(self.move_stop_to_breakeven_after_tp1, bool):
            raise ValueError("Shadow breakeven setting must be boolean.")


@dataclass(frozen=True, slots=True)
class ShadowTradeLevels:
    entry_price: float
    initial_stop: float
    tp1_price: float
    tp2_price: float
    risk_per_unit: float

    def __post_init__(self) -> None:
        for value in (
            self.entry_price,
            self.initial_stop,
            self.tp1_price,
            self.tp2_price,
            self.risk_per_unit,
        ):
            if not _positive(value):
                raise ValueError("Shadow trade levels must be positive.")


@dataclass(frozen=True, slots=True)
class ShadowPriceBar:
    symbol: str
    opened_at: datetime
    closed_at: datetime
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Shadow price bar symbol cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        opened_at = _utc("Shadow bar opened_at", self.opened_at)
        closed_at = _utc("Shadow bar closed_at", self.closed_at)
        if opened_at >= closed_at:
            raise ValueError("Shadow price bar must have positive duration.")
        object.__setattr__(self, "opened_at", opened_at)
        object.__setattr__(self, "closed_at", closed_at)
        for name, value in (
            ("open", self.open),
            ("high", self.high),
            ("low", self.low),
            ("close", self.close),
        ):
            if not _positive(value):
                raise ValueError(f"Shadow bar {name} must be positive.")
        if self.low > min(self.open, self.close) or self.high < max(
            self.open,
            self.close,
        ):
            raise ValueError("Shadow price bar OHLC values are inconsistent.")
        if self.low > self.high:
            raise ValueError("Shadow price bar low cannot exceed high.")


@dataclass(frozen=True, slots=True)
class ShadowTradeEvent:
    event_type: ShadowTradeEventType
    occurred_at: datetime
    price: float | None
    quantity_fraction: float
    realized_r: float
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "occurred_at",
            _utc("Shadow event occurred_at", self.occurred_at),
        )
        if self.price is not None and not _positive(self.price):
            raise ValueError("Shadow event price must be positive.")
        if not _finite(self.quantity_fraction) or not (
            0.0 <= self.quantity_fraction <= 1.0
        ):
            raise ValueError("Shadow event quantity fraction must be between 0 and 1.")
        if not _finite(self.realized_r):
            raise ValueError("Shadow event realized R must be finite.")
        if not self.reason.strip():
            raise ValueError("Shadow event requires a reason.")


@dataclass(frozen=True, slots=True)
class ShadowTrade:
    trade_id: str
    symbol: str
    direction: ShadowDirection
    planned_at: datetime
    entry_expires_at: datetime
    opportunity_score: float
    confidence: float
    stage: ShadowTradeStage
    entry_price: float
    initial_stop: float
    current_stop: float
    tp1_price: float
    tp2_price: float
    risk_per_unit: float
    remaining_fraction: float = 1.0
    realized_r: float = 0.0
    entry_at: datetime | None = None
    closed_at: datetime | None = None
    last_processed_at: datetime | None = None
    bars_held: int = 0
    tp1_hit: bool = False
    tp2_hit: bool = False
    max_favorable_excursion_r: float = 0.0
    max_adverse_excursion_r: float = 0.0
    events: tuple[ShadowTradeEvent, ...] = ()

    def __post_init__(self) -> None:
        if not self.trade_id.strip() or not self.symbol.strip():
            raise ValueError("Shadow trade identity cannot be empty.")
        if self.direction is ShadowDirection.NEUTRAL:
            raise ValueError("Shadow trade direction must be LONG or SHORT.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        planned_at = _utc("Shadow trade planned_at", self.planned_at)
        expires_at = _utc("Shadow trade entry_expires_at", self.entry_expires_at)
        if expires_at <= planned_at:
            raise ValueError("Shadow trade expiry must follow planning time.")
        object.__setattr__(self, "planned_at", planned_at)
        object.__setattr__(self, "entry_expires_at", expires_at)
        for name in ("entry_at", "closed_at", "last_processed_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(f"Shadow trade {name}", value))
        for value in (
            self.entry_price,
            self.initial_stop,
            self.current_stop,
            self.tp1_price,
            self.tp2_price,
            self.risk_per_unit,
        ):
            if not _positive(value):
                raise ValueError("Shadow trade price levels must be positive.")
        if not 0.0 <= self.opportunity_score <= 100.0:
            raise ValueError("Shadow trade opportunity score is outside 0..100.")
        if not 0.0 <= self.confidence <= 100.0:
            raise ValueError("Shadow trade confidence is outside 0..100.")
        if not _finite(self.remaining_fraction) or not (
            0.0 <= self.remaining_fraction <= 1.0
        ):
            raise ValueError("Shadow trade remaining fraction is outside 0..1.")
        for value in (
            self.realized_r,
            self.max_favorable_excursion_r,
            self.max_adverse_excursion_r,
        ):
            if not _finite(value):
                raise ValueError("Shadow trade R metrics must be finite.")
        if self.max_favorable_excursion_r < 0 or self.max_adverse_excursion_r < 0:
            raise ValueError("Shadow trade excursions cannot be negative.")
        if isinstance(self.bars_held, bool) or self.bars_held < 0:
            raise ValueError("Shadow trade bars held cannot be negative.")
        object.__setattr__(self, "events", tuple(self.events))
        if self.terminal and self.closed_at is None:
            raise ValueError("Terminal shadow trade requires closed_at.")
        if self.stage is ShadowTradeStage.CLOSED and self.remaining_fraction != 0:
            raise ValueError("Closed shadow trade cannot retain quantity.")
        if (
            self.stage in {ShadowTradeStage.OPEN, ShadowTradeStage.TP1_HIT}
            and self.entry_at is None
        ):
            raise ValueError("Open shadow trade requires entry_at.")
        if self.tp2_hit and not self.tp1_hit:
            raise ValueError("Shadow TP2 cannot precede TP1.")

    @property
    def terminal(self) -> bool:
        return self.stage in {ShadowTradeStage.CLOSED, ShadowTradeStage.EXPIRED}


@dataclass(frozen=True, slots=True)
class ShadowTradeDecision:
    trade: ShadowTrade | None
    reasons: tuple[str, ...]

    @property
    def accepted(self) -> bool:
        return self.trade is not None


@dataclass(frozen=True, slots=True)
class ShadowTradeOutcome:
    trade_id: str
    symbol: str
    direction: ShadowDirection
    status: ShadowOutcomeStatus
    stage: ShadowTradeStage
    entry_at: datetime | None
    exited_at: datetime | None
    realized_r: float
    max_favorable_excursion_r: float
    max_adverse_excursion_r: float
    bars_held: int
    tp1_hit: bool
    tp2_hit: bool
    events: tuple[ShadowTradeEvent, ...]


@dataclass(frozen=True, slots=True)
class ShadowSimulationResult:
    decision: ShadowTradeDecision
    trade: ShadowTrade | None
    outcome: ShadowTradeOutcome | None


@dataclass(frozen=True, slots=True)
class ShadowPerformanceSummary:
    total_trades: int
    wins: int
    losses: int
    breakeven: int
    not_triggered: int
    total_realized_r: float
    average_realized_r: float
    win_rate: float


class ShadowDecisionEngine:
    """Pure virtual trade planner and deterministic OHLC lifecycle engine."""

    def __init__(self, config: ShadowDecisionConfig | None = None) -> None:
        self._config = config or ShadowDecisionConfig()

    def create_trade(self, opportunity: ShadowOpportunity) -> ShadowTradeDecision:
        if opportunity.decision is not ShadowOpportunityDecision.SHADOW_CANDIDATE:
            return ShadowTradeDecision(
                trade=None,
                reasons=("Opportunity не классифицирован как SHADOW_CANDIDATE.",),
            )
        if opportunity.direction is ShadowDirection.NEUTRAL:
            return ShadowTradeDecision(
                trade=None,
                reasons=("Нейтральный Price Context не создаёт virtual trade.",),
            )
        levels = calculate_shadow_levels(opportunity, config=self._config)
        trade_id = _trade_id(opportunity, levels)
        trade = ShadowTrade(
            trade_id=trade_id,
            symbol=opportunity.symbol,
            direction=opportunity.direction,
            planned_at=opportunity.assessed_at,
            entry_expires_at=opportunity.assessed_at
            + timedelta(seconds=self._config.entry_valid_for_seconds),
            opportunity_score=opportunity.opportunity_score,
            confidence=opportunity.confidence,
            stage=ShadowTradeStage.WAITING_ENTRY,
            entry_price=levels.entry_price,
            initial_stop=levels.initial_stop,
            current_stop=levels.initial_stop,
            tp1_price=levels.tp1_price,
            tp2_price=levels.tp2_price,
            risk_per_unit=levels.risk_per_unit,
        )
        return ShadowTradeDecision(
            trade=trade,
            reasons=("Virtual trade plan создан без отправки ордера.",),
        )

    def process_bar(self, trade: ShadowTrade, bar: ShadowPriceBar) -> ShadowTrade:
        _validate_bar(trade, bar)
        if trade.terminal:
            return trade
        if (
            trade.last_processed_at is not None
            and bar.closed_at <= trade.last_processed_at
        ):
            raise ValueError("Shadow bars must be processed in chronological order.")

        working = replace(trade, last_processed_at=bar.closed_at)
        if working.stage is ShadowTradeStage.WAITING_ENTRY:
            if bar.closed_at > working.entry_expires_at:
                return _expire(working, bar.closed_at)
            if not _entry_touched(working, bar):
                return working
            working = _open_trade(working, bar.closed_at)

        working = _update_excursions(working, bar)
        working = replace(working, bars_held=working.bars_held + 1)
        return _manage_open_trade(working, bar, self._config)


class ShadowOutcomeTracker:
    """In-memory, duplicate-safe aggregate for terminal offline outcomes."""

    def __init__(self) -> None:
        self._outcomes: dict[str, ShadowTradeOutcome] = {}
        self._lock = Lock()

    def record(self, outcome: ShadowTradeOutcome) -> bool:
        if outcome.status in {ShadowOutcomeStatus.PENDING, ShadowOutcomeStatus.OPEN}:
            raise ValueError("Only terminal shadow outcomes can be recorded.")
        with self._lock:
            if outcome.trade_id in self._outcomes:
                return False
            self._outcomes[outcome.trade_id] = outcome
            return True

    def summary(self) -> ShadowPerformanceSummary:
        with self._lock:
            outcomes = tuple(self._outcomes.values())
        wins = sum(item.status is ShadowOutcomeStatus.WIN for item in outcomes)
        losses = sum(item.status is ShadowOutcomeStatus.LOSS for item in outcomes)
        breakeven = sum(
            item.status is ShadowOutcomeStatus.BREAKEVEN for item in outcomes
        )
        not_triggered = sum(
            item.status is ShadowOutcomeStatus.NOT_TRIGGERED for item in outcomes
        )
        realized = sum(item.realized_r for item in outcomes)
        closed_count = wins + losses + breakeven
        return ShadowPerformanceSummary(
            total_trades=len(outcomes),
            wins=wins,
            losses=losses,
            breakeven=breakeven,
            not_triggered=not_triggered,
            total_realized_r=realized,
            average_realized_r=realized / len(outcomes) if outcomes else 0.0,
            win_rate=wins / closed_count * 100 if closed_count else 0.0,
        )


def calculate_shadow_levels(
    opportunity: ShadowOpportunity,
    *,
    config: ShadowDecisionConfig | None = None,
) -> ShadowTradeLevels:
    resolved = config or ShadowDecisionConfig()
    price = opportunity.price_context
    if opportunity.direction is ShadowDirection.LONG:
        entry = max(price.market_price, price.trigger_price)
        stop = price.invalidation_price - price.atr * resolved.stop_atr_buffer
        if stop <= 0 or stop >= entry:
            raise ValueError("Shadow LONG stop must be positive and below entry.")
        risk = entry - stop
        tp1 = entry + risk * resolved.tp1_r
        tp2 = entry + risk * resolved.tp2_r
    elif opportunity.direction is ShadowDirection.SHORT:
        entry = min(price.market_price, price.trigger_price)
        stop = price.invalidation_price + price.atr * resolved.stop_atr_buffer
        if stop <= entry:
            raise ValueError("Shadow SHORT stop must be above entry.")
        risk = stop - entry
        tp1 = entry - risk * resolved.tp1_r
        tp2 = entry - risk * resolved.tp2_r
        if tp2 <= 0:
            raise ValueError("Shadow SHORT TP2 must remain positive.")
    else:
        raise ValueError("Neutral opportunity cannot produce shadow trade levels.")
    return ShadowTradeLevels(
        entry_price=entry,
        initial_stop=stop,
        tp1_price=tp1,
        tp2_price=tp2,
        risk_per_unit=risk,
    )


def shadow_outcome(trade: ShadowTrade) -> ShadowTradeOutcome:
    if trade.stage is ShadowTradeStage.EXPIRED:
        status = ShadowOutcomeStatus.NOT_TRIGGERED
    elif trade.stage is ShadowTradeStage.WAITING_ENTRY:
        status = ShadowOutcomeStatus.PENDING
    elif trade.stage in {ShadowTradeStage.OPEN, ShadowTradeStage.TP1_HIT}:
        status = ShadowOutcomeStatus.OPEN
    elif trade.realized_r > 1e-12:
        status = ShadowOutcomeStatus.WIN
    elif trade.realized_r < -1e-12:
        status = ShadowOutcomeStatus.LOSS
    else:
        status = ShadowOutcomeStatus.BREAKEVEN
    return ShadowTradeOutcome(
        trade_id=trade.trade_id,
        symbol=trade.symbol,
        direction=trade.direction,
        status=status,
        stage=trade.stage,
        entry_at=trade.entry_at,
        exited_at=trade.closed_at,
        realized_r=trade.realized_r,
        max_favorable_excursion_r=trade.max_favorable_excursion_r,
        max_adverse_excursion_r=trade.max_adverse_excursion_r,
        bars_held=trade.bars_held,
        tp1_hit=trade.tp1_hit,
        tp2_hit=trade.tp2_hit,
        events=trade.events,
    )


def simulate_shadow_trade(
    opportunity: ShadowOpportunity,
    bars: tuple[ShadowPriceBar, ...],
    *,
    config: ShadowDecisionConfig | None = None,
) -> ShadowSimulationResult:
    engine = ShadowDecisionEngine(config)
    decision = engine.create_trade(opportunity)
    if decision.trade is None:
        return ShadowSimulationResult(decision=decision, trade=None, outcome=None)
    trade = decision.trade
    ordered_bars = tuple(
        sorted(bars, key=lambda item: (item.opened_at, item.closed_at))
    )
    for bar in ordered_bars:
        trade = engine.process_bar(trade, bar)
        if trade.terminal:
            break
    return ShadowSimulationResult(
        decision=decision,
        trade=trade,
        outcome=shadow_outcome(trade),
    )


def _manage_open_trade(
    trade: ShadowTrade,
    bar: ShadowPriceBar,
    config: ShadowDecisionConfig,
) -> ShadowTrade:
    if _stop_touched(trade, bar):
        return _close_fraction(
            trade,
            event_type=ShadowTradeEventType.STOP,
            occurred_at=bar.closed_at,
            price=trade.current_stop,
            fraction=trade.remaining_fraction,
            close_trade=True,
            reason="STOP_FIRST: virtual stop touched inside OHLC bar.",
        )

    working = trade
    if not working.tp1_hit and _tp1_touched(working, bar):
        fraction = min(config.tp1_close_fraction, working.remaining_fraction)
        working = _close_fraction(
            working,
            event_type=ShadowTradeEventType.TP1,
            occurred_at=bar.closed_at,
            price=working.tp1_price,
            fraction=fraction,
            close_trade=False,
            reason="Virtual TP1 touched.",
        )
        working = replace(
            working,
            stage=ShadowTradeStage.TP1_HIT,
            tp1_hit=True,
            current_stop=(
                working.entry_price
                if config.move_stop_to_breakeven_after_tp1
                else working.current_stop
            ),
        )

    if _tp2_touched(working, bar):
        working = _close_fraction(
            working,
            event_type=ShadowTradeEventType.TP2,
            occurred_at=bar.closed_at,
            price=working.tp2_price,
            fraction=working.remaining_fraction,
            close_trade=True,
            reason="Virtual TP2 touched.",
        )
        return replace(working, tp2_hit=True)

    if working.bars_held >= config.maximum_holding_bars:
        return _close_fraction(
            working,
            event_type=ShadowTradeEventType.TIME_EXIT,
            occurred_at=bar.closed_at,
            price=bar.close,
            fraction=working.remaining_fraction,
            close_trade=True,
            reason="Virtual maximum holding bars reached.",
        )
    return working


def _open_trade(trade: ShadowTrade, occurred_at: datetime) -> ShadowTrade:
    event = ShadowTradeEvent(
        event_type=ShadowTradeEventType.ENTRY,
        occurred_at=occurred_at,
        price=trade.entry_price,
        quantity_fraction=1.0,
        realized_r=0.0,
        reason="Virtual entry level touched.",
    )
    return replace(
        trade,
        stage=ShadowTradeStage.OPEN,
        entry_at=occurred_at,
        events=(*trade.events, event),
    )


def _expire(trade: ShadowTrade, occurred_at: datetime) -> ShadowTrade:
    event = ShadowTradeEvent(
        event_type=ShadowTradeEventType.EXPIRED,
        occurred_at=occurred_at,
        price=None,
        quantity_fraction=0.0,
        realized_r=0.0,
        reason="Virtual entry validity expired before trigger.",
    )
    return replace(
        trade,
        stage=ShadowTradeStage.EXPIRED,
        closed_at=occurred_at,
        events=(*trade.events, event),
    )


def _close_fraction(
    trade: ShadowTrade,
    *,
    event_type: ShadowTradeEventType,
    occurred_at: datetime,
    price: float,
    fraction: float,
    close_trade: bool,
    reason: str,
) -> ShadowTrade:
    realized = _price_r(trade, price) * fraction
    remaining = max(0.0, trade.remaining_fraction - fraction)
    event = ShadowTradeEvent(
        event_type=event_type,
        occurred_at=occurred_at,
        price=price,
        quantity_fraction=fraction,
        realized_r=realized,
        reason=reason,
    )
    return replace(
        trade,
        stage=ShadowTradeStage.CLOSED if close_trade else trade.stage,
        remaining_fraction=0.0 if close_trade else remaining,
        realized_r=trade.realized_r + realized,
        closed_at=occurred_at if close_trade else trade.closed_at,
        events=(*trade.events, event),
    )


def _update_excursions(trade: ShadowTrade, bar: ShadowPriceBar) -> ShadowTrade:
    if trade.direction is ShadowDirection.LONG:
        favorable = (bar.high - trade.entry_price) / trade.risk_per_unit
        adverse = (trade.entry_price - bar.low) / trade.risk_per_unit
    else:
        favorable = (trade.entry_price - bar.low) / trade.risk_per_unit
        adverse = (bar.high - trade.entry_price) / trade.risk_per_unit
    return replace(
        trade,
        max_favorable_excursion_r=max(trade.max_favorable_excursion_r, favorable, 0.0),
        max_adverse_excursion_r=max(trade.max_adverse_excursion_r, adverse, 0.0),
    )


def _price_r(trade: ShadowTrade, price: float) -> float:
    direction = 1.0 if trade.direction is ShadowDirection.LONG else -1.0
    return (price - trade.entry_price) / trade.risk_per_unit * direction


def _entry_touched(trade: ShadowTrade, bar: ShadowPriceBar) -> bool:
    return bar.low <= trade.entry_price <= bar.high


def _stop_touched(trade: ShadowTrade, bar: ShadowPriceBar) -> bool:
    if trade.direction is ShadowDirection.LONG:
        return bar.low <= trade.current_stop
    return bar.high >= trade.current_stop


def _tp1_touched(trade: ShadowTrade, bar: ShadowPriceBar) -> bool:
    if trade.direction is ShadowDirection.LONG:
        return bar.high >= trade.tp1_price
    return bar.low <= trade.tp1_price


def _tp2_touched(trade: ShadowTrade, bar: ShadowPriceBar) -> bool:
    if trade.direction is ShadowDirection.LONG:
        return bar.high >= trade.tp2_price
    return bar.low <= trade.tp2_price


def _validate_bar(trade: ShadowTrade, bar: ShadowPriceBar) -> None:
    if trade.symbol != bar.symbol:
        raise ValueError("Shadow trade and bar must use the same symbol.")
    if bar.closed_at <= trade.planned_at:
        raise ValueError("Shadow bar must close after trade planning time.")


def _trade_id(opportunity: ShadowOpportunity, levels: ShadowTradeLevels) -> str:
    source = "|".join(
        (
            opportunity.symbol,
            opportunity.direction.value,
            opportunity.assessed_at.isoformat(),
            f"{levels.entry_price:.12g}",
            f"{levels.initial_stop:.12g}",
        )
    )
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()[:20]
    return f"shadow-{digest}"


def _utc(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _positive(value: float) -> bool:
    return _finite(value) and value > 0
