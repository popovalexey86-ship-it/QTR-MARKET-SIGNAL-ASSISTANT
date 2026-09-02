from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from market_signal_assistant.qtr_micro_scalper.data.models import (
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import OrderBookState
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowAccumulator
from market_signal_assistant.qtr_micro_scalper_v3.engine import CashScalperEngine
from market_signal_assistant.qtr_micro_scalper_v3.models import (
    ImpulseDirection,
    ImpulseSnapshot,
    SweepDirection,
    V3PriceObservation,
    V3ShadowTrade,
    V3TradeStage,
)
from market_signal_assistant.qtr_micro_scalper_v3.telemetry import (
    ForwardOutcomeTracker,
    JsonlTelemetryJournal,
    build_entry_record,
    build_trade_record,
)


@dataclass(frozen=True, slots=True)
class V3RuntimeResult:
    entry_created: bool
    trade: V3ShadowTrade | None
    blocking_reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _ObservedTrade:
    at: datetime
    price: float
    notional: float
    side: TradeSide


@dataclass(slots=True)
class _ImpulseState:
    direction: ImpulseDirection
    started_at: datetime
    start_price: float
    impulse_id: str
    last_at: datetime


class V3FeatureBuilder:
    """Bounded causal adapter from reusable V2 public data primitives to V3."""

    def __init__(
        self,
        trade_flow: TradeFlowAccumulator,
        orderbook_provider: Callable[[str], OrderBookState],
        *,
        retention: timedelta = timedelta(seconds=20),
    ) -> None:
        self._trade_flow = trade_flow
        self._orderbook_provider = orderbook_provider
        self._retention = retention
        self._events: dict[str, deque[_ObservedTrade]] = {}
        self._impulses: dict[str, _ImpulseState] = {}

    def observe_trade(self, event: PublicTradeEvent) -> ImpulseSnapshot | None:
        events = self._events.setdefault(event.symbol, deque())
        events.append(
            _ObservedTrade(
                event.exchange_at,
                event.price,
                event.quote_notional,
                event.side,
            )
        )
        cutoff = event.exchange_at - self._retention
        while events and events[0].at < cutoff:
            events.popleft()
        try:
            book = self._orderbook_provider(event.symbol).metrics(
                as_of=event.received_at
            )
        except (KeyError, ValueError):
            return None
        if (
            not book.ready
            or book.best_bid is None
            or book.best_ask is None
            or book.spread_bps is None
            or book.bid_depth_10bps is None
            or book.ask_depth_10bps is None
            or book.imbalance_l5 is None
            or book.book_exchange_at is None
        ):
            return None
        flow = self._trade_flow.metrics(event.symbol, as_of=event.received_at)
        window_5 = tuple(
            item
            for item in events
            if item.at >= event.exchange_at - timedelta(seconds=5)
        )
        buy = sum(item.notional for item in window_5 if item.side is TradeSide.BUY)
        sell = sum(item.notional for item in window_5 if item.side is TradeSide.SELL)
        total = buy + sell
        if total <= 0:
            return None
        imbalance = (buy - sell) / total
        direction = (
            ImpulseDirection.LONG if imbalance > 0 else ImpulseDirection.SHORT
        )
        impulse = self._resolve_impulse(event, direction)
        if len(window_5) < 2:
            return None
        displacement_1 = _displacement(events, event, seconds=1)
        displacement_5 = _displacement(events, event, seconds=5)
        displacement_15 = _displacement(events, event, seconds=15)
        sign = 1.0 if direction is ImpulseDirection.LONG else -1.0
        impulse_displacement = max(
            0.0,
            sign * (event.price - impulse.start_price) / impulse.start_price * 10_000,
        )
        delta_baseline = max(abs(flow.delta_5s) / 5.0, 1.0)
        acceleration = abs(flow.delta_1s) / delta_baseline
        response = abs(displacement_5) / max(abs(flow.delta_5s) / 10_000.0, 1.0)
        prices = tuple(item.price for item in events)
        volatility = (max(prices) - min(prices)) / event.price * 10_000
        absorption = abs(imbalance) >= 0.40 and abs(displacement_5) < 2.0
        source_at = min(event.exchange_at, book.book_exchange_at)
        return ImpulseSnapshot(
            symbol=event.symbol,
            observed_at=event.received_at,
            source_at=source_at,
            impulse_id=impulse.impulse_id,
            impulse_started_at=impulse.started_at,
            direction=direction,
            market_price=event.price,
            best_bid=book.best_bid,
            best_ask=book.best_ask,
            spread_bps=book.spread_bps,
            bid_depth_10bps=book.bid_depth_10bps,
            ask_depth_10bps=book.ask_depth_10bps,
            delta_1s=flow.delta_1s,
            delta_5s=flow.delta_5s,
            delta_15s=flow.delta_15s,
            flow_imbalance_5s=imbalance,
            flow_acceleration=acceleration,
            price_displacement_1s_bps=displacement_1,
            price_displacement_5s_bps=displacement_5,
            price_displacement_15s_bps=displacement_15,
            impulse_displacement_bps=impulse_displacement,
            price_response_bps_per_10k=response,
            estimated_potential_bps=volatility,
            local_volatility_bps=volatility,
            orderbook_imbalance=book.imbalance_l5,
            sweep_direction=SweepDirection.NONE,
            absorption_detected=absorption,
            trigger_progress_atr=None,
        )

    def remove_symbol(self, symbol: str) -> None:
        normalized = symbol.strip().upper()
        self._events.pop(normalized, None)
        self._impulses.pop(normalized, None)

    def _resolve_impulse(
        self, event: PublicTradeEvent, direction: ImpulseDirection
    ) -> _ImpulseState:
        current = self._impulses.get(event.symbol)
        if (
            current is None
            or current.direction is not direction
            or (event.exchange_at - current.last_at).total_seconds() >= 5.0
        ):
            identity = "|".join(
                (event.symbol, direction.value, event.exchange_at.isoformat())
            )
            current = _ImpulseState(
                direction=direction,
                started_at=event.exchange_at,
                start_price=event.price,
                impulse_id=hashlib.sha256(identity.encode()).hexdigest()[:24],
                last_at=event.exchange_at,
            )
            self._impulses[event.symbol] = current
        else:
            current.last_at = event.exchange_at
        return current


class V3ShadowRuntime:
    """Shadow-only V3 runtime; forward outcomes outlive strategy exits."""

    def __init__(
        self,
        *,
        engine: CashScalperEngine,
        entry_journal: JsonlTelemetryJournal,
        trade_journal: JsonlTelemetryJournal,
        outcome_journal: JsonlTelemetryJournal,
        notional: float = 1_000.0,
    ) -> None:
        self._engine = engine
        self._entry_journal = entry_journal
        self._trade_journal = trade_journal
        self._outcome_journal = outcome_journal
        self._notional = notional
        self._active: dict[str, V3ShadowTrade] = {}
        self._trackers: dict[str, ForwardOutcomeTracker] = {}

    def process_snapshot(self, snapshot: ImpulseSnapshot) -> V3RuntimeResult:
        if snapshot.symbol in self._active:
            return V3RuntimeResult(
                False,
                self._active[snapshot.symbol],
                ("active_trade",),
            )
        decision = self._engine.evaluate(snapshot, notional=self._notional)
        if not decision.accepted:
            return V3RuntimeResult(False, None, decision.blocking_reasons)
        trade = self._engine.open_shadow_trade(decision)
        assert decision.entry_price is not None
        assert decision.target_price is not None
        assert decision.stop_price is not None
        self._active[trade.symbol] = trade
        entry_record = build_entry_record(
            snapshot,
            recorded_at=snapshot.observed_at,
            notional=self._notional,
            entry_price=decision.entry_price,
            target_price=decision.target_price,
            stop_price=decision.stop_price,
            cost=decision.cost,
        )
        self._entry_journal.append(entry_record)
        self._trade_journal.append(
            build_trade_record(trade, recorded_at=snapshot.observed_at)
        )
        self._trackers[trade.trade_id] = ForwardOutcomeTracker(
            entry_id=entry_record.record_id,
            symbol=trade.symbol,
            direction=trade.direction,
            entry_at=trade.entry_at,
            entry_price=trade.entry_price,
            round_trip_cost_pct=trade.round_trip_cost_pct,
        )
        return V3RuntimeResult(True, trade)

    def process_price(
        self,
        symbol: str,
        observed_at: datetime,
        price: float,
        *,
        directional_failure: bool = False,
    ) -> None:
        normalized = symbol.strip().upper()
        for trade_id, tracker in tuple(self._trackers.items()):
            if tracker.symbol != normalized:
                continue
            for outcome in tracker.observe(observed_at, price):
                self._outcome_journal.append(outcome)
            if tracker.complete:
                self._trackers.pop(trade_id, None)

        trade = self._active.get(normalized)
        if trade is None:
            return
        update = self._engine.manage(
            trade,
            V3PriceObservation(normalized, observed_at, price, directional_failure),
        )
        if not update.changed:
            return
        previous = trade
        self._active[normalized] = update.trade
        transition = (
            previous.stage is not update.trade.stage
            or previous.exit_reason is not update.trade.exit_reason
        )
        if transition:
            self._trade_journal.append(
                build_trade_record(update.trade, recorded_at=observed_at)
            )
        if update.trade.stage is V3TradeStage.CLOSED:
            self._engine.remember_terminal(update.trade)
            self._active.pop(normalized, None)

    def active_trade(self, symbol: str) -> V3ShadowTrade | None:
        return self._active.get(symbol.strip().upper())


def _displacement(
    events: deque[_ObservedTrade], current: PublicTradeEvent, *, seconds: int
) -> float:
    cutoff = current.exchange_at - timedelta(seconds=seconds)
    eligible = tuple(item for item in events if item.at >= cutoff)
    if not eligible:
        return 0.0
    return (current.price - eligible[0].price) / eligible[0].price * 10_000
