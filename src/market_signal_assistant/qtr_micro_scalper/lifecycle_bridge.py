from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, TypeAlias

from market_signal_assistant.qtr_micro_scalper.data.models import (
    LiquidationEvent,
    OrderBookEvent,
    PublicTradeEvent,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowBarResult
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowPriceBar,
    ShadowTrade,
)

LifecycleMarketEvent: TypeAlias = (
    PublicTradeEvent | OrderBookEvent | LiquidationEvent
)


class ShadowBarProcessor(Protocol):
    def process_bars(
        self,
        bars: tuple[ShadowPriceBar, ...],
    ) -> tuple[ShadowBarResult, ...]: ...


@dataclass(frozen=True, slots=True)
class LiveShadowLifecycleConfig:
    """Configuration for observed-trade OHLC lifecycle bars.

    One second is deliberately small compared with the 60-second entry window.
    Missing seconds are never filled and incomplete buckets are never emitted.
    """

    bar_interval_seconds: int = 1

    def __post_init__(self) -> None:
        if (
            isinstance(self.bar_interval_seconds, bool)
            or self.bar_interval_seconds < 1
        ):
            raise ValueError("Lifecycle bar interval must be a positive integer.")


@dataclass(slots=True)
class _ObservedBucket:
    index: int
    opened_at: datetime
    closed_at: datetime
    open: float
    high: float
    low: float
    close: float
    open_key: tuple[datetime, str]
    close_key: tuple[datetime, str]

    @classmethod
    def from_trade(
        cls,
        event: PublicTradeEvent,
        *,
        index: int,
        opened_at: datetime,
        closed_at: datetime,
    ) -> _ObservedBucket:
        key = (event.exchange_at, event.trade_id)
        return cls(
            index=index,
            opened_at=opened_at,
            closed_at=closed_at,
            open=event.price,
            high=event.price,
            low=event.price,
            close=event.price,
            open_key=key,
            close_key=key,
        )

    def observe(self, event: PublicTradeEvent) -> None:
        key = (event.exchange_at, event.trade_id)
        self.high = max(self.high, event.price)
        self.low = min(self.low, event.price)
        if key < self.open_key:
            self.open_key = key
            self.open = event.price
        if key > self.close_key:
            self.close_key = key
            self.close = event.price

    def freeze(self, symbol: str) -> ShadowPriceBar:
        return ShadowPriceBar(
            symbol=symbol,
            opened_at=self.opened_at,
            closed_at=self.closed_at,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
        )


@dataclass(slots=True)
class _TrackedTrade:
    trade_id: str
    starts_at: datetime
    bucket: _ObservedBucket | None = None
    last_emitted_at: datetime | None = None


class LiveShadowLifecycleBridge:
    """Convert observed public trades into completed shadow lifecycle bars.

    Buckets are anchored to ``ShadowTrade.planned_at`` so the 60-second entry
    deadline is an exact bar boundary. OHLC values come exclusively from actual
    ``PublicTradeEvent`` prices; no gap filling or synthetic close is used.
    """

    def __init__(
        self,
        processor: ShadowBarProcessor,
        config: LiveShadowLifecycleConfig | None = None,
    ) -> None:
        self._processor = processor
        self._config = config or LiveShadowLifecycleConfig()
        self._tracked: dict[str, _TrackedTrade] = {}

    def activate(self, trade: ShadowTrade) -> bool:
        if trade.terminal:
            raise ValueError("Terminal shadow trade cannot be tracked live.")
        current = self._tracked.get(trade.symbol)
        if current is not None and current.trade_id == trade.trade_id:
            return False
        self._tracked[trade.symbol] = _TrackedTrade(
            trade_id=trade.trade_id,
            starts_at=trade.planned_at,
        )
        return True

    def process_event(
        self,
        event: LifecycleMarketEvent,
    ) -> tuple[ShadowBarResult, ...]:
        tracked = self._tracked.get(event.symbol)
        if tracked is None:
            return ()
        completed: list[ShadowPriceBar] = []
        if isinstance(event, PublicTradeEvent):
            completed.extend(self._observe_trade(tracked, event))
        completed.extend(
            self._drain_completed(event.symbol, tracked, event.exchange_at)
        )
        if not completed:
            return ()
        results = self._processor.process_bars(tuple(completed))
        for result in results:
            if result.trade is not None and result.trade.terminal:
                self._tracked.pop(result.symbol, None)
        return results

    def stop(self) -> None:
        """Discard incomplete in-memory buckets without inventing closing data."""

        self._tracked.clear()

    def is_tracking(self, symbol: str) -> bool:
        return symbol.strip().upper() in self._tracked

    def tracked_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._tracked))


    def _observe_trade(
        self,
        tracked: _TrackedTrade,
        event: PublicTradeEvent,
    ) -> tuple[ShadowPriceBar, ...]:
        observed_at = event.exchange_at.astimezone(UTC)
        if observed_at < tracked.starts_at:
            return ()
        interval = self._config.bar_interval_seconds
        elapsed = (observed_at - tracked.starts_at).total_seconds()
        index = int(elapsed // interval)
        opened_at = tracked.starts_at + timedelta(seconds=index * interval)
        closed_at = opened_at + timedelta(seconds=interval)
        if (
            tracked.last_emitted_at is not None
            and closed_at <= tracked.last_emitted_at
        ):
            return ()
        current = tracked.bucket
        if current is None:
            tracked.bucket = _ObservedBucket.from_trade(
                event,
                index=index,
                opened_at=opened_at,
                closed_at=closed_at,
            )
            return ()
        if index < current.index:
            return ()
        if index == current.index:
            current.observe(event)
            return ()
        completed = current.freeze(event.symbol)
        tracked.last_emitted_at = current.closed_at
        tracked.bucket = _ObservedBucket.from_trade(
            event,
            index=index,
            opened_at=opened_at,
            closed_at=closed_at,
        )
        return (completed,)

    @staticmethod
    def _drain_completed(
        symbol: str,
        tracked: _TrackedTrade,
        market_at: datetime,
    ) -> tuple[ShadowPriceBar, ...]:
        current = tracked.bucket
        if current is None or current.closed_at > market_at.astimezone(UTC):
            return ()
        completed = current.freeze(symbol)
        tracked.last_emitted_at = current.closed_at
        tracked.bucket = None
        return (completed,)
