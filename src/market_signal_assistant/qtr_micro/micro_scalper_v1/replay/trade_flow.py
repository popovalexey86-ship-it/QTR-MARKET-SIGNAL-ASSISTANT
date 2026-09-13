from __future__ import annotations

from collections import deque
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

from market_signal_assistant.qtr_micro_scalper.data.models import (
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import (
    IngestResult,
    IngestStatus,
    TradeFlowMetrics,
)

Clock = Callable[[], datetime]


class ReplayTradeFlowAccumulator:
    """Fast chronological trade-flow accumulator for historical replay only."""

    def __init__(
        self,
        *,
        retention: timedelta = timedelta(seconds=75),
        clock: Clock,
    ) -> None:
        if retention < timedelta(seconds=60):
            raise ValueError("Trade retention must be at least 60 seconds.")

        self._retention = retention
        self._clock = clock

        self._seen: dict[str, datetime] = {}
        self._seen_queue: deque[tuple[datetime, str]] = deque()

        self._primary_1s: deque[PublicTradeEvent] = deque()
        self._primary_5s: deque[PublicTradeEvent] = deque()
        self._primary_15s: deque[PublicTradeEvent] = deque()
        self._primary_60s: deque[PublicTradeEvent] = deque()
        self._block_60s: deque[PublicTradeEvent] = deque()
        self._rpi_60s: deque[PublicTradeEvent] = deque()

        self._buy_1s = 0.0
        self._sell_1s = 0.0
        self._delta_1s = 0.0
        self._delta_5s = 0.0
        self._delta_15s = 0.0
        self._delta_60s = 0.0
        self._block_delta_60s = 0.0
        self._rpi_delta_60s = 0.0

        self._cvd_process = 0.0
        self._cvd_by_day: dict[date, float] = {}
        self._last_trade_at: datetime | None = None

    def ingest(self, event: PublicTradeEvent) -> IngestResult:
        now = self._normalize_timestamp(self._clock())
        identity = (event.symbol, event.trade_id)

        if event.trade_id in self._seen:
            return IngestResult(IngestStatus.DUPLICATE, identity)

        if event.exchange_at < now - self._retention:
            return IngestResult(IngestStatus.LATE, identity)

        self._seen[event.trade_id] = event.exchange_at
        self._seen_queue.append((event.exchange_at, event.trade_id))

        signed = self._signed_notional(event)

        if not event.is_block_trade:
            self._primary_1s.append(event)
            self._primary_5s.append(event)
            self._primary_15s.append(event)
            self._primary_60s.append(event)

            self._delta_1s += signed
            self._delta_5s += signed
            self._delta_15s += signed
            self._delta_60s += signed

            if event.side is TradeSide.BUY:
                self._buy_1s += event.quote_notional
            else:
                self._sell_1s += event.quote_notional

            self._cvd_process += signed
            day = event.exchange_at.date()
            self._cvd_by_day[day] = self._cvd_by_day.get(day, 0.0) + signed
            self._last_trade_at = event.exchange_at

        if event.is_block_trade:
            self._block_60s.append(event)
            self._block_delta_60s += signed

        if event.is_rpi_trade:
            self._rpi_60s.append(event)
            self._rpi_delta_60s += signed

        return IngestResult(IngestStatus.ACCEPTED, identity)

    def metrics(self, symbol: str, *, as_of: datetime) -> TradeFlowMetrics:
        normalized_symbol = symbol.strip().upper()
        normalized_as_of = self._normalize_timestamp(as_of)

        self._prune_seen(normalized_as_of)
        self._prune_primary_1s(normalized_as_of)
        self._delta_5s = self._prune_delta_window(
            self._primary_5s,
            normalized_as_of,
            5,
            self._delta_5s,
        )
        self._delta_15s = self._prune_delta_window(
            self._primary_15s,
            normalized_as_of,
            15,
            self._delta_15s,
        )
        self._delta_60s = self._prune_delta_window(
            self._primary_60s,
            normalized_as_of,
            60,
            self._delta_60s,
        )
        self._block_delta_60s = self._prune_delta_window(
            self._block_60s,
            normalized_as_of,
            60,
            self._block_delta_60s,
        )
        self._rpi_delta_60s = self._prune_delta_window(
            self._rpi_60s,
            normalized_as_of,
            60,
            self._rpi_delta_60s,
        )

        oldest_day = (normalized_as_of - self._retention).date()
        self._cvd_by_day = {
            day: value
            for day, value in self._cvd_by_day.items()
            if day >= oldest_day
        }

        return TradeFlowMetrics(
            symbol=normalized_symbol,
            as_of=normalized_as_of,
            buy_notional_1s=self._buy_1s,
            sell_notional_1s=self._sell_1s,
            delta_1s=self._delta_1s,
            delta_5s=self._delta_5s,
            delta_15s=self._delta_15s,
            delta_60s=self._delta_60s,
            cvd_process=self._cvd_process,
            cvd_utc_day=self._cvd_by_day.get(normalized_as_of.date(), 0.0),
            cvd_episode=None,
            trade_count_5s=len(self._primary_5s),
            largest_trade_5s=max(
                (event.quote_notional for event in self._primary_5s),
                default=0.0,
            ),
            block_delta_60s=self._block_delta_60s,
            rpi_delta_60s=self._rpi_delta_60s,
            last_trade_at=self._last_trade_at,
        )

    def _prune_seen(self, as_of: datetime) -> None:
        cutoff = as_of - self._retention
        while self._seen_queue and self._seen_queue[0][0] < cutoff:
            _, trade_id = self._seen_queue.popleft()
            self._seen.pop(trade_id, None)

    def _prune_primary_1s(self, as_of: datetime) -> None:
        cutoff = as_of - timedelta(seconds=1)
        while self._primary_1s and self._primary_1s[0].exchange_at < cutoff:
            event = self._primary_1s.popleft()
            signed = self._signed_notional(event)
            self._delta_1s -= signed
            if event.side is TradeSide.BUY:
                self._buy_1s -= event.quote_notional
            else:
                self._sell_1s -= event.quote_notional

    def _prune_delta_window(
        self,
        events: deque[PublicTradeEvent],
        as_of: datetime,
        seconds: int,
        current_delta: float,
    ) -> float:
        cutoff = as_of - timedelta(seconds=seconds)
        while events and events[0].exchange_at < cutoff:
            current_delta -= self._signed_notional(events.popleft())
        return current_delta

    @staticmethod
    def _signed_notional(event: PublicTradeEvent) -> float:
        if event.side is TradeSide.BUY:
            return event.quote_notional
        return -event.quote_notional

    @staticmethod
    def _normalize_timestamp(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Replay timestamp must be timezone-aware.")
        return value.astimezone(UTC)
