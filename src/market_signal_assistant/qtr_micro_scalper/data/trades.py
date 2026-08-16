from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from threading import Lock

from market_signal_assistant.qtr_micro_scalper.data.models import (
    PublicTradeEvent,
    TradeSide,
)

Clock = Callable[[], datetime]
TradeIdentity = tuple[str, str]


class IngestStatus(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    LATE = "late"


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    identity: TradeIdentity

    @property
    def accepted(self) -> bool:
        return self.status is IngestStatus.ACCEPTED


@dataclass(frozen=True, slots=True)
class TradeFlowMetrics:
    symbol: str
    as_of: datetime
    buy_notional_1s: float
    sell_notional_1s: float
    delta_1s: float
    delta_5s: float
    delta_15s: float
    delta_60s: float
    cvd_process: float
    cvd_utc_day: float
    cvd_episode: float | None
    trade_count_5s: int
    largest_trade_5s: float
    block_delta_60s: float
    rpi_delta_60s: float
    last_trade_at: datetime | None


@dataclass(frozen=True, slots=True)
class TradeFlowSimulation:
    metrics: TradeFlowMetrics
    accepted_events: int
    duplicate_events: int
    late_events: int


class TradeFlowAccumulator:
    """Thread-safe rolling delta and CVD collector for normalized trades.

    Primary flow includes ordinary and RPI trades and excludes block trades.
    The bounded event/dedup retention serves rolling metrics; cumulative CVD is
    kept independently and therefore does not decay with the rolling window.
    """

    def __init__(
        self,
        *,
        retention: timedelta = timedelta(seconds=75),
        clock: Clock | None = None,
    ) -> None:
        if retention < timedelta(seconds=60):
            raise ValueError("Trade retention must be at least 60 seconds.")
        self._retention = retention
        self._clock = clock or (lambda: datetime.now(UTC))
        self._events: dict[str, dict[str, PublicTradeEvent]] = {}
        self._cvd_process: dict[str, float] = {}
        self._cvd_by_day: dict[tuple[str, date], float] = {}
        self._episode_started_at: dict[str, datetime] = {}
        self._cvd_episode: dict[str, float] = {}
        self._lock = Lock()

    @property
    def retention(self) -> timedelta:
        return self._retention

    def ingest(self, event: PublicTradeEvent) -> IngestResult:
        if not isinstance(event, PublicTradeEvent):
            raise TypeError("TradeFlowAccumulator accepts PublicTradeEvent only.")
        identity = (event.symbol, event.trade_id)
        now = self._now()
        with self._lock:
            self._prune_locked(now)
            symbol_events = self._events.setdefault(event.symbol, {})
            if event.trade_id in symbol_events:
                return IngestResult(IngestStatus.DUPLICATE, identity)
            if event.exchange_at < now - self._retention:
                return IngestResult(IngestStatus.LATE, identity)

            symbol_events[event.trade_id] = event
            if not event.is_block_trade:
                signed = _signed_notional(event)
                self._cvd_process[event.symbol] = (
                    self._cvd_process.get(event.symbol, 0.0) + signed
                )
                day_key = (event.symbol, event.exchange_at.date())
                self._cvd_by_day[day_key] = self._cvd_by_day.get(day_key, 0.0) + signed
                episode_at = self._episode_started_at.get(event.symbol)
                if episode_at is not None and event.exchange_at >= episode_at:
                    self._cvd_episode[event.symbol] = (
                        self._cvd_episode.get(event.symbol, 0.0) + signed
                    )
            return IngestResult(IngestStatus.ACCEPTED, identity)

    def metrics(self, symbol: str, *, as_of: datetime) -> TradeFlowMetrics:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_as_of = _normalize_timestamp("Trade metrics as_of", as_of)
        with self._lock:
            self._prune_locked(normalized_as_of)
            events = tuple(
                event
                for event in self._events.get(normalized_symbol, {}).values()
                if event.exchange_at <= normalized_as_of
            )
            primary = tuple(event for event in events if not event.is_block_trade)
            primary_1s = _window(primary, normalized_as_of, seconds=1)
            primary_5s = _window(primary, normalized_as_of, seconds=5)
            primary_15s = _window(primary, normalized_as_of, seconds=15)
            primary_60s = _window(primary, normalized_as_of, seconds=60)
            all_60s = _window(events, normalized_as_of, seconds=60)

            episode_at = self._episode_started_at.get(normalized_symbol)
            episode_cvd = None
            if episode_at is not None and episode_at <= normalized_as_of:
                episode_cvd = self._cvd_episode.get(normalized_symbol, 0.0)

            return TradeFlowMetrics(
                symbol=normalized_symbol,
                as_of=normalized_as_of,
                buy_notional_1s=sum(
                    event.quote_notional
                    for event in primary_1s
                    if event.side is TradeSide.BUY
                ),
                sell_notional_1s=sum(
                    event.quote_notional
                    for event in primary_1s
                    if event.side is TradeSide.SELL
                ),
                delta_1s=_delta(primary_1s),
                delta_5s=_delta(primary_5s),
                delta_15s=_delta(primary_15s),
                delta_60s=_delta(primary_60s),
                cvd_process=self._cvd_process.get(normalized_symbol, 0.0),
                cvd_utc_day=self._cvd_by_day.get(
                    (normalized_symbol, normalized_as_of.date()),
                    0.0,
                ),
                cvd_episode=episode_cvd,
                trade_count_5s=len(primary_5s),
                largest_trade_5s=max(
                    (event.quote_notional for event in primary_5s),
                    default=0.0,
                ),
                block_delta_60s=_delta(
                    tuple(event for event in all_60s if event.is_block_trade)
                ),
                rpi_delta_60s=_delta(
                    tuple(event for event in all_60s if event.is_rpi_trade)
                ),
                last_trade_at=max(
                    (event.exchange_at for event in primary),
                    default=None,
                ),
            )

    def start_episode(self, symbol: str, *, at: datetime) -> None:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_at = _normalize_timestamp("Episode start", at)
        with self._lock:
            self._prune_locked(normalized_at)
            self._episode_started_at[normalized_symbol] = normalized_at
            self._cvd_episode[normalized_symbol] = _delta(
                tuple(
                    event
                    for event in self._events.get(normalized_symbol, {}).values()
                    if not event.is_block_trade and event.exchange_at >= normalized_at
                )
            )

    def event_count(self, symbol: str) -> int:
        normalized_symbol = _normalize_symbol(symbol)
        with self._lock:
            self._prune_locked(self._now())
            return len(self._events.get(normalized_symbol, {}))

    def _now(self) -> datetime:
        return _normalize_timestamp("Trade collector clock", self._clock())

    def _prune_locked(self, now: datetime) -> None:
        cutoff = now - self._retention
        for symbol in tuple(self._events):
            retained = {
                trade_id: event
                for trade_id, event in self._events[symbol].items()
                if event.exchange_at >= cutoff
            }
            if retained:
                self._events[symbol] = retained
            else:
                del self._events[symbol]

        oldest_day = (now - self._retention).date()
        self._cvd_by_day = {
            key: value
            for key, value in self._cvd_by_day.items()
            if key[1] >= oldest_day
        }


def simulate_trade_flow(
    events: Iterable[PublicTradeEvent],
    *,
    symbol: str,
    as_of: datetime,
    retention: timedelta = timedelta(seconds=75),
    episode_started_at: datetime | None = None,
) -> TradeFlowSimulation:
    """Replay normalized events offline through the production accumulator math."""

    normalized_as_of = _normalize_timestamp("Simulation as_of", as_of)
    accumulator = TradeFlowAccumulator(
        retention=retention,
        clock=lambda: normalized_as_of,
    )
    if episode_started_at is not None:
        accumulator.start_episode(symbol, at=episode_started_at)

    counts = {
        IngestStatus.ACCEPTED: 0,
        IngestStatus.DUPLICATE: 0,
        IngestStatus.LATE: 0,
    }
    ordered_events = sorted(
        events,
        key=lambda event: (event.exchange_at, event.symbol, event.trade_id),
    )
    for event in ordered_events:
        result = accumulator.ingest(event)
        counts[result.status] += 1

    return TradeFlowSimulation(
        metrics=accumulator.metrics(symbol, as_of=normalized_as_of),
        accepted_events=counts[IngestStatus.ACCEPTED],
        duplicate_events=counts[IngestStatus.DUPLICATE],
        late_events=counts[IngestStatus.LATE],
    )


def _window(
    events: tuple[PublicTradeEvent, ...],
    as_of: datetime,
    *,
    seconds: int,
) -> tuple[PublicTradeEvent, ...]:
    cutoff = as_of - timedelta(seconds=seconds)
    return tuple(event for event in events if cutoff <= event.exchange_at <= as_of)


def _delta(events: tuple[PublicTradeEvent, ...]) -> float:
    return sum(_signed_notional(event) for event in events)


def _signed_notional(event: PublicTradeEvent) -> float:
    if event.side is TradeSide.BUY:
        return event.quote_notional
    return -event.quote_notional


def _normalize_symbol(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Trade symbol cannot be empty.")
    return value.strip().upper()


def _normalize_timestamp(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return value.astimezone(UTC)
