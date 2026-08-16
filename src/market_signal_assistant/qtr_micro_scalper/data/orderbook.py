from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevel,
)


class OrderBookProcessStatus(StrEnum):
    APPLIED_SNAPSHOT = "applied_snapshot"
    APPLIED_DELTA = "applied_delta"
    IGNORED_STALE = "ignored_stale"
    SNAPSHOT_REQUIRED = "snapshot_required"
    DESYNCHRONIZED = "desynchronized"


@dataclass(frozen=True, slots=True)
class OrderBookProcessResult:
    status: OrderBookProcessStatus
    update_id: int
    ready: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class OrderBookMetrics:
    symbol: str
    as_of: datetime
    book_exchange_at: datetime | None
    book_age_ms: float | None
    update_id: int | None
    cross_sequence: int | None
    bid_levels: int
    ask_levels: int
    best_bid: float | None
    best_ask: float | None
    mid_price: float | None
    microprice: float | None
    spread_bps: float | None
    bid_depth_5bps: float | None
    ask_depth_5bps: float | None
    bid_depth_10bps: float | None
    ask_depth_10bps: float | None
    bid_depth_25bps: float | None
    ask_depth_25bps: float | None
    imbalance_l1: float | None
    imbalance_l5: float | None
    imbalance_l10: float | None
    imbalance_l25: float | None
    imbalance_l50: float | None
    ready: bool
    health_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class OrderBookSimulation:
    metrics: OrderBookMetrics
    results: tuple[OrderBookProcessResult, ...]


class OrderBookState:
    """Thread-safe local order book reconstructed from normalized events."""

    def __init__(
        self,
        symbol: str,
        *,
        depth: int = 50,
        require_contiguous_update_ids: bool = True,
    ) -> None:
        if isinstance(depth, bool) or not isinstance(depth, int) or depth < 1:
            raise ValueError("Order book depth must be a positive integer.")
        if not isinstance(require_contiguous_update_ids, bool):
            raise ValueError("Contiguous update setting must be boolean.")
        self._symbol = _normalize_symbol(symbol)
        self._depth = depth
        self._require_contiguous = require_contiguous_update_ids
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._has_snapshot = False
        self._desynchronized = False
        self._last_update_id: int | None = None
        self._last_cross_sequence: int | None = None
        self._last_exchange_at: datetime | None = None
        self._lock = Lock()

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def ready(self) -> bool:
        with self._lock:
            return not self._health_reasons_locked()

    def process(self, event: OrderBookEvent) -> OrderBookProcessResult:
        if not isinstance(event, OrderBookEvent):
            raise TypeError("OrderBookState accepts OrderBookEvent only.")
        if event.symbol != self._symbol:
            raise ValueError("Order book event symbol does not match state symbol.")
        with self._lock:
            if event.event_type is OrderBookEventType.SNAPSHOT:
                return self._apply_snapshot_locked(event)
            return self._apply_delta_locked(event)

    def metrics(self, *, as_of: datetime) -> OrderBookMetrics:
        normalized_as_of = _normalize_timestamp("Order book metrics as_of", as_of)
        with self._lock:
            if (
                self._last_exchange_at is not None
                and normalized_as_of < self._last_exchange_at
            ):
                raise ValueError("Order book metrics as_of precedes latest event.")
            bids = _sorted_bids(self._bids)
            asks = _sorted_asks(self._asks)
            best_bid = bids[0] if bids else None
            best_ask = asks[0] if asks else None
            mid = _mid_price(best_bid, best_ask)
            reasons = self._health_reasons_locked()

            return OrderBookMetrics(
                symbol=self._symbol,
                as_of=normalized_as_of,
                book_exchange_at=self._last_exchange_at,
                book_age_ms=_age_ms(normalized_as_of, self._last_exchange_at),
                update_id=self._last_update_id,
                cross_sequence=self._last_cross_sequence,
                bid_levels=len(bids),
                ask_levels=len(asks),
                best_bid=best_bid.price if best_bid is not None else None,
                best_ask=best_ask.price if best_ask is not None else None,
                mid_price=mid,
                microprice=_microprice(best_bid, best_ask),
                spread_bps=_spread_bps(best_bid, best_ask, mid),
                bid_depth_5bps=_depth_in_band(bids, mid, bps=5, is_bid=True),
                ask_depth_5bps=_depth_in_band(asks, mid, bps=5, is_bid=False),
                bid_depth_10bps=_depth_in_band(bids, mid, bps=10, is_bid=True),
                ask_depth_10bps=_depth_in_band(asks, mid, bps=10, is_bid=False),
                bid_depth_25bps=_depth_in_band(bids, mid, bps=25, is_bid=True),
                ask_depth_25bps=_depth_in_band(asks, mid, bps=25, is_bid=False),
                imbalance_l1=_imbalance(bids, asks, levels=1),
                imbalance_l5=_imbalance(bids, asks, levels=5),
                imbalance_l10=_imbalance(bids, asks, levels=10),
                imbalance_l25=_imbalance(bids, asks, levels=25),
                imbalance_l50=_imbalance(bids, asks, levels=50),
                ready=not reasons,
                health_reasons=reasons,
            )

    def levels(
        self,
    ) -> tuple[tuple[OrderBookLevel, ...], tuple[OrderBookLevel, ...]]:
        """Return an immutable view, primarily for deterministic diagnostics."""

        with self._lock:
            return _sorted_bids(self._bids), _sorted_asks(self._asks)

    def _apply_snapshot_locked(
        self,
        event: OrderBookEvent,
    ) -> OrderBookProcessResult:
        bids = {level.price: level.quantity for level in event.bids}
        asks = {level.price: level.quantity for level in event.asks}
        bids, asks = self._trim(bids, asks)

        self._bids = bids
        self._asks = asks
        self._has_snapshot = True
        self._last_update_id = event.update_id
        self._last_cross_sequence = event.cross_sequence
        self._last_exchange_at = event.exchange_at
        invalid_reasons = _book_reasons(bids, asks)
        self._desynchronized = bool(invalid_reasons)
        if invalid_reasons:
            return OrderBookProcessResult(
                status=OrderBookProcessStatus.DESYNCHRONIZED,
                update_id=event.update_id,
                ready=False,
                reason=invalid_reasons[0],
            )
        return OrderBookProcessResult(
            status=OrderBookProcessStatus.APPLIED_SNAPSHOT,
            update_id=event.update_id,
            ready=True,
        )

    def _apply_delta_locked(self, event: OrderBookEvent) -> OrderBookProcessResult:
        if not self._has_snapshot or self._desynchronized:
            return OrderBookProcessResult(
                status=OrderBookProcessStatus.SNAPSHOT_REQUIRED,
                update_id=event.update_id,
                ready=False,
                reason="snapshot_required",
            )
        if self._last_update_id is None:
            raise RuntimeError("Order book snapshot state is inconsistent.")
        if event.update_id <= self._last_update_id:
            return OrderBookProcessResult(
                status=OrderBookProcessStatus.IGNORED_STALE,
                update_id=event.update_id,
                ready=True,
                reason="stale_update",
            )
        if self._require_contiguous and event.update_id != self._last_update_id + 1:
            return self._mark_desynchronized(event.update_id, "update_gap")
        if (
            event.cross_sequence is not None
            and self._last_cross_sequence is not None
            and event.cross_sequence <= self._last_cross_sequence
        ):
            return self._mark_desynchronized(event.update_id, "sequence_rollback")

        bids = dict(self._bids)
        asks = dict(self._asks)
        _apply_levels(bids, event.bids)
        _apply_levels(asks, event.asks)
        bids, asks = self._trim(bids, asks)
        invalid_reasons = _book_reasons(bids, asks)
        if invalid_reasons:
            return self._mark_desynchronized(event.update_id, invalid_reasons[0])

        self._bids = bids
        self._asks = asks
        self._last_update_id = event.update_id
        self._last_cross_sequence = event.cross_sequence
        self._last_exchange_at = event.exchange_at
        return OrderBookProcessResult(
            status=OrderBookProcessStatus.APPLIED_DELTA,
            update_id=event.update_id,
            ready=True,
        )

    def _mark_desynchronized(
        self,
        update_id: int,
        reason: str,
    ) -> OrderBookProcessResult:
        self._desynchronized = True
        return OrderBookProcessResult(
            status=OrderBookProcessStatus.DESYNCHRONIZED,
            update_id=update_id,
            ready=False,
            reason=reason,
        )

    def _trim(
        self,
        bids: dict[float, float],
        asks: dict[float, float],
    ) -> tuple[dict[float, float], dict[float, float]]:
        retained_bids = sorted(bids, reverse=True)[: self._depth]
        retained_asks = sorted(asks)[: self._depth]
        return (
            {price: bids[price] for price in retained_bids},
            {price: asks[price] for price in retained_asks},
        )

    def _health_reasons_locked(self) -> tuple[str, ...]:
        if not self._has_snapshot:
            return ("snapshot_required",)
        reasons: list[str] = []
        if self._desynchronized:
            reasons.append("desynchronized")
        reasons.extend(_book_reasons(self._bids, self._asks))
        return tuple(dict.fromkeys(reasons))


def simulate_orderbook(
    events: Iterable[OrderBookEvent],
    *,
    symbol: str,
    as_of: datetime,
    depth: int = 50,
    require_contiguous_update_ids: bool = True,
) -> OrderBookSimulation:
    """Replay normalized events offline in their supplied arrival order."""

    state = OrderBookState(
        symbol,
        depth=depth,
        require_contiguous_update_ids=require_contiguous_update_ids,
    )
    results = tuple(state.process(event) for event in events)
    return OrderBookSimulation(
        metrics=state.metrics(as_of=as_of),
        results=results,
    )


def _apply_levels(
    target: dict[float, float],
    levels: tuple[OrderBookLevel, ...],
) -> None:
    for level in levels:
        if level.quantity == 0:
            target.pop(level.price, None)
        else:
            target[level.price] = level.quantity


def _sorted_bids(book: dict[float, float]) -> tuple[OrderBookLevel, ...]:
    return tuple(
        OrderBookLevel(price, book[price]) for price in sorted(book, reverse=True)
    )


def _sorted_asks(book: dict[float, float]) -> tuple[OrderBookLevel, ...]:
    return tuple(OrderBookLevel(price, book[price]) for price in sorted(book))


def _book_reasons(
    bids: dict[float, float],
    asks: dict[float, float],
) -> tuple[str, ...]:
    reasons: list[str] = []
    if not bids:
        reasons.append("empty_bid_book")
    if not asks:
        reasons.append("empty_ask_book")
    if bids and asks and max(bids) >= min(asks):
        reasons.append("crossed_book")
    return tuple(reasons)


def _mid_price(
    best_bid: OrderBookLevel | None,
    best_ask: OrderBookLevel | None,
) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    return (best_bid.price + best_ask.price) / 2


def _spread_bps(
    best_bid: OrderBookLevel | None,
    best_ask: OrderBookLevel | None,
    mid: float | None,
) -> float | None:
    if best_bid is None or best_ask is None or mid is None or mid == 0:
        return None
    return (best_ask.price - best_bid.price) / mid * 10_000


def _microprice(
    best_bid: OrderBookLevel | None,
    best_ask: OrderBookLevel | None,
) -> float | None:
    if best_bid is None or best_ask is None:
        return None
    total_quantity = best_bid.quantity + best_ask.quantity
    if total_quantity == 0:
        return None
    return (
        best_ask.price * best_bid.quantity
        + best_bid.price * best_ask.quantity
    ) / total_quantity


def _imbalance(
    bids: tuple[OrderBookLevel, ...],
    asks: tuple[OrderBookLevel, ...],
    *,
    levels: int,
) -> float | None:
    bid_quantity = sum(level.quantity for level in bids[:levels])
    ask_quantity = sum(level.quantity for level in asks[:levels])
    total_quantity = bid_quantity + ask_quantity
    if total_quantity == 0:
        return None
    return (bid_quantity - ask_quantity) / total_quantity


def _depth_in_band(
    levels: tuple[OrderBookLevel, ...],
    mid: float | None,
    *,
    bps: int,
    is_bid: bool,
) -> float | None:
    if mid is None:
        return None
    distance = bps / 10_000
    if is_bid:
        boundary = mid * (1 - distance)
        retained = (level for level in levels if level.price >= boundary)
    else:
        boundary = mid * (1 + distance)
        retained = (level for level in levels if level.price <= boundary)
    return sum(level.price * level.quantity for level in retained)


def _age_ms(as_of: datetime, exchange_at: datetime | None) -> float | None:
    if exchange_at is None:
        return None
    return (as_of - exchange_at).total_seconds() * 1_000


def _normalize_symbol(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Order book symbol cannot be empty.")
    return value.strip().upper()


def _normalize_timestamp(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return value.astimezone(UTC)
