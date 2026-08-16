from __future__ import annotations

import math
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from enum import StrEnum

_NOTIONAL_REL_TOLERANCE = 1e-9
_NOTIONAL_ABS_TOLERANCE = 1e-12


class TradeSide(StrEnum):
    """Aggressor side reported by the public trade feed."""

    BUY = "BUY"
    SELL = "SELL"


class OrderBookEventType(StrEnum):
    SNAPSHOT = "SNAPSHOT"
    DELTA = "DELTA"


class LiquidationSide(StrEnum):
    """Side of the position which was liquidated, not the wire order side."""

    LONG = "LONG"
    SHORT = "SHORT"


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    price: float
    quantity: float

    def __post_init__(self) -> None:
        _require_positive("Order book price", self.price)
        _require_non_negative("Order book quantity", self.quantity)


@dataclass(frozen=True, slots=True)
class PublicTradeEvent:
    symbol: str
    trade_id: str
    exchange_at: datetime
    received_at: datetime
    side: TradeSide
    price: float
    quantity: float
    quote_notional: float
    sequence: int | None = None
    is_block_trade: bool = False
    is_rpi_trade: bool = False
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "trade_id", _require_text("Trade ID", self.trade_id))
        object.__setattr__(
            self,
            "exchange_at",
            _normalize_timestamp("Trade exchange_at", self.exchange_at),
        )
        object.__setattr__(
            self,
            "received_at",
            _normalize_timestamp("Trade received_at", self.received_at),
        )
        if not isinstance(self.side, TradeSide):
            raise ValueError("Trade side must be TradeSide.BUY or TradeSide.SELL.")
        _require_positive("Trade price", self.price)
        _require_positive("Trade quantity", self.quantity)
        _validate_quote_notional(self.price, self.quantity, self.quote_notional)
        _validate_optional_counter("Trade sequence", self.sequence)
        _validate_bool("is_block_trade", self.is_block_trade)
        _validate_bool("is_rpi_trade", self.is_rpi_trade)
        _validate_schema_version(self.schema_version)


@dataclass(frozen=True, slots=True)
class OrderBookEvent:
    symbol: str
    event_type: OrderBookEventType
    exchange_at: datetime
    received_at: datetime
    update_id: int
    bids: tuple[OrderBookLevel, ...] = ()
    asks: tuple[OrderBookLevel, ...] = ()
    cross_sequence: int | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(
            self,
            "exchange_at",
            _normalize_timestamp("Order book exchange_at", self.exchange_at),
        )
        object.__setattr__(
            self,
            "received_at",
            _normalize_timestamp("Order book received_at", self.received_at),
        )
        if not isinstance(self.event_type, OrderBookEventType):
            raise ValueError("Order book event type must be SNAPSHOT or DELTA.")
        _validate_counter("Order book update_id", self.update_id)
        _validate_optional_counter("Order book cross_sequence", self.cross_sequence)
        _validate_schema_version(self.schema_version)

        bids = tuple(self.bids)
        asks = tuple(self.asks)
        _validate_book_side("bid", bids, self.event_type)
        _validate_book_side("ask", asks, self.event_type)
        object.__setattr__(self, "bids", bids)
        object.__setattr__(self, "asks", asks)


@dataclass(frozen=True, slots=True)
class LiquidationEvent:
    symbol: str
    exchange_at: datetime
    received_at: datetime
    side: LiquidationSide
    bankruptcy_price: float
    quantity: float
    quote_notional: float
    liquidation_id: str | None = None
    sequence: int | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(
            self,
            "exchange_at",
            _normalize_timestamp("Liquidation exchange_at", self.exchange_at),
        )
        object.__setattr__(
            self,
            "received_at",
            _normalize_timestamp("Liquidation received_at", self.received_at),
        )
        if not isinstance(self.side, LiquidationSide):
            raise ValueError("Liquidation side must be LONG or SHORT.")
        _require_positive("Liquidation bankruptcy_price", self.bankruptcy_price)
        _require_positive("Liquidation quantity", self.quantity)
        _validate_quote_notional(
            self.bankruptcy_price,
            self.quantity,
            self.quote_notional,
        )
        if self.liquidation_id is not None:
            object.__setattr__(
                self,
                "liquidation_id",
                _require_text("Liquidation ID", self.liquidation_id),
            )
        _validate_optional_counter("Liquidation sequence", self.sequence)
        _validate_schema_version(self.schema_version)


@dataclass(frozen=True, slots=True)
class MicrostructureSnapshot:
    """Immutable, normalized input for future shadow scoring.

    Flow delta and CVD are signed. Depth and notionals use quote currency.
    Missing measurements remain ``None`` and are never converted to zero.
    """

    symbol: str
    generated_at: datetime
    window_started_at: datetime

    market_price: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    mid_price: float | None = None
    microprice: float | None = None
    spread_bps: float | None = None

    bid_depth_5bps: float | None = None
    ask_depth_5bps: float | None = None
    bid_depth_10bps: float | None = None
    ask_depth_10bps: float | None = None
    bid_depth_25bps: float | None = None
    ask_depth_25bps: float | None = None
    imbalance_l1: float | None = None
    imbalance_l5: float | None = None
    imbalance_l10: float | None = None
    imbalance_l25: float | None = None
    imbalance_l50: float | None = None

    buy_notional_1s: float | None = None
    sell_notional_1s: float | None = None
    delta_1s: float | None = None
    delta_5s: float | None = None
    delta_15s: float | None = None
    delta_60s: float | None = None
    cvd_process: float | None = None
    cvd_utc_day: float | None = None
    cvd_episode: float | None = None
    trade_count_5s: int | None = None
    largest_trade_5s: float | None = None

    long_liquidations_5s: float | None = None
    short_liquidations_5s: float | None = None
    long_liquidations_60s: float | None = None
    short_liquidations_60s: float | None = None
    liquidation_imbalance_60s: float | None = None

    book_exchange_at: datetime | None = None
    trade_exchange_at: datetime | None = None
    liquidation_exchange_at: datetime | None = None
    book_age_ms: float | None = None
    trade_age_ms: float | None = None
    liquidation_age_ms: float | None = None
    dropped_events: int = 0
    reconnect_count: int = 0
    ready: bool = False
    health_reasons: tuple[str, ...] = ("snapshot_not_ready",)
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        generated_at = _normalize_timestamp("Snapshot generated_at", self.generated_at)
        window_started_at = _normalize_timestamp(
            "Snapshot window_started_at", self.window_started_at
        )
        if window_started_at > generated_at:
            raise ValueError("Snapshot window cannot start after generated_at.")
        object.__setattr__(self, "generated_at", generated_at)
        object.__setattr__(self, "window_started_at", window_started_at)
        _normalize_optional_snapshot_timestamps(self)
        _validate_snapshot_numbers(self)
        _validate_counter("Snapshot dropped_events", self.dropped_events)
        _validate_counter("Snapshot reconnect_count", self.reconnect_count)
        _validate_bool("Snapshot ready", self.ready)
        _validate_schema_version(self.schema_version)

        reasons = tuple(
            _require_text("Health reason", item) for item in self.health_reasons
        )
        if len(reasons) != len(set(reasons)):
            raise ValueError("Snapshot health reasons must be unique.")
        object.__setattr__(self, "health_reasons", reasons)
        if self.ready:
            if reasons:
                raise ValueError("Ready snapshot cannot contain health reasons.")
            self._validate_ready_fields()
        elif not reasons:
            raise ValueError("Not-ready snapshot must explain its health reasons.")

    def _validate_ready_fields(self) -> None:
        required = (
            self.market_price,
            self.best_bid,
            self.best_ask,
            self.mid_price,
            self.spread_bps,
            self.book_exchange_at,
            self.trade_exchange_at,
            self.delta_1s,
            self.delta_5s,
            self.delta_15s,
            self.delta_60s,
        )
        if any(value is None for value in required):
            raise ValueError("Ready snapshot is missing mandatory market data.")
        if (
            self.best_bid is not None
            and self.best_ask is not None
            and self.best_bid >= self.best_ask
        ):
            raise ValueError("Ready snapshot order book must not be crossed.")


_POSITIVE_SNAPSHOT_FIELDS = frozenset(
    {
        "market_price",
        "best_bid",
        "best_ask",
        "mid_price",
        "microprice",
    }
)
_NON_NEGATIVE_SNAPSHOT_FIELDS = frozenset(
    {
        "spread_bps",
        "bid_depth_5bps",
        "ask_depth_5bps",
        "bid_depth_10bps",
        "ask_depth_10bps",
        "bid_depth_25bps",
        "ask_depth_25bps",
        "buy_notional_1s",
        "sell_notional_1s",
        "largest_trade_5s",
        "long_liquidations_5s",
        "short_liquidations_5s",
        "long_liquidations_60s",
        "short_liquidations_60s",
        "book_age_ms",
        "trade_age_ms",
        "liquidation_age_ms",
    }
)
_IMBALANCE_SNAPSHOT_FIELDS = frozenset(
    {
        "imbalance_l1",
        "imbalance_l5",
        "imbalance_l10",
        "imbalance_l25",
        "imbalance_l50",
        "liquidation_imbalance_60s",
    }
)
_SIGNED_SNAPSHOT_FIELDS = frozenset(
    {
        "delta_1s",
        "delta_5s",
        "delta_15s",
        "delta_60s",
        "cvd_process",
        "cvd_utc_day",
        "cvd_episode",
    }
)


def _validate_snapshot_numbers(snapshot: MicrostructureSnapshot) -> None:
    for model_field in fields(snapshot):
        name = model_field.name
        value = getattr(snapshot, name)
        if name in _POSITIVE_SNAPSHOT_FIELDS and value is not None:
            _require_positive(f"Snapshot {name}", value)
        elif name in _NON_NEGATIVE_SNAPSHOT_FIELDS and value is not None:
            _require_non_negative(f"Snapshot {name}", value)
        elif name in _IMBALANCE_SNAPSHOT_FIELDS and value is not None:
            _require_finite(f"Snapshot {name}", value)
            if not -1.0 <= value <= 1.0:
                raise ValueError(f"Snapshot {name} must be between -1 and 1.")
        elif name in _SIGNED_SNAPSHOT_FIELDS and value is not None:
            _require_finite(f"Snapshot {name}", value)
    if snapshot.trade_count_5s is not None:
        _validate_counter("Snapshot trade_count_5s", snapshot.trade_count_5s)


def _normalize_optional_snapshot_timestamps(snapshot: MicrostructureSnapshot) -> None:
    for name in (
        "book_exchange_at",
        "trade_exchange_at",
        "liquidation_exchange_at",
    ):
        value = getattr(snapshot, name)
        if value is not None:
            object.__setattr__(
                snapshot,
                name,
                _normalize_timestamp(f"Snapshot {name}", value),
            )


def _validate_book_side(
    name: str,
    levels: tuple[OrderBookLevel, ...],
    event_type: OrderBookEventType,
) -> None:
    if any(not isinstance(level, OrderBookLevel) for level in levels):
        raise ValueError(f"Order book {name}s must contain OrderBookLevel values.")
    prices = tuple(level.price for level in levels)
    if len(prices) != len(set(prices)):
        raise ValueError(f"Order book {name}s contain duplicate prices.")
    if event_type is OrderBookEventType.SNAPSHOT and any(
        level.quantity == 0 for level in levels
    ):
        raise ValueError("Order book snapshot quantities must be positive.")


def _normalize_symbol(value: str) -> str:
    return _require_text("Symbol", value).upper()


def _require_text(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be empty.")
    return value.strip()


def _normalize_timestamp(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return value.astimezone(UTC)


def _require_finite(name: str, value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number.")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number.")


def _require_positive(name: str, value: float) -> None:
    _require_finite(name, value)
    if value <= 0:
        raise ValueError(f"{name} must be positive.")


def _require_non_negative(name: str, value: float) -> None:
    _require_finite(name, value)
    if value < 0:
        raise ValueError(f"{name} cannot be negative.")


def _validate_quote_notional(price: float, quantity: float, value: float) -> None:
    _require_positive("Quote notional", value)
    expected = price * quantity
    if not math.isclose(
        value,
        expected,
        rel_tol=_NOTIONAL_REL_TOLERANCE,
        abs_tol=_NOTIONAL_ABS_TOLERANCE,
    ):
        raise ValueError("Quote notional must equal price multiplied by quantity.")


def _validate_counter(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")


def _validate_optional_counter(name: str, value: int | None) -> None:
    if value is not None:
        _validate_counter(name, value)


def _validate_schema_version(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("Schema version must be a positive integer.")


def _validate_bool(name: str, value: bool) -> None:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be boolean.")
