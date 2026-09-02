from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType


class ImpulseDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"


class SweepDirection(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    NONE = "NONE"


class V3TradeStage(StrEnum):
    OPEN = "OPEN"
    RUNNER = "RUNNER"
    CLOSED = "CLOSED"


class V3ExitReason(StrEnum):
    CASH_TARGET = "CASH_TARGET"
    CASH_STOP = "CASH_STOP"
    TIME_STOP = "TIME_STOP"
    DIRECTIONAL_FAILURE = "DIRECTIONAL_FAILURE"
    RUNNER_TARGET = "RUNNER_TARGET"
    RUNNER_STOP = "RUNNER_STOP"


@dataclass(frozen=True, slots=True)
class CashCostEstimate:
    fee_bps: float
    spread_bps: float
    slippage_bps: float
    total_round_trip_bps: float
    total_round_trip_pct: float
    expected_cash: float

    def __post_init__(self) -> None:
        for name, value in (
            ("fee_bps", self.fee_bps),
            ("spread_bps", self.spread_bps),
            ("slippage_bps", self.slippage_bps),
            ("total_round_trip_bps", self.total_round_trip_bps),
            ("total_round_trip_pct", self.total_round_trip_pct),
            ("expected_cash", self.expected_cash),
        ):
            _non_negative(name, value)


@dataclass(frozen=True, slots=True)
class ImpulseSnapshot:
    """Causal V3 entry input expressed in native prices, bps and notionals."""

    symbol: str
    observed_at: datetime
    source_at: datetime
    impulse_id: str
    impulse_started_at: datetime
    direction: ImpulseDirection
    market_price: float
    best_bid: float
    best_ask: float
    spread_bps: float
    bid_depth_10bps: float
    ask_depth_10bps: float
    delta_1s: float
    delta_5s: float
    delta_15s: float
    flow_imbalance_5s: float
    flow_acceleration: float
    price_displacement_1s_bps: float
    price_displacement_5s_bps: float
    price_displacement_15s_bps: float
    impulse_displacement_bps: float
    price_response_bps_per_10k: float
    estimated_potential_bps: float
    local_volatility_bps: float
    orderbook_imbalance: float
    sweep_direction: SweepDirection
    absorption_detected: bool
    trigger_progress_atr: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "impulse_id", _text("impulse_id", self.impulse_id))
        observed = _utc("observed_at", self.observed_at)
        source = _utc("source_at", self.source_at)
        started = _utc("impulse_started_at", self.impulse_started_at)
        if source > observed or started > observed:
            raise ValueError("Causal V3 timestamps cannot be in the future.")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "source_at", source)
        object.__setattr__(self, "impulse_started_at", started)
        for name, value in (
            ("market_price", self.market_price),
            ("best_bid", self.best_bid),
            ("best_ask", self.best_ask),
            ("bid_depth_10bps", self.bid_depth_10bps),
            ("ask_depth_10bps", self.ask_depth_10bps),
        ):
            _positive(name, value)
        if self.best_bid >= self.best_ask:
            raise ValueError("V3 order book must not be crossed.")
        for name, value in (
            ("spread_bps", self.spread_bps),
            ("flow_acceleration", self.flow_acceleration),
            ("impulse_displacement_bps", self.impulse_displacement_bps),
            ("price_response_bps_per_10k", self.price_response_bps_per_10k),
            ("estimated_potential_bps", self.estimated_potential_bps),
            ("local_volatility_bps", self.local_volatility_bps),
        ):
            _non_negative(name, value)
        for name, value in (
            ("delta_1s", self.delta_1s),
            ("delta_5s", self.delta_5s),
            ("delta_15s", self.delta_15s),
            ("flow_imbalance_5s", self.flow_imbalance_5s),
            ("price_displacement_1s_bps", self.price_displacement_1s_bps),
            ("price_displacement_5s_bps", self.price_displacement_5s_bps),
            ("price_displacement_15s_bps", self.price_displacement_15s_bps),
            ("orderbook_imbalance", self.orderbook_imbalance),
        ):
            _finite(name, value)
        if not -1.0 <= self.flow_imbalance_5s <= 1.0:
            raise ValueError("flow_imbalance_5s must be between -1 and 1.")
        if not -1.0 <= self.orderbook_imbalance <= 1.0:
            raise ValueError("orderbook_imbalance must be between -1 and 1.")
        if self.trigger_progress_atr is not None:
            _non_negative("trigger_progress_atr", self.trigger_progress_atr)

    @property
    def source_age_ms(self) -> float:
        return (self.observed_at - self.source_at).total_seconds() * 1_000

    @property
    def impulse_age_seconds(self) -> float:
        return (self.observed_at - self.impulse_started_at).total_seconds()


@dataclass(frozen=True, slots=True)
class V3EntryDecision:
    accepted: bool
    direction: ImpulseDirection
    evaluated_at: datetime
    impulse_id: str
    entry_price: float | None
    target_price: float | None
    stop_price: float | None
    break_even_price: float | None
    cost: CashCostEstimate
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    blocking_reasons: tuple[str, ...]
    snapshot: ImpulseSnapshot

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evaluated_at",
            _utc("evaluated_at", self.evaluated_at),
        )
        for name in ("entry_price", "target_price", "stop_price", "break_even_price"):
            value = getattr(self, name)
            if value is not None:
                _positive(name, value)
        if self.accepted and (
            self.direction is ImpulseDirection.NONE
            or self.entry_price is None
            or self.target_price is None
            or self.stop_price is None
            or self.break_even_price is None
            or self.blocking_reasons
        ):
            raise ValueError("Accepted V3 decision is incomplete or blocked.")


@dataclass(frozen=True, slots=True)
class V3PriceObservation:
    symbol: str
    observed_at: datetime
    price: float
    directional_failure: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "observed_at", _utc("observed_at", self.observed_at))
        _positive("price", self.price)


@dataclass(frozen=True, slots=True)
class V3ShadowTrade:
    trade_id: str
    symbol: str
    impulse_id: str
    direction: ImpulseDirection
    stage: V3TradeStage
    entry_at: datetime
    entry_price: float
    stop_price: float
    target_price: float
    runner_target_price: float
    runner_stop_price: float
    round_trip_cost_pct: float
    runner_fraction: float
    remaining_fraction: float
    last_observed_at: datetime
    last_price: float
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    primary_exit_at: datetime | None = None
    exit_at: datetime | None = None
    exit_price: float | None = None
    exit_reason: V3ExitReason | None = None
    gross_return_pct: float = 0.0
    transaction_cost_pct: float = 0.0
    net_return_pct: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        for name in ("entry_at", "last_observed_at", "primary_exit_at", "exit_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(name, value))
        for name in (
            "entry_price",
            "stop_price",
            "target_price",
            "runner_target_price",
            "runner_stop_price",
            "last_price",
        ):
            _positive(name, getattr(self, name))
        if self.exit_price is not None:
            _positive("exit_price", self.exit_price)
        for name in ("runner_fraction", "remaining_fraction"):
            value = getattr(self, name)
            _finite(name, value)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1.")
        _non_negative("round_trip_cost_pct", self.round_trip_cost_pct)
        _non_negative("mfe_pct", self.mfe_pct)
        _non_negative("mae_pct", self.mae_pct)


@dataclass(frozen=True, slots=True)
class V3TradeUpdate:
    trade: V3ShadowTrade
    changed: bool


@dataclass(frozen=True, slots=True)
class V3EntryTelemetry:
    record_id: str
    recorded_at: datetime
    symbol: str
    direction: ImpulseDirection
    impulse_id: str
    snapshot: ImpulseSnapshot
    notional: float
    entry_price: float
    target_price: float
    stop_price: float
    estimated_round_trip_cost_bps: float
    estimated_round_trip_cost_pct: float
    estimated_round_trip_cost_cash: float


@dataclass(frozen=True, slots=True)
class V3ForwardOutcome:
    record_id: str
    entry_id: str
    symbol: str
    direction: ImpulseDirection
    entry_at: datetime
    measured_at: datetime
    window_seconds: int
    mfe_pct: float
    mae_pct: float
    gross_hypothetical_pct: float
    transaction_cost_pct: float
    net_hypothetical_pct: float
    reached_025: bool
    reached_050: bool
    time_to_025_seconds: float | None
    time_to_050_seconds: float | None


@dataclass(frozen=True, slots=True)
class V3TradeRecord:
    record_id: str
    recorded_at: datetime
    trade_id: str
    symbol: str
    impulse_id: str
    direction: ImpulseDirection
    entry_at: datetime
    exit_at: datetime | None
    entry_price: float
    exit_price: float | None
    exit_reason: V3ExitReason | None
    gross_return_pct: float
    transaction_cost_pct: float
    net_return_pct: float
    mfe_pct: float
    mae_pct: float


@dataclass(frozen=True, slots=True)
class V3DirectionStats:
    trade_count: int
    gross_return_pct: float
    transaction_cost_pct: float
    net_return_pct: float
    mean_net_per_trade_pct: float
    median_net_per_trade_pct: float
    win_rate: float
    profit_factor: float | None
    max_drawdown_pct: float


@dataclass(frozen=True, slots=True)
class V3AnalyticsSnapshot:
    trade_count: int
    long_count: int
    short_count: int
    gross_return_pct: float
    transaction_cost_pct: float
    net_return_pct: float
    mean_net_per_trade_pct: float
    median_net_per_trade_pct: float
    win_rate: float
    profit_factor: float | None
    max_drawdown_pct: float
    reach_025_by_seconds: Mapping[int, float] = field(default_factory=dict)
    reach_050_by_seconds: Mapping[int, float] = field(default_factory=dict)
    by_direction: Mapping[ImpulseDirection, V3DirectionStats] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reach_025_by_seconds",
            MappingProxyType(dict(self.reach_025_by_seconds)),
        )
        object.__setattr__(
            self,
            "reach_050_by_seconds",
            MappingProxyType(dict(self.reach_050_by_seconds)),
        )
        object.__setattr__(
            self,
            "by_direction",
            MappingProxyType(dict(self.by_direction)),
        )


def _symbol(value: str) -> str:
    return _text("symbol", value).upper()


def _text(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be empty.")
    return value.strip()


def _utc(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(name: str, value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be finite.")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite.")


def _positive(name: str, value: float) -> None:
    _finite(name, value)
    if value <= 0:
        raise ValueError(f"{name} must be positive.")


def _non_negative(name: str, value: float) -> None:
    _finite(name, value)
    if value < 0:
        raise ValueError(f"{name} cannot be negative.")
