from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum

from market_signal_assistant.setup_engine.models import SetupDirection, SetupType


class MicroDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class MicroStage(StrEnum):
    PREPARED = "PREPARED"
    ENTRY_ACKNOWLEDGED = "ENTRY_ACKNOWLEDGED"
    OPEN = "OPEN"
    TP1_FILLED = "TP1_FILLED"
    TP2_FILLED = "TP2_FILLED"
    RUNNER = "RUNNER"
    EXIT_ACKNOWLEDGED = "EXIT_ACKNOWLEDGED"
    CLOSED = "CLOSED"
    BLOCKED = "BLOCKED"


class MicroExitReason(StrEnum):
    STOP = "STOP"
    TP1 = "TP1"
    TP2 = "TP2"
    RUNNER_TARGET = "RUNNER_TARGET"
    TIME_EXIT = "TIME_EXIT"
    RUNNER_TIME_EXIT = "RUNNER_TIME_EXIT"
    STRUCTURE_EXIT = "STRUCTURE_EXIT"
    STOP_PROTECTION_FAILED = "STOP_PROTECTION_FAILED"


class EntrySkipReason(StrEnum):
    DISABLED = "disabled"
    KILL_SWITCH = "kill_switch"
    PREFLIGHT_BLOCKED = "preflight_blocked"
    STATE_BLOCKED = "state_blocked"
    INVALID_STATE = "invalid_state"
    INVALID_TYPE = "invalid_type"
    INVALID_DIRECTION = "invalid_direction"
    STALE = "stale"
    TOO_FAR = "too_far"
    CURRENT_FAILURE = "current_failure"
    SPREAD = "spread"
    STRUCTURAL_STOP_MISSING = "structural_stop_missing"
    ATR_MISSING = "atr_missing"
    DUPLICATE_EPISODE = "duplicate_episode"
    SYMBOL_ALREADY_OPEN = "symbol_already_open"
    MAX_POSITIONS = "max_positions"
    LOSS_PAUSE = "loss_pause"
    DAILY_LOSS_LIMIT = "daily_loss_limit"
    QUANTITY_LIMITS = "quantity_limits"
    LEVERAGE_LIMIT = "leverage_limit"
    UNSUPPORTED_INSTRUMENT = "unsupported_instrument"
    FEES_TOO_HIGH = "fees_too_high"
    NOTIONAL_LIMIT = "notional_limit"
    REVALIDATION_FAILED = "revalidation_failed"


class LeverageUpdateResult(StrEnum):
    CHANGED = "changed"
    ALREADY_SET = "already_set"


class InstrumentUniverseStatus(StrEnum):
    ELIGIBLE = "eligible"
    STOCK = "stock"
    PRELAUNCH = "prelaunch"
    NON_USDT = "non_usdt"
    UNSUPPORTED_CONTRACT = "unsupported_contract"
    UNSUPPORTED_STATUS = "unsupported_status"


@dataclass(frozen=True, slots=True)
class InstrumentRules:
    symbol: str
    qty_step: float
    min_order_qty: float
    max_market_order_qty: float
    min_notional_value: float
    max_leverage: int = 100
    universe_status: InstrumentUniverseStatus = InstrumentUniverseStatus.ELIGIBLE


@dataclass(frozen=True, slots=True)
class EntryPlan:
    trade_id: str
    setup_episode_id: str
    symbol: str
    direction: MicroDirection
    setup_type: SetupType
    setup_confidence: float
    signal_at: datetime
    signal_price: float
    entry_price: float
    stop_price: float
    risk_pct: float
    risk_amount: float
    qty: float
    leverage: int
    tp1_price: float
    tp1_qty: float
    tp2_price: float
    tp2_qty: float
    runner_target_price: float
    runner_qty: float
    initial_r: float
    order_link_id: str
    pre_submit_price: float | None = None
    notional: float = 0.0
    estimated_round_trip_fees: float = 0.0
    estimated_fees_r_pct: float = 0.0


@dataclass(frozen=True, slots=True)
class EntryDecision:
    plan: EntryPlan | None
    skip_reason: EntrySkipReason | None
    skip_detail: str | None = None
    instrument_status: InstrumentUniverseStatus | None = None

    @property
    def accepted(self) -> bool:
        return self.plan is not None


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    check_id: str
    label: str
    passed: bool
    detail: str
    blocking: bool = True
    informational: bool = False


@dataclass(frozen=True, slots=True)
class PreflightResult:
    ready: bool
    reason: str | None
    equity: float | None = None
    account_type: str | None = None
    position_mode: str | None = None
    open_positions: int = 0
    active_orders: int = 0
    mode: str = "demo"
    host: str = "api-demo.bybit.com"
    symbol: str = "BTCUSDT"
    base_leverage: int = 5
    max_allowed_leverage: int = 10
    instrument_rules: InstrumentRules | None = None
    qtr_positions: int = 0
    qtr_orders: int = 0
    foreign_positions: int = 0
    foreign_orders: int = 0
    reconciliation_ok: bool = False
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    checks: tuple[PreflightCheck, ...] = ()


@dataclass(frozen=True, slots=True)
class OrderAcknowledgement:
    order_id: str
    order_link_id: str
    accepted: bool


@dataclass(frozen=True, slots=True)
class ExecutionFill:
    order_id: str
    average_price: float
    filled_qty: float
    fee: float
    filled_at: datetime


@dataclass(frozen=True, slots=True)
class DemoPosition:
    symbol: str
    side: str
    size: float
    average_price: float
    position_idx: int = 0


@dataclass(frozen=True, slots=True)
class DemoOrder:
    symbol: str
    order_id: str
    order_link_id: str
    side: str
    qty: float
    status: str


@dataclass(frozen=True, slots=True)
class MicroPosition:
    trade_id: str
    setup_episode_id: str
    symbol: str
    direction: MicroDirection
    setup_type: SetupType
    setup_confidence: float
    entry_order_link_id: str
    entry_order_id: str | None
    average_fill: float | None
    filled_qty: float
    initial_qty: float
    current_qty: float
    leverage: int
    risk_pct: float
    risk_amount: float
    structural_stop: float
    current_stop: float
    initial_r: float
    tp1_price: float
    tp1_qty: float
    tp2_price: float
    tp2_qty: float
    runner_target_price: float
    runner_qty: float
    realised_partial_pnl: float
    fees: float
    opened_at: datetime | None
    last_updated: datetime
    stage: MicroStage
    signal_at: datetime
    signal_price: float
    max_favorable_price: float | None = None
    max_adverse_price: float | None = None
    order_submitted_at: datetime | None = None
    pending_exit_order_id: str | None = None
    pending_exit_order_link_id: str | None = None
    pending_exit_reason: MicroExitReason | None = None
    pending_exit_qty: float = 0.0
    pending_new_stop: float | None = None
    tp1_fill_price: float | None = None
    tp2_fill_price: float | None = None
    runner_exit_price: float | None = None
    pre_submit_price: float | None = None
    planned_notional: float = 0.0
    actual_risk_at_fill: float | None = None
    actual_risk_pct: float | None = None
    entry_fees: float = 0.0
    exit_fees: float = 0.0
    funding: float | None = None
    journaled: bool = False


@dataclass(frozen=True, slots=True)
class MicroState:
    updated_at: datetime | None
    trading_enabled: bool
    trading_day: date
    day_start_equity: float
    realised_daily_pnl: float
    consecutive_losses: int
    loss_pause_until: datetime | None
    positions: dict[str, MicroPosition]
    blocked_reason: str | None = None


def direction_from_setup(value: SetupDirection) -> MicroDirection | None:
    if value is SetupDirection.UP:
        return MicroDirection.LONG
    if value is SetupDirection.DOWN:
        return MicroDirection.SHORT
    return None


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("QTR Micro timestamp must be timezone-aware.")
    return value.astimezone(UTC)
