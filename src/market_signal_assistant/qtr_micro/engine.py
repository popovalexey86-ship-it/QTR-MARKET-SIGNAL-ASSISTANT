from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal

from market_signal_assistant.qtr_micro.models import (
    EntryDecision,
    EntryPlan,
    EntrySkipReason,
    InstrumentRules,
    InstrumentUniverseStatus,
    MicroDirection,
    MicroExitReason,
    MicroPosition,
    MicroStage,
    MicroState,
    PreflightResult,
    direction_from_setup,
)
from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine.models import SetupState, SetupType

_ALLOWED_SETUP_TYPES = frozenset(
    (SetupType.BREAKOUT, SetupType.RETEST, SetupType.CONTINUATION)
)


@dataclass(frozen=True, slots=True)
class ManagementDecision:
    action: MicroExitReason | None
    close_qty: float
    new_stop: float | None = None


class QtrMicroEntryEngine:
    """Pure fail-closed entry and risk calculation."""

    def __init__(self, settings: QtrMicroSettings) -> None:
        self._settings = settings

    def prepare_entry(
        self,
        candidate: QtrSetupCandidate,
        *,
        now: datetime,
        equity: float,
        rules: InstrumentRules,
        state: MicroState,
        preflight: PreflightResult,
        expected_entry_price: float | None = None,
    ) -> EntryDecision:
        result = candidate.result
        if not self._settings.enabled:
            return _skip(EntrySkipReason.DISABLED)
        if self._settings.kill_switch:
            return _skip(EntrySkipReason.KILL_SWITCH)
        if not preflight.ready:
            return _skip(EntrySkipReason.PREFLIGHT_BLOCKED)
        if not state.trading_enabled or state.blocked_reason is not None:
            return _skip(
                EntrySkipReason.STATE_BLOCKED,
                detail=state.blocked_reason or "Local Micro state запрещает entry.",
            )
        if rules.universe_status is not InstrumentUniverseStatus.ELIGIBLE:
            return _skip(
                EntrySkipReason.UNSUPPORTED_INSTRUMENT,
                detail="Инструмент не входит в торговую вселенную QTR Micro.",
                instrument_status=rules.universe_status,
            )
        if result.setup_state is not SetupState.READY_TO_CONSIDER:
            return _skip(EntrySkipReason.INVALID_STATE)
        if not result.trade_eligible:
            return _skip(EntrySkipReason.INVALID_STATE)
        if result.data_quality != "COMPLETE" or result.technical_gap:
            return _skip(
                EntrySkipReason.INVALID_STATE,
                detail="Технические данные setup неполные или повреждены.",
            )
        if result.setup_type not in _ALLOWED_SETUP_TYPES:
            return _skip(EntrySkipReason.INVALID_TYPE)
        direction = direction_from_setup(result.direction)
        if direction is None:
            return _skip(EntrySkipReason.INVALID_DIRECTION)
        observed_at = _utc(now)
        signal_at = _utc(result.analyzed_at)
        if observed_at - signal_at > timedelta(
            seconds=self._settings.max_signal_age_seconds
        ):
            return _skip(EntrySkipReason.STALE)
        if result.current_breakout_failure or result.is_late:
            return _skip(EntrySkipReason.CURRENT_FAILURE)
        if not result.spread_ok:
            return _skip(EntrySkipReason.SPREAD)
        if (
            result.distance_to_trigger_atr is None
            or result.distance_to_trigger_atr > self._settings.max_entry_distance_atr
        ):
            return _skip(EntrySkipReason.TOO_FAR)
        if result.current_price is None or result.invalidation_level is None:
            return _skip(EntrySkipReason.STRUCTURAL_STOP_MISSING)
        if candidate.atr_value is None:
            return _skip(EntrySkipReason.ATR_MISSING)
        open_positions = tuple(
            item
            for item in state.positions.values()
            if item.stage not in {MicroStage.CLOSED, MicroStage.BLOCKED}
        )
        if any(
            item.setup_episode_id == candidate.episode_id
            for item in state.positions.values()
        ):
            return _skip(EntrySkipReason.DUPLICATE_EPISODE)
        if any(item.symbol == result.symbol for item in open_positions):
            return _skip(EntrySkipReason.SYMBOL_ALREADY_OPEN)
        if len(open_positions) >= self._settings.max_open_positions:
            return _skip(EntrySkipReason.MAX_POSITIONS)
        if state.loss_pause_until is not None and observed_at < _utc(
            state.loss_pause_until
        ):
            return _skip(EntrySkipReason.LOSS_PAUSE)
        loss_limit = state.day_start_equity * self._settings.daily_loss_limit_pct / 100
        if (
            state.trading_day == observed_at.date()
            and state.realised_daily_pnl <= -loss_limit
        ):
            return _skip(EntrySkipReason.DAILY_LOSS_LIMIT)
        entry_price = expected_entry_price or result.current_price
        stop_price = _structural_stop(
            direction,
            result.invalidation_level,
            candidate.atr_value,
            self._settings.stop_atr_buffer,
        )
        if not _valid_stop(direction, entry_price, stop_price):
            return _skip(EntrySkipReason.STRUCTURAL_STOP_MISSING)
        risk_pct = self._settings.base_risk_pct
        risk_amount = equity * risk_pct / 100
        initial_r = abs(entry_price - stop_price)
        risk_qty = risk_amount / initial_r
        notional_limit = min(
            self._settings.max_notional_usdt,
            equity * self._settings.max_notional_equity_pct / 100,
        )
        qty = _round_down(
            min(
                risk_qty,
                rules.max_market_order_qty,
                notional_limit / entry_price,
            ),
            rules.qty_step,
        )
        notional = qty * entry_price
        if qty < rules.min_order_qty or notional < rules.min_notional_value:
            return _skip(EntrySkipReason.QUANTITY_LIMITS)
        required_leverage = max(1, math.ceil(notional / equity))
        leverage = max(5, self._settings.base_leverage, required_leverage)
        leverage_limit = min(self._settings.max_leverage, rules.max_leverage)
        if leverage > leverage_limit:
            return _skip(EntrySkipReason.LEVERAGE_LIMIT)
        estimated_fees = notional * 2 * self._settings.taker_fee_rate
        estimated_fees_r_pct = estimated_fees / risk_amount * 100
        if estimated_fees_r_pct > self._settings.max_estimated_fees_r_pct:
            return _skip(
                EntrySkipReason.FEES_TOO_HIGH,
                detail=(
                    f"Estimated round-trip fees {estimated_fees_r_pct:.2f}% R exceed "
                    f"{self._settings.max_estimated_fees_r_pct:.2f}% R."
                ),
            )
        tp1_qty = _round_down(qty * self._settings.tp1_close_pct / 100, rules.qty_step)
        tp2_qty = _round_down(qty * self._settings.tp2_close_pct / 100, rules.qty_step)
        runner_qty = _round_down(qty - tp1_qty - tp2_qty, rules.qty_step)
        if min(tp1_qty, tp2_qty, runner_qty) <= 0:
            return _skip(EntrySkipReason.QUANTITY_LIMITS)
        sign = 1 if direction is MicroDirection.LONG else -1
        digest = hashlib.sha256(
            f"{result.symbol}|{candidate.episode_id}".encode()
        ).hexdigest()[:20]
        trade_id = f"QTRM-{digest}"
        plan = EntryPlan(
            trade_id=trade_id,
            setup_episode_id=candidate.episode_id,
            symbol=result.symbol,
            direction=direction,
            setup_type=result.setup_type,
            setup_confidence=result.confidence,
            signal_at=signal_at,
            signal_price=entry_price,
            entry_price=entry_price,
            stop_price=stop_price,
            risk_pct=risk_pct,
            risk_amount=risk_amount,
            qty=qty,
            leverage=leverage,
            tp1_price=entry_price + sign * self._settings.tp1_r * initial_r,
            tp1_qty=tp1_qty,
            tp2_price=entry_price + sign * self._settings.tp2_r * initial_r,
            tp2_qty=tp2_qty,
            runner_target_price=(
                entry_price + sign * self._settings.runner_initial_r * initial_r
            ),
            runner_qty=runner_qty,
            initial_r=initial_r,
            order_link_id=trade_id,
            pre_submit_price=(
                entry_price if expected_entry_price is not None else None
            ),
            notional=notional,
            estimated_round_trip_fees=estimated_fees,
            estimated_fees_r_pct=estimated_fees_r_pct,
        )
        return EntryDecision(plan, None)

    def revalidate_entry(
        self,
        candidate: QtrSetupCandidate,
        original_plan: EntryPlan,
        *,
        current_price: float,
        now: datetime,
        equity: float,
        rules: InstrumentRules,
        state: MicroState,
        preflight: PreflightResult,
    ) -> EntryDecision:
        """Re-run every entry gate and size from the last tradable price."""
        result = candidate.result
        direction = direction_from_setup(result.direction)
        if (
            candidate.episode_id != original_plan.setup_episode_id
            or result.symbol != original_plan.symbol
            or direction is not original_plan.direction
            or result.setup_type is not original_plan.setup_type
        ):
            return _skip(EntrySkipReason.REVALIDATION_FAILED)
        if current_price <= 0 or candidate.atr_value is None:
            return _skip(EntrySkipReason.REVALIDATION_FAILED)
        trigger = result.trigger_level
        if trigger is None:
            return _skip(EntrySkipReason.REVALIDATION_FAILED)
        distance_atr = abs(current_price - trigger) / candidate.atr_value
        correct_side = (
            current_price >= trigger
            if direction is MicroDirection.LONG
            else current_price <= trigger
        )
        if not correct_side or distance_atr > self._settings.max_entry_distance_atr:
            return _skip(EntrySkipReason.TOO_FAR)
        return self.prepare_entry(
            candidate,
            now=now,
            equity=equity,
            rules=rules,
            state=state,
            preflight=preflight,
            expected_entry_price=current_price,
        )

    def manage(
        self,
        position: MicroPosition,
        *,
        current_price: float,
        now: datetime,
        setup_cancelled: bool = False,
        opposite_structure: bool = False,
        structure_degraded: bool = False,
    ) -> ManagementDecision:
        if position.opened_at is None or position.average_fill is None:
            return ManagementDecision(None, 0)
        if setup_cancelled or opposite_structure:
            return ManagementDecision(
                MicroExitReason.STRUCTURE_EXIT, position.current_qty
            )
        age = _utc(now) - _utc(position.opened_at)
        r_value = _r_value(position, current_price)
        if age >= timedelta(minutes=self._settings.runner_max_hold_minutes):
            return ManagementDecision(
                MicroExitReason.RUNNER_TIME_EXIT, position.current_qty
            )
        if age >= timedelta(
            minutes=self._settings.normal_max_hold_minutes
        ) and position.stage not in {MicroStage.TP2_FILLED, MicroStage.RUNNER}:
            return ManagementDecision(MicroExitReason.TIME_EXIT, position.current_qty)
        if (
            age >= timedelta(minutes=self._settings.progress_check_minutes)
            and r_value < 0.5
            and structure_degraded
        ):
            return ManagementDecision(MicroExitReason.TIME_EXIT, position.current_qty)
        if position.stage is MicroStage.OPEN and r_value >= self._settings.tp1_r:
            return ManagementDecision(
                MicroExitReason.TP1,
                min(position.tp1_qty, position.current_qty),
                position.average_fill,
            )
        if position.stage is MicroStage.TP1_FILLED and r_value >= self._settings.tp2_r:
            return ManagementDecision(
                MicroExitReason.TP2,
                min(position.tp2_qty, position.current_qty),
            )
        if (
            position.stage in {MicroStage.TP2_FILLED, MicroStage.RUNNER}
            and r_value >= self._settings.runner_initial_r
        ):
            return ManagementDecision(
                MicroExitReason.RUNNER_TARGET, position.current_qty
            )
        return ManagementDecision(None, 0)


def _skip(
    reason: EntrySkipReason,
    *,
    detail: str | None = None,
    instrument_status: InstrumentUniverseStatus | None = None,
) -> EntryDecision:
    return EntryDecision(None, reason, detail, instrument_status)


def _structural_stop(
    direction: MicroDirection,
    invalidation: float,
    atr: float,
    buffer_atr: float,
) -> float:
    buffer = atr * buffer_atr
    if direction is MicroDirection.LONG:
        return invalidation - buffer
    return invalidation + buffer


def _valid_stop(
    direction: MicroDirection, entry_price: float, stop_price: float
) -> bool:
    if stop_price <= 0 or entry_price <= 0:
        return False
    if direction is MicroDirection.LONG:
        return stop_price < entry_price
    return stop_price > entry_price


def _round_down(value: float, step: float) -> float:
    if value <= 0 or step <= 0:
        return 0.0
    raw = Decimal(str(value)) / Decimal(str(step))
    units = raw.to_integral_value(rounding=ROUND_DOWN)
    return float(units * Decimal(str(step)))


def _r_value(position: MicroPosition, current_price: float) -> float:
    assert position.average_fill is not None
    sign = 1 if position.direction is MicroDirection.LONG else -1
    return sign * (current_price - position.average_fill) / position.initial_r


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("QTR Micro requires timezone-aware timestamps.")
    return value.astimezone(UTC)
