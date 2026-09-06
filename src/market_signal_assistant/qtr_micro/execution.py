from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from market_signal_assistant.qtr_micro.client import DemoApiError, DemoTradingClient
from market_signal_assistant.qtr_micro.engine import (
    ManagementDecision,
    QtrMicroEntryEngine,
    _round_down,
)
from market_signal_assistant.qtr_micro.journal import (
    JsonlQtrMicroTradeJournal,
    TradeJournalEntry,
)
from market_signal_assistant.qtr_micro.models import (
    EntryPlan,
    ExecutionFill,
    InstrumentRules,
    MicroDirection,
    MicroExitReason,
    MicroPosition,
    MicroStage,
    MicroState,
    OrderAcknowledgement,
)
from market_signal_assistant.qtr_micro.runtime_audit import (
    JsonlQtrMicroRuntimeAudit,
    QtrMicroRuntimeAuditRecord,
    QtrMicroRuntimeEvent,
)
from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
from market_signal_assistant.qtr_micro.state import JsonQtrMicroStateStore

_LOGGER = logging.getLogger(__name__)
STOP_INSTALL_ATTEMPTS = 3
JOURNAL_RECOVERY_BLOCK = (
    "Закрытая owned позиция не имеет подтверждённого exit fill "
    "для exactly-once journal."
)


class QtrMicroExecutionService:
    """Two-phase Demo entry and protective position management."""

    def __init__(
        self,
        *,
        settings: QtrMicroSettings,
        client: DemoTradingClient,
        state_store: JsonQtrMicroStateStore,
        engine: QtrMicroEntryEngine,
        journal: JsonlQtrMicroTradeJournal | None = None,
        runtime_audit: JsonlQtrMicroRuntimeAudit | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._client = client
        self._state_store = state_store
        self._engine = engine
        self._journal = journal
        self._runtime_audit = runtime_audit
        self._sleeper = sleeper
        self._monotonic = monotonic

    def submit_and_confirm_entry(
        self,
        plan: EntryPlan,
        now: datetime,
        rules: InstrumentRules,
    ) -> MicroPosition:
        """Submit once and confirm within a short bounded window."""
        position = self.submit_entry(plan, now)
        if position.stage is not MicroStage.ENTRY_ACKNOWLEDGED:
            return position
        deadline = self._monotonic() + self._settings.fill_confirmation_timeout_seconds
        while True:
            self._audit(
                QtrMicroRuntimeEvent.ENTRY_CONFIRMATION_POLL,
                position,
                _utc(now),
            )
            confirmed = self.confirm_entry(plan.trade_id, now, rules=rules)
            if (
                confirmed is not None
                and confirmed.stage is not MicroStage.ENTRY_ACKNOWLEDGED
            ):
                return confirmed
            if self._monotonic() >= deadline:
                assert position.entry_order_id is not None
                self._client.cancel_order(position.symbol, position.entry_order_id)
                try:
                    self.reconcile(now)
                except DemoApiError as error:
                    _LOGGER.warning(
                        "Entry timeout reconciliation failed safely: %s.",
                        _typed_demo_reason(error),
                    )
                state = self._state_store.load(
                    today=_utc(now).date(), trading_enabled=self._settings.enabled
                )
                timed_out = replace(
                    position,
                    current_qty=0.0,
                    stage=MicroStage.CLOSED,
                    last_updated=_utc(now),
                )
                self._save_position(state, timed_out, _utc(now))
                self._audit(
                    QtrMicroRuntimeEvent.ENTRY_CONFIRMATION_TIMEOUT,
                    timed_out,
                    _utc(now),
                    reason=(
                        "Entry order was not filled before the confirmation timeout."
                    ),
                )
                return timed_out
            self._sleeper(self._settings.fill_poll_interval_seconds)

    def submit_entry(self, plan: EntryPlan, now: datetime) -> MicroPosition:
        timestamp = _utc(now)
        state = self._state_store.load(
            today=timestamp.date(), trading_enabled=self._settings.enabled
        )
        if not state.trading_enabled or state.blocked_reason is not None:
            raise DemoApiError("QTR Micro state запрещает новый entry.")
        if plan.trade_id in state.positions:
            existing = state.positions[plan.trade_id]
            self._audit(
                QtrMicroRuntimeEvent.ENTRY_ABORTED,
                existing,
                timestamp,
                reason=f"Повторная отправка запрещена; stage={existing.stage.value}.",
            )
            return state.positions[plan.trade_id]
        prepared = _position_from_plan(plan, timestamp)
        self._save_position(state, prepared, timestamp)
        self._audit(QtrMicroRuntimeEvent.PREPARED, prepared, timestamp)
        order_attempted = False
        try:
            leverage_result = self._client.set_leverage(plan.symbol, plan.leverage)
            self._audit(
                QtrMicroRuntimeEvent.LEVERAGE_READY,
                prepared,
                timestamp,
                detail=leverage_result.value,
            )
            order_attempted = True
            self._audit(
                QtrMicroRuntimeEvent.ENTRY_SUBMIT_ATTEMPT,
                prepared,
                timestamp,
            )
            acknowledgement = self._client.create_market_order(
                symbol=plan.symbol,
                side="Buy" if plan.direction is MicroDirection.LONG else "Sell",
                qty=plan.qty,
                order_link_id=plan.order_link_id,
            )
        except Exception as error:
            event = (
                QtrMicroRuntimeEvent.ENTRY_REJECTED
                if order_attempted
                else QtrMicroRuntimeEvent.ENTRY_ABORTED
            )
            self._audit(
                event,
                prepared,
                timestamp,
                reason=_typed_demo_reason(error),
                ret_code=(error.ret_code if isinstance(error, DemoApiError) else None),
            )
            raise
        acknowledged = replace(
            prepared,
            entry_order_id=acknowledgement.order_id,
            stage=MicroStage.ENTRY_ACKNOWLEDGED,
            order_submitted_at=timestamp,
            last_updated=timestamp,
        )
        self._save_position(state, acknowledged, timestamp)
        self._audit(QtrMicroRuntimeEvent.ENTRY_ACK, acknowledged, timestamp)
        return acknowledged

    def confirm_entry(
        self,
        trade_id: str,
        now: datetime,
        *,
        rules: InstrumentRules | None = None,
    ) -> MicroPosition | None:
        timestamp = _utc(now)
        state = self._state_store.load(
            today=timestamp.date(), trading_enabled=self._settings.enabled
        )
        position = state.positions.get(trade_id)
        if position is None or position.entry_order_id is None:
            return None
        fill = self._client.execution_fill(position.entry_order_id, position.symbol)
        if fill is None:
            self._audit(
                QtrMicroRuntimeEvent.ENTRY_CONFIRMATION_WAIT,
                position,
                timestamp,
            )
            return None
        if fill.filled_qty < position.initial_qty:
            self._client.cancel_order(position.symbol, position.entry_order_id)
        self._audit(QtrMicroRuntimeEvent.ENTRY_FILLED, position, timestamp)
        filled_r = abs(fill.average_price - position.structural_stop)
        valid_fill = filled_r > 0 and (
            fill.average_price > position.structural_stop
            if position.direction is MicroDirection.LONG
            else fill.average_price < position.structural_stop
        )
        if not valid_fill:
            unsafe = replace(
                position,
                average_fill=fill.average_price,
                filled_qty=fill.filled_qty,
                current_qty=fill.filled_qty,
            )
            try:
                self._emergency_close(unsafe)
            except Exception as error:
                _LOGGER.critical(
                    "Не удалось подтвердить emergency close unsafe fill (%s).",
                    type(error).__name__,
                )
            records = dict(state.positions)
            records[trade_id] = replace(
                unsafe, stage=MicroStage.BLOCKED, last_updated=timestamp
            )
            self._state_store.save(
                replace(
                    state,
                    updated_at=timestamp,
                    trading_enabled=False,
                    blocked_reason="Fill оказался за structural stop.",
                    positions=records,
                )
            )
            self._audit(
                QtrMicroRuntimeEvent.ENTRY_ABORTED,
                records[trade_id],
                timestamp,
                reason="Fill оказался за structural stop.",
            )
            return records[trade_id]
        actual_risk = fill.filled_qty * filled_r
        equity = (
            position.risk_amount / (position.risk_pct / 100)
            if position.risk_pct > 0
            else 0.0
        )
        allowed_risk = min(
            equity * self._settings.max_risk_pct / 100,
            position.risk_amount * (1 + self._settings.actual_risk_tolerance_pct / 100),
        )
        self._audit(
            QtrMicroRuntimeEvent.ACTUAL_RISK_CHECK,
            position,
            timestamp,
            detail=f"actual={actual_risk:.8f}; allowed={allowed_risk:.8f}",
        )
        effective_qty = fill.filled_qty
        if actual_risk > allowed_risk:
            safe_qty = (
                _round_down(allowed_risk / filled_r, rules.qty_step)
                if rules is not None
                else 0.0
            )
            can_resize = bool(
                rules is not None
                and safe_qty >= rules.min_order_qty
                and safe_qty * fill.average_price >= rules.min_notional_value
                and safe_qty < fill.filled_qty
            )
            if can_resize:
                resize_qty = fill.filled_qty - safe_qty
                acknowledgement = self._client.create_market_order(
                    symbol=position.symbol,
                    side=(
                        "Sell" if position.direction is MicroDirection.LONG else "Buy"
                    ),
                    qty=resize_qty,
                    order_link_id=f"{position.trade_id}-RSZ",
                    reduce_only=True,
                )
                resize_fill = self._client.execution_fill(
                    acknowledgement.order_id, position.symbol
                )
                if resize_fill is not None and resize_fill.filled_qty >= resize_qty:
                    effective_qty = safe_qty
                    self._audit(
                        QtrMicroRuntimeEvent.ACTUAL_RISK_RESIZE,
                        position,
                        timestamp,
                        detail=f"qty={fill.filled_qty:.8f}->{safe_qty:.8f}",
                    )
                else:
                    can_resize = False
            if not can_resize:
                unsafe = replace(
                    position,
                    average_fill=fill.average_price,
                    filled_qty=fill.filled_qty,
                    current_qty=fill.filled_qty,
                    entry_fees=fill.fee,
                    fees=position.fees + fill.fee,
                    actual_risk_at_fill=actual_risk,
                    actual_risk_pct=(actual_risk / equity * 100 if equity else None),
                )
                self._emergency_close(unsafe)
                blocked = replace(
                    unsafe, stage=MicroStage.BLOCKED, last_updated=timestamp
                )
                records = dict(state.positions)
                records[trade_id] = blocked
                self._state_store.save(
                    replace(
                        state,
                        updated_at=timestamp,
                        trading_enabled=False,
                        blocked_reason="Actual fill risk exceeded the hard limit.",
                        positions=records,
                    )
                )
                self._audit(
                    QtrMicroRuntimeEvent.ACTUAL_RISK_EXCEEDED,
                    blocked,
                    timestamp,
                    reason="Actual fill risk exceeded the hard limit.",
                )
                return blocked
        sign = 1 if position.direction is MicroDirection.LONG else -1
        tp1_qty = position.tp1_qty
        tp2_qty = position.tp2_qty
        runner_qty = position.runner_qty
        if rules is not None and effective_qty != position.initial_qty:
            tp1_qty = _round_down(
                effective_qty * self._settings.tp1_close_pct / 100,
                rules.qty_step,
            )
            tp2_qty = _round_down(
                effective_qty * self._settings.tp2_close_pct / 100,
                rules.qty_step,
            )
            runner_qty = _round_down(
                effective_qty - tp1_qty - tp2_qty,
                rules.qty_step,
            )
            if min(tp1_qty, tp2_qty, runner_qty) <= 0:
                unsafe = replace(
                    position,
                    average_fill=fill.average_price,
                    filled_qty=effective_qty,
                    current_qty=effective_qty,
                    entry_fees=fill.fee,
                    fees=position.fees + fill.fee,
                    actual_risk_at_fill=effective_qty * filled_r,
                    actual_risk_pct=(
                        effective_qty * filled_r / equity * 100 if equity else None
                    ),
                )
                self._emergency_close(unsafe)
                blocked = replace(
                    unsafe, stage=MicroStage.BLOCKED, last_updated=timestamp
                )
                records = dict(state.positions)
                records[trade_id] = blocked
                self._state_store.save(
                    replace(
                        state,
                        updated_at=timestamp,
                        trading_enabled=False,
                        blocked_reason=(
                            "Partial fill слишком мал для схемы TP1/TP2/runner."
                        ),
                        positions=records,
                    )
                )
                self._audit(
                    QtrMicroRuntimeEvent.ENTRY_ABORTED,
                    blocked,
                    timestamp,
                    reason="Partial fill слишком мал для схемы TP1/TP2/runner.",
                )
                return blocked
        filled_position = replace(
            position,
            average_fill=fill.average_price,
            filled_qty=effective_qty,
            initial_qty=effective_qty,
            current_qty=effective_qty,
            fees=position.fees + fill.fee,
            entry_fees=position.entry_fees + fill.fee,
            actual_risk_at_fill=effective_qty * filled_r,
            actual_risk_pct=(
                effective_qty * filled_r / equity * 100 if equity else None
            ),
            opened_at=fill.filled_at,
            last_updated=timestamp,
            stage=MicroStage.ENTRY_ACKNOWLEDGED,
            initial_r=filled_r,
            tp1_qty=tp1_qty,
            tp2_qty=tp2_qty,
            runner_qty=runner_qty,
            tp1_price=(fill.average_price + sign * self._settings.tp1_r * filled_r),
            tp2_price=(fill.average_price + sign * self._settings.tp2_r * filled_r),
            runner_target_price=(
                fill.average_price + sign * self._settings.runner_initial_r * filled_r
            ),
        )
        stop_installed = False
        for _ in range(STOP_INSTALL_ATTEMPTS):
            try:
                self._audit(
                    QtrMicroRuntimeEvent.PROTECTION_ATTEMPT,
                    filled_position,
                    timestamp,
                )
                self._client.set_protective_stop(
                    symbol=filled_position.symbol,
                    stop_price=filled_position.structural_stop,
                )
                stop_installed = True
                break
            except Exception:
                continue
        if not stop_installed:
            self._audit(
                QtrMicroRuntimeEvent.PROTECTION_FAILED,
                filled_position,
                timestamp,
                reason="Protective stop installation failed.",
            )
            try:
                self._emergency_close(filled_position)
            except Exception as error:
                _LOGGER.critical(
                    "Не удалось подтвердить emergency Demo close (%s).",
                    type(error).__name__,
                )
            blocked = replace(
                filled_position,
                stage=MicroStage.BLOCKED,
                last_updated=timestamp,
            )
            records = dict(state.positions)
            records[trade_id] = blocked
            self._state_store.save(
                replace(
                    state,
                    updated_at=timestamp,
                    trading_enabled=False,
                    blocked_reason="Не удалось установить protective stop.",
                    positions=records,
                )
            )
            _LOGGER.critical(
                "Protective stop не установлен; Demo position закрывается, "
                "entries заблокированы."
            )
            self._audit(
                QtrMicroRuntimeEvent.ENTRY_ABORTED,
                blocked,
                timestamp,
                reason="Не удалось установить protective stop.",
            )
            return blocked
        opened = replace(filled_position, stage=MicroStage.OPEN)
        self._save_position(state, opened, timestamp)
        self._audit(QtrMicroRuntimeEvent.ENTRY_PROTECTED, opened, timestamp)
        return opened

    def _audit(
        self,
        event: QtrMicroRuntimeEvent,
        position: MicroPosition,
        occurred_at: datetime,
        *,
        reason: str | None = None,
        detail: str | None = None,
        ret_code: int | None = None,
    ) -> None:
        if self._runtime_audit is None:
            return
        self._runtime_audit.append(
            QtrMicroRuntimeAuditRecord(
                occurred_at=occurred_at,
                event=event,
                trade_id=position.trade_id,
                symbol=position.symbol,
                stage=position.stage.value,
                reason=reason,
                detail=detail,
                ret_code=ret_code,
            )
        )

    def manage_position(
        self,
        trade_id: str,
        *,
        current_price: float,
        now: datetime,
        setup_cancelled: bool = False,
        opposite_structure: bool = False,
        structure_degraded: bool = False,
    ) -> ManagementDecision:
        timestamp = _utc(now)
        state = self._state_store.load(
            today=timestamp.date(), trading_enabled=self._settings.enabled
        )
        position = state.positions[trade_id]
        if position.pending_exit_order_id is not None:
            return self._confirm_exit(state, position, timestamp)
        observed = replace(
            position,
            max_favorable_price=_favorable(position, current_price),
            max_adverse_price=_adverse(position, current_price),
            last_updated=timestamp,
        )
        if observed != position:
            self._save_position(state, observed, timestamp)
            state = replace(
                state,
                updated_at=timestamp,
                positions={**state.positions, trade_id: observed},
            )
            position = observed
        decision = self._engine.manage(
            position,
            current_price=current_price,
            now=timestamp,
            setup_cancelled=setup_cancelled,
            opposite_structure=opposite_structure,
            structure_degraded=structure_degraded,
        )
        if decision.action is None:
            return decision
        acknowledgement = self._reduce(position, decision.close_qty, decision.action)
        pending = replace(
            position,
            pending_exit_order_id=acknowledgement.order_id,
            pending_exit_order_link_id=acknowledgement.order_link_id,
            pending_exit_reason=decision.action,
            pending_exit_qty=decision.close_qty,
            pending_new_stop=decision.new_stop,
            stage=MicroStage.EXIT_ACKNOWLEDGED,
            last_updated=timestamp,
        )
        self._save_position(state, pending, timestamp)
        return ManagementDecision(None, 0)

    def reconcile(self, now: datetime) -> MicroState:
        from market_signal_assistant.qtr_micro.reconciliation import (
            reconcile_demo_state,
        )

        timestamp = _utc(now)
        state = self._state_store.load(
            today=timestamp.date(), trading_enabled=self._settings.enabled
        )
        reconciled = reconcile_demo_state(
            state,
            positions=self._client.list_positions(),
            orders=self._client.list_active_orders(),
            now=timestamp,
        )
        for trade_id, previous in state.positions.items():
            current = reconciled.positions.get(trade_id)
            if (
                previous.stage in {MicroStage.PREPARED, MicroStage.ENTRY_ACKNOWLEDGED}
                and current is not None
                and current.stage is MicroStage.CLOSED
            ):
                self._audit(
                    QtrMicroRuntimeEvent.ENTRY_ABORTED,
                    current,
                    timestamp,
                    reason=(
                        "Reconciliation не обнаружил owned order или позицию "
                        "на Bybit Demo."
                    ),
                )
        records = dict(reconciled.positions)
        changed = False
        for trade_id, position in tuple(records.items()):
            if (
                position.stage is MicroStage.CLOSED
                and not position.journaled
                and position.average_fill is not None
                and self._journal is not None
            ):
                prior_position = state.positions.get(trade_id)
                fill: ExecutionFill | None = None
                reason = position.pending_exit_reason
                if (
                    prior_position is not None
                    and prior_position.stage is MicroStage.EXIT_ACKNOWLEDGED
                    and prior_position.pending_exit_order_id is not None
                    and prior_position.pending_exit_reason is not None
                ):
                    fill = self._client.execution_fill(
                        prior_position.pending_exit_order_id, prior_position.symbol
                    )
                    reason = prior_position.pending_exit_reason
                    if fill is not None:
                        closed_qty = min(fill.filled_qty, prior_position.current_qty)
                        if closed_qty >= prior_position.current_qty:
                            gross = _realised_pnl(prior_position, fill, closed_qty)
                            position = replace(
                                prior_position,
                                current_qty=0.0,
                                realised_partial_pnl=(
                                    prior_position.realised_partial_pnl + gross
                                ),
                                fees=prior_position.fees + fill.fee,
                                exit_fees=prior_position.exit_fees + fill.fee,
                                stage=MicroStage.CLOSED,
                                last_updated=timestamp,
                                pending_exit_order_id=None,
                                pending_exit_order_link_id=None,
                                pending_exit_reason=None,
                                pending_exit_qty=0.0,
                                pending_new_stop=None,
                                runner_exit_price=fill.average_price,
                            )
                            records[trade_id] = position
                            reconciled = record_trade_result(
                                reconciled,
                                pnl=position.realised_partial_pnl - position.fees,
                                now=timestamp,
                                settings=self._settings,
                            )
                        else:
                            fill = None
                elif (
                    prior_position is not None
                    and prior_position.stage
                    in {
                        MicroStage.OPEN,
                        MicroStage.TP1_FILLED,
                        MicroStage.TP2_FILLED,
                        MicroStage.RUNNER,
                    }
                ):
                    fill = self._client.protective_stop_fill(
                        symbol=prior_position.symbol,
                        opened_at=(
                            prior_position.opened_at or prior_position.signal_at
                        ),
                        direction=prior_position.direction,
                        expected_qty=prior_position.current_qty,
                    )
                    if fill is not None:
                        closed_qty = min(fill.filled_qty, prior_position.current_qty)
                        if closed_qty >= prior_position.current_qty:
                            gross = _realised_pnl(prior_position, fill, closed_qty)
                            position = replace(
                                prior_position,
                                current_qty=0.0,
                                realised_partial_pnl=(
                                    prior_position.realised_partial_pnl + gross
                                ),
                                fees=prior_position.fees + fill.fee,
                                exit_fees=prior_position.exit_fees + fill.fee,
                                stage=MicroStage.CLOSED,
                                last_updated=fill.filled_at,
                                pending_exit_order_id=None,
                                pending_exit_order_link_id=None,
                                pending_exit_reason=None,
                                pending_exit_qty=0.0,
                                pending_new_stop=None,
                                runner_exit_price=fill.average_price,
                            )
                            records[trade_id] = position
                            reason = MicroExitReason.STOP
                            reconciled = record_trade_result(
                                reconciled,
                                pnl=position.realised_partial_pnl - position.fees,
                                now=timestamp,
                                settings=self._settings,
                            )
                        else:
                            fill = None
                elif position.runner_exit_price is not None:
                    fill = ExecutionFill(
                        order_id="durable-exit-fill",
                        average_price=position.runner_exit_price,
                        filled_qty=position.initial_qty,
                        fee=0.0,
                        filled_at=position.last_updated,
                    )
                    reason = reason or MicroExitReason.STRUCTURE_EXIT
                if fill is None or reason is None:
                    reconciled = replace(
                        reconciled,
                        trading_enabled=False,
                        blocked_reason=JOURNAL_RECOVERY_BLOCK,
                    )
                    continue
                if self._write_journal(
                    position,
                    fill,
                    reason,
                    timestamp,
                ):
                    records[trade_id] = replace(position, journaled=True)
                    changed = True
        if changed:
            reconciled = replace(reconciled, positions=records, updated_at=timestamp)
        self._state_store.save(reconciled)
        return reconciled

    def _reduce(
        self,
        position: MicroPosition,
        qty: float,
        reason: MicroExitReason,
    ) -> OrderAcknowledgement:
        suffix = {
            MicroExitReason.TP1: "T1",
            MicroExitReason.TP2: "T2",
            MicroExitReason.RUNNER_TARGET: "R3",
            MicroExitReason.TIME_EXIT: "TM",
            MicroExitReason.RUNNER_TIME_EXIT: "RT",
            MicroExitReason.STRUCTURE_EXIT: "SX",
        }.get(reason, "CL")
        return self._client.create_market_order(
            symbol=position.symbol,
            side="Sell" if position.direction is MicroDirection.LONG else "Buy",
            qty=qty,
            order_link_id=f"{position.trade_id}-{suffix}",
            reduce_only=True,
        )

    def _confirm_exit(
        self,
        state: MicroState,
        position: MicroPosition,
        timestamp: datetime,
    ) -> ManagementDecision:
        assert position.pending_exit_order_id is not None
        assert position.pending_exit_reason is not None
        fill = self._client.execution_fill(
            position.pending_exit_order_id, position.symbol
        )
        if fill is None:
            return ManagementDecision(None, 0)
        closed_qty = min(fill.filled_qty, position.current_qty)
        remaining = max(0.0, position.current_qty - closed_qty)
        reason = position.pending_exit_reason
        gross = _realised_pnl(position, fill, closed_qty)
        current_stop = position.current_stop
        stop_update_failed = False
        if position.pending_new_stop is not None and remaining > 0:
            for attempt in range(1, STOP_INSTALL_ATTEMPTS + 1):
                self._audit(
                    QtrMicroRuntimeEvent.PROTECTION_ATTEMPT,
                    position,
                    timestamp,
                    detail=f"breakeven_attempt={attempt}",
                )
                try:
                    self._client.set_protective_stop(
                        symbol=position.symbol,
                        stop_price=position.pending_new_stop,
                    )
                    current_stop = position.pending_new_stop
                    break
                except Exception:
                    if attempt == STOP_INSTALL_ATTEMPTS:
                        stop_update_failed = True
        updated = replace(
            position,
            current_qty=remaining,
            current_stop=current_stop,
            realised_partial_pnl=position.realised_partial_pnl + gross,
            fees=position.fees + fill.fee,
            exit_fees=position.exit_fees + fill.fee,
            stage=_next_stage(reason, remaining),
            last_updated=timestamp,
            pending_exit_order_id=None,
            pending_exit_order_link_id=None,
            pending_exit_reason=None,
            pending_exit_qty=0.0,
            pending_new_stop=None,
            tp1_fill_price=(
                fill.average_price
                if reason is MicroExitReason.TP1
                else position.tp1_fill_price
            ),
            tp2_fill_price=(
                fill.average_price
                if reason is MicroExitReason.TP2
                else position.tp2_fill_price
            ),
            runner_exit_price=(
                fill.average_price if remaining <= 0 else position.runner_exit_price
            ),
            max_favorable_price=_favorable(position, fill.average_price),
            max_adverse_price=_adverse(position, fill.average_price),
        )
        records = dict(state.positions)
        records[position.trade_id] = updated
        resulting_state = replace(state, updated_at=timestamp, positions=records)
        if stop_update_failed:
            self._audit(
                QtrMicroRuntimeEvent.PROTECTION_FAILED,
                updated,
                timestamp,
                reason="Не удалось перенести protective stop в breakeven после TP1.",
            )
            try:
                self._emergency_close(updated)
            except Exception as error:
                _LOGGER.critical(
                    "Не удалось отправить emergency close после BE failure (%s).",
                    type(error).__name__,
                )
            updated = replace(updated, stage=MicroStage.BLOCKED)
            records[position.trade_id] = updated
            resulting_state = replace(
                resulting_state,
                trading_enabled=False,
                blocked_reason=(
                    "Не удалось перенести protective stop в breakeven после TP1."
                ),
                positions=records,
            )
            self._state_store.save(resulting_state)
            return ManagementDecision(reason, closed_qty, current_stop)
        if remaining <= 0:
            trade_net = updated.realised_partial_pnl - updated.fees
            resulting_state = record_trade_result(
                resulting_state,
                pnl=trade_net,
                now=timestamp,
                settings=self._settings,
            )
            try:
                journal_present = self._write_journal(updated, fill, reason, timestamp)
                updated = replace(updated, journaled=journal_present)
                records[position.trade_id] = updated
                resulting_state = replace(resulting_state, positions=records)
            except OSError:
                _LOGGER.critical("Trade закрыт, но QTR Micro journal не записан.")
                resulting_state = replace(
                    resulting_state,
                    trading_enabled=False,
                    blocked_reason=JOURNAL_RECOVERY_BLOCK,
                )
            self._state_store.save(resulting_state)
            return ManagementDecision(reason, closed_qty, current_stop)
        self._state_store.save(resulting_state)
        return ManagementDecision(reason, closed_qty, current_stop)

    def _write_journal(
        self,
        position: MicroPosition,
        fill: ExecutionFill,
        reason: MicroExitReason,
        timestamp: datetime,
    ) -> bool:
        if self._journal is None or position.average_fill is None:
            return False
        opened_at = position.opened_at or position.signal_at
        submitted_at = position.order_submitted_at or position.signal_at
        total_gross = position.realised_partial_pnl
        total_net = total_gross - position.fees
        result_r = total_net / position.risk_amount if position.risk_amount else 0.0
        already_present = self._journal.contains(position.trade_id)
        written = self._journal.append_once(
            TradeJournalEntry(
                trade_id=position.trade_id,
                setup_episode=position.setup_episode_id,
                symbol=position.symbol,
                direction=position.direction,
                setup_type=position.setup_type.name_ru,
                setup_confidence=position.setup_confidence,
                entry_signal_timestamp=position.signal_at,
                order_submit_timestamp=submitted_at,
                fill_timestamp=opened_at,
                signal_price=position.signal_price,
                average_fill=position.average_fill,
                slippage=position.average_fill - position.signal_price,
                initial_stop=position.structural_stop,
                risk_pct=position.risk_pct,
                risk_usdt=position.risk_amount,
                leverage=position.leverage,
                qty=position.initial_qty,
                tp1_fill=position.tp1_fill_price,
                tp2_fill=position.tp2_fill_price,
                runner_exit=fill.average_price,
                structure_time_exits=(
                    (reason.value,)
                    if reason
                    in {
                        MicroExitReason.STRUCTURE_EXIT,
                        MicroExitReason.TIME_EXIT,
                        MicroExitReason.RUNNER_TIME_EXIT,
                    }
                    else ()
                ),
                fees=position.fees,
                funding=position.funding,
                realised_gross_pnl=total_gross,
                realised_net_pnl=total_net,
                result_r=result_r,
                max_favorable_excursion=_excursion(position, favorable=True),
                max_adverse_excursion=_excursion(position, favorable=False),
                hold_duration_seconds=max(
                    0, int((timestamp - opened_at).total_seconds())
                ),
                exit_reason=reason.value,
                outcome=(
                    "winning"
                    if total_net > 0
                    else "losing"
                    if total_net < 0
                    else "breakeven"
                ),
                gross_pnl=total_gross,
                entry_fees=position.entry_fees,
                exit_fees=max(0.0, position.fees - position.entry_fees),
                total_fees=position.fees,
                net_pnl=total_net,
                gross_r=(
                    total_gross / position.actual_risk_at_fill
                    if position.actual_risk_at_fill
                    else None
                ),
                net_r=(
                    total_net / position.actual_risk_at_fill
                    if position.actual_risk_at_fill
                    else None
                ),
                actual_risk_at_fill=position.actual_risk_at_fill,
                actual_risk_pct=position.actual_risk_pct,
                planned_risk_usdt=position.risk_amount,
                planned_notional=position.planned_notional,
                effective_leverage=(
                    position.planned_notional
                    / (position.risk_amount / (position.risk_pct / 100))
                    if position.risk_pct > 0 and position.risk_amount > 0
                    else None
                ),
                pre_submit_price=position.pre_submit_price,
                signal_to_submit_slippage=(
                    position.pre_submit_price - position.signal_price
                    if position.pre_submit_price is not None
                    else None
                ),
                submit_to_fill_slippage=(
                    position.average_fill - position.pre_submit_price
                    if position.pre_submit_price is not None
                    else None
                ),
            )
        )
        self._audit(
            (
                QtrMicroRuntimeEvent.JOURNAL_ALREADY_PRESENT
                if already_present
                else QtrMicroRuntimeEvent.JOURNAL_WRITE
            ),
            position,
            timestamp,
        )
        return written or already_present

    def _emergency_close(self, position: MicroPosition) -> None:
        self._client.create_market_order(
            symbol=position.symbol,
            side="Sell" if position.direction is MicroDirection.LONG else "Buy",
            qty=position.current_qty,
            order_link_id=f"{position.trade_id}-SAFE",
            reduce_only=True,
        )

    def _save_position(
        self, state: MicroState, position: MicroPosition, now: datetime
    ) -> None:
        records = dict(state.positions)
        records[position.trade_id] = position
        self._state_store.save(replace(state, updated_at=now, positions=records))


def record_trade_result(
    state: MicroState,
    *,
    pnl: float,
    now: datetime,
    settings: QtrMicroSettings,
) -> MicroState:
    timestamp = _utc(now)
    if state.trading_day != timestamp.date():
        state = replace(
            state,
            trading_day=timestamp.date(),
            day_start_equity=max(
                0.0, state.day_start_equity + state.realised_daily_pnl
            ),
            realised_daily_pnl=0.0,
            consecutive_losses=0,
            loss_pause_until=None,
        )
    losses = state.consecutive_losses + 1 if pnl < 0 else 0
    pause = state.loss_pause_until
    if losses >= settings.max_consecutive_losses:
        pause = timestamp + timedelta(minutes=settings.loss_pause_minutes)
    return replace(
        state,
        updated_at=timestamp,
        realised_daily_pnl=state.realised_daily_pnl + pnl,
        consecutive_losses=losses,
        loss_pause_until=pause,
    )


def _position_from_plan(plan: EntryPlan, now: datetime) -> MicroPosition:
    return MicroPosition(
        trade_id=plan.trade_id,
        setup_episode_id=plan.setup_episode_id,
        symbol=plan.symbol,
        direction=plan.direction,
        setup_type=plan.setup_type,
        setup_confidence=plan.setup_confidence,
        entry_order_link_id=plan.order_link_id,
        entry_order_id=None,
        average_fill=None,
        filled_qty=0.0,
        initial_qty=plan.qty,
        current_qty=plan.qty,
        leverage=plan.leverage,
        risk_pct=plan.risk_pct,
        risk_amount=plan.risk_amount,
        structural_stop=plan.stop_price,
        current_stop=plan.stop_price,
        initial_r=plan.initial_r,
        tp1_price=plan.tp1_price,
        tp1_qty=plan.tp1_qty,
        tp2_price=plan.tp2_price,
        tp2_qty=plan.tp2_qty,
        runner_target_price=plan.runner_target_price,
        runner_qty=plan.runner_qty,
        realised_partial_pnl=0.0,
        fees=0.0,
        opened_at=None,
        last_updated=now,
        stage=MicroStage.PREPARED,
        signal_at=plan.signal_at,
        signal_price=plan.signal_price,
        pre_submit_price=plan.pre_submit_price,
        planned_notional=plan.notional,
    )


def _next_stage(reason: MicroExitReason, remaining: float) -> MicroStage:
    if remaining <= 0:
        return MicroStage.CLOSED
    if reason is MicroExitReason.TP1:
        return MicroStage.TP1_FILLED
    if reason is MicroExitReason.TP2:
        return MicroStage.TP2_FILLED
    return MicroStage.CLOSED


def _favorable(position: MicroPosition, price: float) -> float:
    previous = position.max_favorable_price
    if previous is None:
        return price
    if position.direction is MicroDirection.LONG:
        return max(previous, price)
    return min(previous, price)


def _adverse(position: MicroPosition, price: float) -> float:
    previous = position.max_adverse_price
    if previous is None:
        return price
    if position.direction is MicroDirection.LONG:
        return min(previous, price)
    return max(previous, price)


def _realised_pnl(
    position: MicroPosition,
    fill: ExecutionFill,
    qty: float,
) -> float:
    if position.average_fill is None:
        return 0.0
    sign = 1 if position.direction is MicroDirection.LONG else -1
    return sign * (fill.average_price - position.average_fill) * qty


def _excursion(position: MicroPosition, *, favorable: bool) -> float | None:
    if position.average_fill is None:
        return None
    price = position.max_favorable_price if favorable else position.max_adverse_price
    if price is None:
        return None
    sign = 1 if position.direction is MicroDirection.LONG else -1
    return sign * (price - position.average_fill)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("QTR Micro timestamp must be timezone-aware.")
    return value.astimezone(UTC)


def _typed_demo_reason(error: Exception) -> str:
    if isinstance(error, DemoApiError):
        if error.ret_code == 110007:
            return "Недостаточно доступного баланса для Demo order (110007)."
        if error.ret_code == 110090:
            return (
                "Размер или стоимость Demo order превышает лимит инструмента (110090)."
            )
        return str(error)
    return f"Техническая ошибка {type(error).__name__}."
