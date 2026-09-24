from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from market_signal_assistant.qtr_micro.client import DemoTradingClient
from market_signal_assistant.qtr_micro.engine import QtrMicroEntryEngine
from market_signal_assistant.qtr_micro.execution import QtrMicroExecutionService
from market_signal_assistant.qtr_micro.journal import (
    JsonlQtrMicroDecisionAudit,
    JsonlQtrMicroTradeJournal,
)
from market_signal_assistant.qtr_micro.models import (
    EntryPlan,
    MicroExitReason,
    MicroPosition,
    MicroStage,
    MicroState,
    PreflightResult,
)
from market_signal_assistant.qtr_micro.preflight import QtrMicroPreflight
from market_signal_assistant.qtr_micro.runtime_audit import (
    JsonlQtrMicroRuntimeAudit,
    QtrMicroRuntimeAuditRecord,
    QtrMicroRuntimeEvent,
)
from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
from market_signal_assistant.qtr_micro.state import JsonQtrMicroStateStore
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine.models import SetupDirection, SetupState
from market_signal_assistant.telegram.qtr_micro import (
    format_micro_closed,
    format_micro_entry,
    format_micro_tp,
)

_LOGGER = logging.getLogger(__name__)
MicroSender = Callable[[int, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class QtrMicroPositionSnapshot:
    trade_id: str
    symbol: str
    direction: str
    setup: str
    scanner_level: float | None
    actual_entry: float | None
    current_price: float | None
    initial_qty: float
    current_qty: float
    notional: float
    leverage: int
    initial_sl: float
    current_sl: float
    tp1: float
    tp2: float
    runner_target: float
    initial_risk_usdt: float | None
    current_pnl_est: float
    current_r: float | None
    mfe_r: float | None
    mae_r: float | None
    max_r: float | None
    opened_at: datetime | None
    duration_seconds: int
    stage: MicroStage
    exit_price: float | None
    gross_pnl: float
    fees: float
    net_pnl: float


@dataclass(frozen=True, slots=True)
class QtrMicroPositionEvent:
    event_type: str
    snapshot: QtrMicroPositionSnapshot
    exit_reason: MicroExitReason | None = None


QtrMicroPositionEventHandler = Callable[
    [QtrMicroPositionEvent], Awaitable[None]
]


@dataclass(frozen=True, slots=True)
class QtrMicroRuntimeStatus:
    enabled: bool
    demo_api_ready: bool
    blocked_reason: str | None
    open_positions: int
    daily_pnl: float
    kill_switch: bool


@dataclass(frozen=True, slots=True)
class PostEntryManagementFlags:
    setup_cancelled: bool
    opposite_structure: bool
    structure_degraded: bool


def post_entry_management_flags(
    position: MicroPosition,
    candidate: QtrSetupCandidate | None,
) -> PostEntryManagementFlags:
    """Map current setup evidence without treating degradation as hard cancel."""

    if candidate is None:
        return PostEntryManagementFlags(False, False, False)
    result = candidate.result
    current_failure = bool(result.current_breakout_failure)
    expected_direction = (
        SetupDirection.UP
        if position.direction.value == "LONG"
        else SetupDirection.DOWN
    )
    return PostEntryManagementFlags(
        setup_cancelled=(
            result.setup_state is SetupState.CANCELLED and not current_failure
        ),
        opposite_structure=bool(
            result.structure_confirmation
            and result.direction
            not in {expected_direction, SetupDirection.NEUTRAL}
        ),
        structure_degraded=current_failure,
    )


class QtrMicroRuntime:
    """Explicit Telegram-owned Demo lifecycle; no task or network at import."""

    def __init__(
        self,
        *,
        settings: QtrMicroSettings,
        client: DemoTradingClient | None,
        state_store: JsonQtrMicroStateStore,
        allowed_chat_ids: frozenset[int],
        clock: Callable[[], datetime] | None = None,
        decision_audit: JsonlQtrMicroDecisionAudit | None = None,
        runtime_audit: JsonlQtrMicroRuntimeAudit | None = None,
        position_event_handler: QtrMicroPositionEventHandler | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._state_store = state_store
        self._allowed_chat_ids = allowed_chat_ids
        self._clock = clock or (lambda: datetime.now(UTC))
        self._decision_audit = decision_audit or JsonlQtrMicroDecisionAudit()
        self._runtime_audit = runtime_audit or JsonlQtrMicroRuntimeAudit()
        self._engine = QtrMicroEntryEngine(settings)
        self._execution = (
            QtrMicroExecutionService(
                settings=settings,
                client=client,
                state_store=state_store,
                engine=self._engine,
                journal=JsonlQtrMicroTradeJournal(),
                runtime_audit=self._runtime_audit,
            )
            if client is not None
            else None
        )
        self._preflight = QtrMicroPreflight(
            settings,
            client,
            state_store=state_store,
            clock=self._clock,
        )
        self._preflight_result = PreflightResult(False, "Preflight ещё не выполнен.")
        self._position_event_handler = position_event_handler
        self._lock = asyncio.Lock()

    @property
    def status(self) -> QtrMicroRuntimeStatus:
        now = self._clock()
        state = self._state_store.load(
            today=now.date(), trading_enabled=self._settings.enabled
        )
        open_count = sum(
            item.stage not in {MicroStage.CLOSED, MicroStage.BLOCKED}
            for item in state.positions.values()
        )
        return QtrMicroRuntimeStatus(
            enabled=self._settings.enabled,
            demo_api_ready=self._preflight_result.ready,
            blocked_reason=self._preflight_result.reason or state.blocked_reason,
            open_positions=open_count,
            daily_pnl=state.realised_daily_pnl,
            kill_switch=self._settings.kill_switch,
        )

    def set_position_event_handler(
        self, handler: QtrMicroPositionEventHandler | None
    ) -> None:
        self._position_event_handler = handler

    async def initialize(self) -> PreflightResult:
        async with self._lock:
            result = await asyncio.to_thread(self._preflight.run, None)
            self._preflight_result = result
            if not result.ready:
                _LOGGER.warning("QTR Micro Demo заблокирован: %s", result.reason)
                return result
            assert self._execution is not None
            state = self._state_store.load(
                today=self._clock().date(), trading_enabled=True
            )
            if state.day_start_equity <= 0 and result.equity is not None:
                self._state_store.save(
                    replace(
                        state,
                        trading_enabled=True,
                        day_start_equity=result.equity,
                        updated_at=self._clock(),
                    )
                )
            reconciled = await asyncio.to_thread(
                self._execution.reconcile, self._clock()
            )
            if self._position_event_handler is not None:
                for position in reconciled.positions.values():
                    if position.stage not in {
                        MicroStage.OPEN,
                        MicroStage.TP1_FILLED,
                        MicroStage.TP2_FILLED,
                        MicroStage.RUNNER,
                        MicroStage.EXIT_ACKNOWLEDGED,
                    }:
                        continue
                    price = await self._price_for_position(position)
                    await self._emit_position_event(
                        "POSITION_RECOVERED", position, price, self._clock()
                    )
            return result

    async def get_position_snapshot(
        self, trade_id: str
    ) -> QtrMicroPositionSnapshot | None:
        async with self._lock:
            now = self._clock()
            state = self._state_store.load(
                today=now.date(), trading_enabled=self._settings.enabled
            )
            position = state.positions.get(trade_id)
            if position is None:
                return None
            price = await self._price_for_position(position)
            return _position_snapshot(position, price, now)

    async def request_human_close(
        self, trade_id: str
    ) -> QtrMicroPositionSnapshot | None:
        if self._execution is None:
            return None
        async with self._lock:
            now = self._clock()
            position = await asyncio.to_thread(
                self._execution.request_human_close, trade_id, now
            )
            if position is None:
                return None
            price = await self._price_for_position(position)
            snapshot = _position_snapshot(position, price, now)
            if (
                position.stage is MicroStage.EXIT_ACKNOWLEDGED
                and position.pending_exit_reason is MicroExitReason.HUMAN_CLOSE
            ):
                await self._emit_position_event(
                    "CLOSE_PENDING",
                    position,
                    price,
                    now,
                    exit_reason=MicroExitReason.HUMAN_CLOSE,
                )
            return snapshot

    async def handle_candidates(
        self,
        candidates: tuple[QtrSetupCandidate, ...],
        send: MicroSender,
    ) -> None:
        if not self._preflight_result.ready:
            return
        if self._client is None or self._execution is None:
            return
        async with self._lock:
            now = self._clock()
            assert self._execution is not None
            await asyncio.to_thread(self._execution.reconcile, now)
            try:
                remote_positions, equity = await asyncio.gather(
                    asyncio.to_thread(self._client.list_positions),
                    asyncio.to_thread(self._client.wallet_equity),
                )
            except Exception as error:
                _LOGGER.warning(
                    "QTR Micro cycle заблокирован до получения свежего Demo state "
                    "(%s).",
                    type(error).__name__,
                )
                return
            remote_symbols = {item.symbol for item in remote_positions}
            state = self._state_store.load(today=now.date(), trading_enabled=True)
            if state.trading_day != now.date():
                state = replace(
                    state,
                    updated_at=now,
                    trading_day=now.date(),
                    day_start_equity=equity,
                    realised_daily_pnl=0.0,
                    consecutive_losses=0,
                    loss_pause_until=None,
                )
                self._state_store.save(state)
            await self._confirm_pending(state, send, now)
            state = self._state_store.load(today=now.date(), trading_enabled=True)
            by_symbol = {item.result.symbol: item for item in candidates}
            await self._manage_open(state, by_symbol, send, now)
            state = self._state_store.load(today=now.date(), trading_enabled=True)
            if self._settings.kill_switch:
                return
            if not state.trading_enabled or state.blocked_reason is not None:
                return
            for candidate in candidates:
                if candidate.result.symbol in remote_symbols:
                    continue
                if any(
                    item.setup_episode_id == candidate.episode_id
                    for item in state.positions.values()
                ):
                    continue
                try:
                    position_mode = await asyncio.to_thread(
                        self._client.position_mode, candidate.result.symbol
                    )
                    if position_mode != "ONE_WAY":
                        _LOGGER.warning(
                            "QTR Micro entry пропущен для %s: "
                            "неподдерживаемый position mode %s.",
                            candidate.result.symbol,
                            position_mode,
                        )
                        continue
                    rules = await asyncio.to_thread(
                        self._client.instrument_rules, candidate.result.symbol
                    )
                    decision = self._engine.prepare_entry(
                        candidate,
                        now=now,
                        equity=equity,
                        rules=rules,
                        state=state,
                        preflight=self._preflight_result,
                    )
                    if decision.plan is None:
                        self._decision_audit.append_skip(
                            decided_at=now,
                            symbol=candidate.result.symbol,
                            episode_id=candidate.episode_id,
                            decision=decision,
                        )
                        _LOGGER.info(
                            "QTR Micro entry пропущен для %s: %s (%s).",
                            candidate.result.symbol,
                            decision.skip_detail
                            or (
                                decision.skip_reason.value
                                if decision.skip_reason is not None
                                else "неизвестная причина"
                            ),
                            (
                                decision.instrument_status.value
                                if decision.instrument_status is not None
                                else "n/a"
                            ),
                        )
                        continue
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.ENTRY_REVALIDATION_STARTED,
                        decision.plan,
                        now,
                    )
                    fresh_price = await asyncio.to_thread(
                        self._client.current_market_price,
                        candidate.result.symbol,
                    )
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.FRESH_PRICE_LOADED,
                        decision.plan,
                        now,
                        detail=f"price={fresh_price:.12g}",
                    )
                    revalidated = self._engine.revalidate_entry(
                        candidate,
                        decision.plan,
                        current_price=fresh_price,
                        now=now,
                        equity=equity,
                        rules=rules,
                        state=state,
                        preflight=self._preflight_result,
                    )
                    if revalidated.plan is None:
                        self._append_entry_audit(
                            QtrMicroRuntimeEvent.ENTRY_REVALIDATION_REJECTED,
                            decision.plan,
                            now,
                            detail=(
                                revalidated.skip_reason.value
                                if revalidated.skip_reason is not None
                                else "unknown"
                            ),
                        )
                        self._decision_audit.append_skip(
                            decided_at=now,
                            symbol=candidate.result.symbol,
                            episode_id=candidate.episode_id,
                            decision=revalidated,
                        )
                        continue
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.ENTRY_REVALIDATION_PASSED,
                        revalidated.plan,
                        now,
                    )
                    self._append_entry_audit(
                        QtrMicroRuntimeEvent.SIZE_RECALCULATED,
                        revalidated.plan,
                        now,
                        detail=(
                            f"qty={revalidated.plan.qty:.12g}; "
                            f"notional={revalidated.plan.notional:.8f}; "
                            f"fees_r_pct={revalidated.plan.estimated_fees_r_pct:.4f}"
                        ),
                    )
                    confirmed = await asyncio.to_thread(
                        self._execution.submit_and_confirm_entry,
                        revalidated.plan,
                        now,
                        rules,
                    )
                    if confirmed.stage is MicroStage.OPEN:
                        if self._position_event_handler is not None:
                            await self._emit_position_event(
                                "POSITION_OPENED",
                                confirmed,
                                confirmed.average_fill,
                                now,
                            )
                        else:
                            await self._broadcast(
                                send,
                                format_micro_entry(_plan_from_position(confirmed)),
                            )
                except Exception as error:
                    _LOGGER.warning(
                        "QTR Micro entry пропущен для %s (%s): %s.",
                        candidate.result.symbol,
                        type(error).__name__,
                        str(error),
                    )

    def _append_entry_audit(
        self,
        event: QtrMicroRuntimeEvent,
        plan: EntryPlan,
        occurred_at: datetime,
        *,
        detail: str | None = None,
    ) -> None:
        self._runtime_audit.append(
            QtrMicroRuntimeAuditRecord(
                occurred_at=occurred_at,
                event=event,
                trade_id=plan.trade_id,
                symbol=plan.symbol,
                stage=MicroStage.PREPARED.value,
                detail=detail,
            )
        )

    async def _confirm_pending(
        self, state: MicroState, send: MicroSender, now: datetime
    ) -> None:
        assert self._execution is not None
        assert self._client is not None
        for position in state.positions.values():
            if position.stage is not MicroStage.ENTRY_ACKNOWLEDGED:
                continue
            confirmed = await asyncio.to_thread(
                self._execution.confirm_entry,
                position.trade_id,
                now,
                rules=await asyncio.to_thread(
                    self._client.instrument_rules, position.symbol
                ),
            )
            if confirmed is None or confirmed.stage is not MicroStage.OPEN:
                continue
            if self._position_event_handler is not None:
                await self._emit_position_event(
                    "POSITION_OPENED",
                    confirmed,
                    confirmed.average_fill,
                    now,
                )
            else:
                plan = _plan_from_position(confirmed)
                await self._broadcast(send, format_micro_entry(plan))

    async def _manage_open(
        self,
        state: MicroState,
        candidates: dict[str, QtrSetupCandidate],
        send: MicroSender,
        now: datetime,
    ) -> None:
        assert self._execution is not None
        assert self._client is not None
        for position in state.positions.values():
            if position.stage not in {
                MicroStage.OPEN,
                MicroStage.TP1_FILLED,
                MicroStage.TP2_FILLED,
                MicroStage.RUNNER,
                MicroStage.EXIT_ACKNOWLEDGED,
            }:
                continue
            candidate = candidates.get(position.symbol)
            current_price = (
                candidate.result.current_price if candidate is not None else None
            )
            if current_price is None:
                try:
                    current_price = await asyncio.to_thread(
                        self._client.current_market_price, position.symbol
                    )
                except Exception as error:
                    _LOGGER.warning(
                        "QTR Micro management пропущен для %s: свежая цена "
                        "недоступна (%s).",
                        position.symbol,
                        type(error).__name__,
                    )
                    continue
            assert current_price is not None
            management_flags = post_entry_management_flags(position, candidate)
            decision = await asyncio.to_thread(
                self._execution.manage_position,
                position.trade_id,
                current_price=current_price,
                now=now,
                setup_cancelled=management_flags.setup_cancelled,
                opposite_structure=management_flags.opposite_structure,
                structure_degraded=management_flags.structure_degraded,
            )
            if decision.action is not None and self._position_event_handler is not None:
                refreshed = self._state_store.load(
                    today=now.date(), trading_enabled=self._settings.enabled
                )
                updated = refreshed.positions.get(position.trade_id)
                if updated is not None:
                    event_type = (
                        "POSITION_CLOSED"
                        if updated.stage is MicroStage.CLOSED
                        else "POSITION_UPDATED"
                    )
                    event_price = (
                        updated.runner_exit_price
                        if updated.stage is MicroStage.CLOSED
                        and updated.runner_exit_price is not None
                        else current_price
                    )
                    await self._emit_position_event(
                        event_type,
                        updated,
                        event_price,
                        now,
                        exit_reason=decision.action,
                    )
            elif decision.action in {MicroExitReason.TP1, MicroExitReason.TP2}:
                result_r = 1.0 if decision.action is MicroExitReason.TP1 else 2.0
                await self._broadcast(
                    send, format_micro_tp(position.symbol, decision.action, result_r)
                )
            elif decision.action is not None:
                await self._broadcast(
                    send,
                    format_micro_closed(
                        position,
                        reason=decision.action.value,
                        pnl=0.0,
                        result_r=0.0,
                        hold_minutes=_hold_minutes(position, now),
                    ),
                )

    async def _price_for_position(self, position: MicroPosition) -> float | None:
        if position.stage is MicroStage.CLOSED and position.runner_exit_price is not None:
            return position.runner_exit_price
        if self._client is None:
            return position.average_fill
        try:
            return await asyncio.to_thread(
                self._client.current_market_price, position.symbol
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader snapshot price unavailable for %s (%s).",
                position.symbol,
                type(error).__name__,
            )
            return position.average_fill

    async def _emit_position_event(
        self,
        event_type: str,
        position: MicroPosition,
        current_price: float | None,
        now: datetime,
        *,
        exit_reason: MicroExitReason | None = None,
    ) -> None:
        handler = self._position_event_handler
        if handler is None:
            return
        try:
            await handler(
                QtrMicroPositionEvent(
                    event_type=event_type,
                    snapshot=_position_snapshot(position, current_price, now),
                    exit_reason=exit_reason,
                )
            )
        except Exception as error:
            _LOGGER.warning(
                "QTR Trader position event delivery failed (%s).",
                type(error).__name__,
            )

    async def _broadcast(self, send: MicroSender, text: str) -> None:
        for chat_id in sorted(self._allowed_chat_ids):
            await send(chat_id, text)


def _plan_from_position(position: MicroPosition) -> EntryPlan:
    if position.average_fill is None:
        raise ValueError("Filled Micro position has no average fill.")
    return EntryPlan(
        trade_id=position.trade_id,
        setup_episode_id=position.setup_episode_id,
        symbol=position.symbol,
        direction=position.direction,
        setup_type=position.setup_type,
        setup_confidence=position.setup_confidence,
        signal_at=position.signal_at,
        signal_price=position.signal_price,
        entry_price=position.average_fill,
        stop_price=position.structural_stop,
        risk_pct=position.risk_pct,
        risk_amount=position.risk_amount,
        qty=position.initial_qty,
        leverage=position.leverage,
        tp1_price=position.tp1_price,
        tp1_qty=position.tp1_qty,
        tp2_price=position.tp2_price,
        tp2_qty=position.tp2_qty,
        runner_target_price=position.runner_target_price,
        runner_qty=position.runner_qty,
        initial_r=position.initial_r,
        order_link_id=position.entry_order_link_id,
        scanner_level=position.scanner_level,
    )


def _position_snapshot(
    position: MicroPosition,
    current_price: float | None,
    now: datetime,
) -> QtrMicroPositionSnapshot:
    entry = position.average_fill
    effective_price = current_price if current_price is not None else entry
    sign = 1.0 if position.direction.value == "LONG" else -1.0
    unrealized = 0.0
    if entry is not None and effective_price is not None:
        unrealized = sign * (effective_price - entry) * position.current_qty
    gross_pnl = position.realised_partial_pnl + unrealized
    net_pnl = gross_pnl - position.fees
    risk_usdt = position.actual_risk_at_fill
    current_r = (
        net_pnl / risk_usdt
        if risk_usdt is not None and risk_usdt > 0
        else None
    )
    risk_distance = (
        abs(entry - position.structural_stop) if entry is not None else 0.0
    )

    def excursion_r(price: float | None) -> float | None:
        if entry is None or price is None or risk_distance <= 0:
            return None
        return sign * (price - entry) / risk_distance

    mfe_r = excursion_r(position.max_favorable_price)
    mae_r = excursion_r(position.max_adverse_price)
    duration_seconds = 0
    if position.opened_at is not None:
        duration_seconds = max(
            0, int((now - position.opened_at).total_seconds())
        )
    notional = (
        entry * position.initial_qty
        if entry is not None
        else position.planned_notional
    )
    return QtrMicroPositionSnapshot(
        trade_id=position.trade_id,
        symbol=position.symbol,
        direction=position.direction.value,
        setup=position.setup_type.name_ru,
        scanner_level=position.scanner_level,
        actual_entry=entry,
        current_price=effective_price,
        initial_qty=position.initial_qty,
        current_qty=position.current_qty,
        notional=notional,
        leverage=position.leverage,
        initial_sl=position.structural_stop,
        current_sl=position.current_stop,
        tp1=position.tp1_price,
        tp2=position.tp2_price,
        runner_target=position.runner_target_price,
        initial_risk_usdt=risk_usdt,
        current_pnl_est=net_pnl,
        current_r=current_r,
        mfe_r=mfe_r,
        mae_r=mae_r,
        max_r=mfe_r,
        opened_at=position.opened_at,
        duration_seconds=duration_seconds,
        stage=position.stage,
        exit_price=position.runner_exit_price,
        gross_pnl=gross_pnl,
        fees=position.fees,
        net_pnl=net_pnl,
    )


def _hold_minutes(position: MicroPosition, now: datetime) -> int:
    opened_at = position.opened_at
    if opened_at is None:
        return 0
    return max(0, int((now - opened_at).total_seconds() // 60))
